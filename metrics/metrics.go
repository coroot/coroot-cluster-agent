package metrics

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/coroot/coroot-cluster-agent/flags"
	"github.com/coroot/coroot-cluster-agent/metrics/aws"
	"github.com/coroot/coroot-cluster-agent/metrics/mongo"
	postgres "github.com/coroot/coroot-pg-agent/collector"
	"github.com/coroot/coroot/model"
	"github.com/coroot/logger"
	"github.com/go-kit/log/level"
	redis "github.com/oliver006/redis_exporter/exporter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	memcached "github.com/prometheus/memcached_exporter/pkg/exporter"
	"k8s.io/klog"
)

type Metrics struct {
	endpoint       *url.URL
	apiKey         string
	listenAddr     string
	scrapeInterval time.Duration
	scrapeTimeout  time.Duration
	walDir         string

	reg *prometheus.Registry

	exporters map[string]*Exporter

	aws *aws.Discoverer
}

func NewMetrics() (*Metrics, error) {
	if *flags.MetricsScrapeInterval == 0 {
		klog.Infoln("scrape interval is not set, disabling the scraper")
		return nil, nil
	}

	ms := &Metrics{
		endpoint:       (*flags.CorootURL).JoinPath("/v1/metrics"),
		apiKey:         *flags.APIKey,
		listenAddr:     *flags.ListenAddress,
		scrapeInterval: *flags.MetricsScrapeInterval,
		scrapeTimeout:  *flags.MetricsScrapeTimeout,
		walDir:         *flags.MetricsWALDir,
		reg:            prometheus.NewRegistry(),
		exporters:      map[string]*Exporter{},
	}

	klog.Infof("endpoint: %s, scrape interval: %s", ms.endpoint, ms.scrapeInterval)

	err := ms.runScraper()
	if err != nil {
		return nil, err
	}

	return ms, nil
}

func (ms *Metrics) ListenConfigUpdates(updates <-chan model.Config) {
	go func() {
		for cfg := range updates {
			ms.updateExporters(cfg.ApplicationInstrumentation)
			ms.updateAWS(cfg.AWSConfig)
		}
	}()
}

func (ms *Metrics) HttpHandler() http.Handler {
	return promhttp.HandlerFor(ms.reg, promhttp.HandlerOpts{})
}

func (ms *Metrics) updateExporters(targets []model.ApplicationInstrumentation) {
	actual := map[string]bool{}
	for _, t := range targets {
		config := ExporterConfig(t)
		actual[config.Address()] = true
		exporter := ms.exporters[config.Address()]
		switch {
		case exporter == nil:
			klog.Infof("new target: %s", config)
			ms.startExporter(config)
		case config.Equal(exporter.Config):
			continue
		default:
			klog.Infof("updating target: %s", config)
			ms.stopExporter(exporter.Config)
			ms.startExporter(config)
		}
	}
	for _, exporter := range ms.exporters {
		config := exporter.Config
		if actual[config.Address()] {
			continue
		}
		klog.Infof("deleting target: %s", config)
		ms.stopExporter(config)
	}
}

func (ms *Metrics) startExporter(config ExporterConfig) {
	collector, stop, err := ms.createCollector(config)
	if err != nil {
		klog.Errorln(err)
		return
	}

	exporter := &Exporter{Config: config, Collector: collector, Stop: stop}
	err = prometheus.WrapRegistererWith(config.Labels(), ms.reg).Register(exporter)
	if err != nil {
		klog.Errorln(err)
		return
	}
	ms.exporters[config.Address()] = exporter
}

func (ms *Metrics) stopExporter(config ExporterConfig) {
	exporter := ms.exporters[config.Address()]
	if exporter == nil {
		return
	}

	prometheus.WrapRegistererWith(config.Labels(), ms.reg).Unregister(exporter)
	if exporter.Stop != nil {
		exporter.Stop()
	}
	delete(ms.exporters, config.Address())
}

func (ms *Metrics) createCollector(config ExporterConfig) (prometheus.Collector, func(), error) {
	switch config.Type {

	case model.ApplicationTypePostgres:
		userPass := url.UserPassword(config.Credentials.Username, config.Credentials.Password)
		query := url.Values{}
		query.Set("connect_timeout", "1")
		query.Set("statement_timeout", strconv.Itoa(int(ms.scrapeTimeout.Milliseconds())))
		sslmode := config.Params["sslmode"]
		if sslmode == "" {
			sslmode = "disable"
		}
		query.Set("sslmode", sslmode)
		dsn := fmt.Sprintf("postgresql://%s@%s/postgres?%s", userPass, config.Address(), query.Encode())
		collector, err := postgres.New(dsn, ms.scrapeTimeout, logger.NewKlog(config.Address()))
		if err != nil {
			return nil, nil, err
		}
		return collector, func() { _ = collector.Close() }, nil

	case model.ApplicationTypeRedis:
		dsn := fmt.Sprintf("redis://%s", config.Address())
		opts := redis.Options{
			User:                           config.Credentials.Username,
			Password:                       config.Credentials.Password,
			Namespace:                      "redis",
			ConnectionTimeouts:             ms.scrapeTimeout,
			RedisMetricsOnly:               true,
			ExcludeLatencyHistogramMetrics: true,
		}
		collector, err := redis.NewRedisExporter(dsn, opts)
		if err != nil {
			return nil, nil, err
		}
		return collector, nil, nil

	case model.ApplicationTypeMongodb:
		collector := mongo.New(
			config.Address(),
			config.Credentials.Username,
			config.Credentials.Password,
			ms.scrapeTimeout,
		)
		return collector, func() { _ = collector.Close() }, nil

	case model.ApplicationTypeMemcached:
		collector := memcached.New(
			config.Address(),
			ms.scrapeTimeout,
			level.NewFilter(&promLogger{l: logger.NewKlog(config.Address())}, level.AllowInfo()),
			nil,
		)
		return collector, nil, nil
	}
	return nil, nil, fmt.Errorf("unsupported application type: %s", config.Type)
}

func (ms *Metrics) updateAWS(cfg *model.AWSConfig) {
	switch {
	case cfg == nil && ms.aws == nil:
	case cfg == nil && ms.aws != nil:
		ms.aws.Stop()
		ms.aws = nil
	case cfg != nil && ms.aws == nil:
		d, err := aws.NewDiscoverer(cfg, ms.reg)
		if err != nil {
			klog.Errorln(err)
		} else {
			ms.aws = d
		}
	default:
		err := ms.aws.Update(cfg)
		if err != nil {
			klog.Errorln(err)
			ms.aws.Stop()
			ms.aws = nil
		}
	}
}

type promLogger struct {
	l logger.Logger
}

func (l *promLogger) Log(keyvals ...interface{}) error {
	l.l.Info(keyvals...)
	return nil
}
