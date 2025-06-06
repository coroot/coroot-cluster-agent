package metrics

import (
	"cmp"
	"fmt"
	"maps"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/coroot/coroot-cluster-agent/k8s"
	"github.com/coroot/coroot-cluster-agent/metrics/mongo"
	"github.com/coroot/coroot-cluster-agent/metrics/mysql"
	postgres "github.com/coroot/coroot-pg-agent/collector"
	"github.com/coroot/logger"
	"github.com/go-kit/log/level"
	redis "github.com/oliver006/redis_exporter/exporter"
	"github.com/prometheus/client_golang/prometheus"
	memcached "github.com/prometheus/memcached_exporter/pkg/exporter"
)

type TargetType string

const (
	TargetTypePostgres  TargetType = "postgres"
	TargetTypeMysql     TargetType = "mysql"
	TargetTypeRedis     TargetType = "redis"
	TargetTypeMongodb   TargetType = "mongodb"
	TargetTypeMemcached TargetType = "memcached"
)

type Credentials struct {
	Username string
	Password string
}

type CredentialsSecret struct {
	Namespace   string
	Name        string
	UsernameKey string
	PasswordKey string
}

type Target struct {
	Type              TargetType
	Addr              string
	Sni               string
	Credentials       Credentials
	CredentialsSecret CredentialsSecret
	Params            map[string]string

	Description                  string
	DiscoveredFromPodAnnotations bool

	coll   prometheus.Collector
	stop   func()
	logger logger.Logger
}

func (t *Target) Equal(other *Target) bool {
	return t.Type == other.Type &&
		t.Addr == other.Addr &&
		t.Credentials == other.Credentials &&
		t.CredentialsSecret == other.CredentialsSecret &&
		maps.Equal(t.Params, other.Params)
}

func (t *Target) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("exporter", "", nil, nil)
}

func (t *Target) Collect(ch chan<- prometheus.Metric) {
	if t.coll != nil {
		start := time.Now()
		t.coll.Collect(ch)
		t.logger.Info("metrics collection completed in", time.Since(start).Truncate(time.Millisecond))
	}
}

func (t *Target) Labels() prometheus.Labels {
	return prometheus.Labels{"address": t.Addr}
}

func (t *Target) String() string {
	return fmt.Sprintf("%s://%s (%s)", t.Type, t.Addr, t.Description)
}

func (t *Target) IsExporterStarted() bool {
	return t.coll != nil
}

func (t *Target) StartExporter(reg *prometheus.Registry, credentials Credentials, scrapeInterval, scrapeTimeout time.Duration) error {
	collectTimeout := scrapeTimeout - time.Second
	if collectTimeout <= 0 {
		collectTimeout = time.Second
	}
	switch t.Type {

	case TargetTypePostgres:
		userPass := url.UserPassword(credentials.Username, credentials.Password)
		query := url.Values{}
		query.Set("connect_timeout", "1")
		query.Set("statement_timeout", strconv.Itoa(int(collectTimeout.Milliseconds())))
		sslmode := t.Params["sslmode"]
		if sslmode == "" {
			sslmode = "disable"
		}
		query.Set("sslmode", sslmode)
		dsn := fmt.Sprintf("postgresql://%s@%s/postgres?%s", userPass, t.Addr, query.Encode())
		collector, err := postgres.New(dsn, scrapeInterval, collectTimeout, t.logger)
		if err != nil {
			return err
		}
		t.coll = collector
		t.stop = func() { _ = collector.Close() }

	case TargetTypeMysql:
		userPass := fmt.Sprintf("%s:%s", credentials.Username, credentials.Password)
		query := url.Values{}
		query.Set("timeout", fmt.Sprintf("%dms", collectTimeout.Milliseconds()))
		tls := t.Params["tls"]
		if tls == "" {
			tls = "false"
		}
		query.Set("tls", tls)
		dsn := fmt.Sprintf("%s@tcp(%s)/mysql?%s", userPass, t.Addr, query.Encode())
		collector, err := mysql.New(dsn, t.logger, scrapeInterval, collectTimeout)
		if err != nil {
			return err
		}
		t.coll = collector
		t.stop = func() { _ = collector.Close() }

	case TargetTypeRedis:
		dsn := fmt.Sprintf("redis://%s", t.Addr)
		opts := redis.Options{
			User:                           credentials.Username,
			Password:                       credentials.Password,
			Namespace:                      "redis",
			ConnectionTimeouts:             collectTimeout,
			RedisMetricsOnly:               true,
			ExcludeLatencyHistogramMetrics: true,
		}
		collector, err := redis.NewRedisExporter(dsn, opts)
		if err != nil {
			return err
		}
		t.coll = collector
		t.stop = func() {}

	case TargetTypeMongodb:
		sni := t.Sni
		tlsParam := t.Params["tls"]
		if tlsParam == "" {
			tlsParam = "false"
		}
		collector := mongo.New(
			t.Addr,
			credentials.Username,
			credentials.Password,
			tlsParam,
			sni,
			collectTimeout,
			t.logger,
		)
		t.coll = collector
		t.stop = func() { _ = collector.Close() }

	case TargetTypeMemcached:
		collector := memcached.New(
			t.Addr,
			collectTimeout,
			level.NewFilter(&promLogger{l: t.logger}, level.AllowInfo()),
			nil,
		)
		t.coll = collector
		t.stop = func() {}

	default:
		return fmt.Errorf("unsupported target type: %s", t.Type)
	}

	return prometheus.WrapRegistererWith(t.Labels(), reg).Register(t)
}

func (t *Target) StopExporter(reg *prometheus.Registry) {
	if t.coll != nil {
		prometheus.WrapRegistererWith(t.Labels(), reg).Unregister(t)
		if t.stop != nil {
			t.stop()
		}
		t.coll = nil
	}
}

func TargetFromConfig(i config.ApplicationInstrumentation) *Target {
	t := &Target{
		Type: TargetType(i.Type),
		Addr: net.JoinHostPort(i.Host, i.Port),
		Sni:  i.Sni,
		Credentials: Credentials{
			Username: i.Credentials.Username,
			Password: i.Credentials.Password,
		},
		Params:      i.Params,
		Description: i.Instance,
	}
	t.logger = logger.NewKlog(t.String())
	return t
}

func TargetFromPod(pod *k8s.Pod) *Target {
	if pod == nil || pod.Annotations == nil {
		return nil
	}

	var t *Target

	if pod.Annotations["coroot.com/postgres-scrape"] == "true" {
		t = &Target{
			Type: TargetTypePostgres,
			Addr: net.JoinHostPort(pod.IP, cmp.Or(pod.Annotations["coroot.com/postgres-scrape-port"], "5432")),
			Credentials: Credentials{
				Username: pod.Annotations["coroot.com/postgres-scrape-credentials-username"],
				Password: pod.Annotations["coroot.com/postgres-scrape-credentials-password"],
			},
			CredentialsSecret: CredentialsSecret{
				Namespace:   pod.Id.Namespace,
				Name:        pod.Annotations["coroot.com/postgres-scrape-credentials-secret-name"],
				UsernameKey: pod.Annotations["coroot.com/postgres-scrape-credentials-secret-username-key"],
				PasswordKey: pod.Annotations["coroot.com/postgres-scrape-credentials-secret-password-key"],
			},
			Params: map[string]string{
				"sslmode": pod.Annotations["coroot.com/postgres-scrape-param-sslmode"],
			},
		}
	}

	if pod.Annotations["coroot.com/mysql-scrape"] == "true" {
		t = &Target{
			Type: TargetTypeMysql,
			Addr: net.JoinHostPort(pod.IP, cmp.Or(pod.Annotations["coroot.com/mysql-scrape-port"], "3306")),
			Credentials: Credentials{
				Username: pod.Annotations["coroot.com/mysql-scrape-credentials-username"],
				Password: pod.Annotations["coroot.com/mysql-scrape-credentials-password"],
			},
			CredentialsSecret: CredentialsSecret{
				Namespace:   pod.Id.Namespace,
				Name:        pod.Annotations["coroot.com/mysql-scrape-credentials-secret-name"],
				UsernameKey: pod.Annotations["coroot.com/mysql-scrape-credentials-secret-username-key"],
				PasswordKey: pod.Annotations["coroot.com/mysql-scrape-credentials-secret-password-key"],
			},
			Params: map[string]string{
				"tls": pod.Annotations["coroot.com/mysql-scrape-param-tls"],
			},
		}
	}

	if pod.Annotations["coroot.com/redis-scrape"] == "true" {
		t = &Target{
			Type: TargetTypeRedis,
			Addr: net.JoinHostPort(pod.IP, cmp.Or(pod.Annotations["coroot.com/redis-scrape-port"], "6379")),
			Credentials: Credentials{
				Username: pod.Annotations["coroot.com/redis-scrape-credentials-username"],
				Password: pod.Annotations["coroot.com/redis-scrape-credentials-password"],
			},
			CredentialsSecret: CredentialsSecret{
				Namespace:   pod.Id.Namespace,
				Name:        pod.Annotations["coroot.com/redis-scrape-credentials-secret-name"],
				UsernameKey: pod.Annotations["coroot.com/redis-scrape-credentials-secret-username-key"],
				PasswordKey: pod.Annotations["coroot.com/redis-scrape-credentials-secret-password-key"],
			},
		}
	}

	if pod.Annotations["coroot.com/mongodb-scrape"] == "true" {
		t = &Target{
			Type: TargetTypeMongodb,
			Addr: net.JoinHostPort(pod.IP, cmp.Or(pod.Annotations["coroot.com/mongodb-scrape-port"], "27017")),
			Sni:  pod.Id.Name,
			Credentials: Credentials{
				Username: pod.Annotations["coroot.com/mongodb-scrape-credentials-username"],
				Password: pod.Annotations["coroot.com/mongodb-scrape-credentials-password"],
			},
			CredentialsSecret: CredentialsSecret{
				Namespace:   pod.Id.Namespace,
				Name:        pod.Annotations["coroot.com/mongodb-scrape-credentials-secret-name"],
				UsernameKey: pod.Annotations["coroot.com/mongodb-scrape-credentials-secret-username-key"],
				PasswordKey: pod.Annotations["coroot.com/mongodb-scrape-credentials-secret-password-key"],
			},
			Params: map[string]string{
				"tls": pod.Annotations["coroot.com/mongodb-scrape-param-tls"],
			},
		}
	}

	if pod.Annotations["coroot.com/memcached-scrape"] == "true" {
		t = &Target{
			Type: TargetTypeMemcached,
			Addr: net.JoinHostPort(pod.IP, cmp.Or(pod.Annotations["coroot.com/memcached-scrape-port"], "11211")),
		}
	}

	if t != nil {
		t.DiscoveredFromPodAnnotations = true
		t.Description = fmt.Sprintf("ns=%s, pod=%s, node=%s", pod.Id.Namespace, pod.Id.Name, pod.Id.NodeName)
		t.logger = logger.NewKlog(t.String())
	}

	return t
}
