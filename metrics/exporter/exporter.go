package exporter

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
	"github.com/coroot/coroot-cluster-agent/metrics/mongo"
	"github.com/coroot/coroot-cluster-agent/metrics/mysql"
	"github.com/coroot/coroot-cluster-agent/metrics/postgres"
	"github.com/coroot/logger"
	"github.com/go-kit/log/level"
	gomysql "github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	redis "github.com/oliver006/redis_exporter/exporter"
	"github.com/prometheus/client_golang/prometheus"
	memcached "github.com/prometheus/memcached_exporter/pkg/exporter"
)

type Type string

const (
	TypePostgres  Type = "postgres"
	TypeMysql     Type = "mysql"
	TypeRedis     Type = "redis"
	TypeMongodb   Type = "mongodb"
	TypeMemcached Type = "memcached"
)

type Credentials struct {
	Username string
	Password string
}

type Config struct {
	Type        Type
	Addr        string
	Credentials Credentials
	TLS         common.TLSCredentials
	Params      map[string]string
	Description string
	LogService  string
}

type Options struct {
	ScrapeInterval   time.Duration
	CollectTimeout   time.Duration
	ChangeEmitter    dbtracker.ChangeEmitter
	Logs             *common.LogsPipeline
	MaxTablesPerDB   int
	TrackSizes       bool
	TrackBloat       bool
	ExcludeDatabases []string
}

type Exporter struct {
	prometheus.Collector
	close func()
}

func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("exporter", "", nil, nil)
}

func (e *Exporter) Close() {
	if e.close != nil {
		e.close()
	}
}

func New(cfg Config, opts Options, logger logger.Logger) (*Exporter, error) {
	switch cfg.Type {

	case TypePostgres:
		userPass := url.UserPassword(cfg.Credentials.Username, cfg.Credentials.Password)
		query := url.Values{}
		query.Set("connect_timeout", "1")
		query.Set("statement_timeout", strconv.Itoa(int(opts.CollectTimeout.Milliseconds())))
		sslmode := cfg.Params["sslmode"]
		pqTLSName := ""
		if cfg.TLS.CA != "" || (cfg.TLS.Cert != "" && cfg.TLS.Key != "") {
			tlsCfg, err := common.DatabaseTLSConfig(cfg.TLS, false)
			if err != nil {
				return nil, err
			}
			pqTLSName = "coroot-" + cfg.Addr
			if err = pq.RegisterTLSConfig(pqTLSName, tlsCfg); err != nil {
				return nil, err
			}
			sslmode = "pqgo-" + pqTLSName
		}
		if sslmode == "" {
			sslmode = "disable"
		}
		query.Set("sslmode", sslmode)
		dsn := fmt.Sprintf("postgresql://%s@%s/postgres?%s", userPass, cfg.Addr, query.Encode())
		collector, err := postgres.New(dsn, opts.ScrapeInterval, opts.CollectTimeout, logger, opts.ChangeEmitter, cfg.Addr, opts.MaxTablesPerDB, opts.TrackSizes, opts.TrackBloat, opts.ExcludeDatabases)
		if err != nil {
			if pqTLSName != "" {
				_ = pq.RegisterTLSConfig(pqTLSName, nil)
			}
			return nil, err
		}
		return &Exporter{Collector: collector, close: func() {
			_ = collector.Close()
			if pqTLSName != "" {
				_ = pq.RegisterTLSConfig(pqTLSName, nil)
			}
		}}, nil

	case TypeMysql:
		userPass := fmt.Sprintf("%s:%s", cfg.Credentials.Username, cfg.Credentials.Password)
		query := url.Values{}
		query.Set("timeout", fmt.Sprintf("%dms", opts.CollectTimeout.Milliseconds()))
		tlsParam := cfg.Params["tls"]
		tlsConfigName := ""
		if (cfg.TLS.CA != "" || (cfg.TLS.Cert != "" && cfg.TLS.Key != "")) && tlsParam != "false" {
			tlsCfg, err := common.DatabaseTLSConfig(cfg.TLS, tlsParam == "skip-verify")
			if err != nil {
				return nil, err
			}
			tlsConfigName = "coroot-" + cfg.Addr
			if err = gomysql.RegisterTLSConfig(tlsConfigName, tlsCfg); err != nil {
				return nil, err
			}
			tlsParam = tlsConfigName
		}
		if tlsParam == "" {
			tlsParam = "false"
		}
		query.Set("tls", tlsParam)
		dsn := fmt.Sprintf("%s@tcp(%s)/?%s", userPass, cfg.Addr, query.Encode())
		collector, err := mysql.New(dsn, logger, opts.ScrapeInterval, opts.CollectTimeout,
			opts.ChangeEmitter, cfg.Addr, opts.MaxTablesPerDB, opts.TrackSizes, opts.ExcludeDatabases)
		if err != nil {
			if tlsConfigName != "" {
				gomysql.DeregisterTLSConfig(tlsConfigName)
			}
			return nil, err
		}
		if cfg.LogService != "" {
			collector.StartErrorLog(opts.Logs, cfg.LogService, cfg.Description)
		}
		return &Exporter{Collector: collector, close: func() {
			_ = collector.Close()
			if tlsConfigName != "" {
				gomysql.DeregisterTLSConfig(tlsConfigName)
			}
		}}, nil

	case TypeRedis:
		redisOpts := redis.Options{
			User:                           cfg.Credentials.Username,
			Password:                       cfg.Credentials.Password,
			Namespace:                      "redis",
			ConnectionTimeouts:             opts.CollectTimeout,
			RedisMetricsOnly:               true,
			ExcludeLatencyHistogramMetrics: true,
		}
		if strings.Contains(cfg.Description, "elasticache:") || strings.Contains(cfg.Description, "memorystore:") || strings.Contains(cfg.Description, "ocicache:") { // managed services don't allow the CONFIG command
			redisOpts.ConfigCommandName = "-"
		}
		tls := cfg.Params["tls"]
		if tls == "" && strings.Contains(cfg.Description, "ocicache:") {
			tls = "skip-verify"
		}
		scheme := "redis"
		if tls != "" {
			scheme = "rediss"
			redisOpts.SkipTLSVerification = tls == "skip-verify"
		}
		dsn := fmt.Sprintf("%s://%s", scheme, cfg.Addr)
		collector, err := redis.NewRedisExporter(dsn, redisOpts)
		if err != nil {
			return nil, err
		}
		return &Exporter{Collector: collector}, nil

	case TypeMongodb:
		collector := mongo.New(
			cfg.Addr,
			cfg.Credentials.Username,
			cfg.Credentials.Password,
			cfg.TLS,
			cfg.Params,
			opts.ScrapeInterval,
			opts.CollectTimeout,
			logger,
			opts.ChangeEmitter,
			cfg.Addr,
			opts.MaxTablesPerDB,
			opts.TrackSizes,
		)
		return &Exporter{Collector: collector, close: func() { _ = collector.Close() }}, nil

	case TypeMemcached:
		collector := memcached.New(
			cfg.Addr,
			opts.CollectTimeout,
			level.NewFilter(&promLogger{l: logger}, level.AllowInfo()),
			nil,
		)
		return &Exporter{Collector: collector}, nil
	}
	return nil, fmt.Errorf("unsupported target type: %s", cfg.Type)
}

type promLogger struct {
	l logger.Logger
}

func (l *promLogger) Log(keyvals ...interface{}) error {
	l.l.Info(keyvals...)
	return nil
}
