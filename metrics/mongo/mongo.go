package mongo

import (
	"context"
	"crypto/tls"
	"sync"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/logger"
	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	dUp            = common.Desc("mongo_up", "")
	dScrapeError   = common.Desc("mongo_scrape_error", "", "error", "warning")
	dInfo          = common.Desc("mongo_info", "", "server_version")
	dRsStatus      = common.Desc("mongo_rs_status", "", "rs", "role")
	dRsLastApplied = common.Desc("mongo_rs_last_applied_timestamp_ms", "")
)

const (
	StatusReplicationNotEnabled        = 76
	StatusReplicationNotYetInitialized = 94
)

type Collector struct {
	host       string
	client     *mongo.Client
	clientOpts *options.ClientOptions
	clientLock sync.Mutex
	logger     logger.Logger

	collectTimeout time.Duration
}

func New(host string, username, password, tlsOpt string, sni string, collectTimeout time.Duration, logger logger.Logger) *Collector {
	c := &Collector{
		logger:         logger,
		collectTimeout: collectTimeout,
	}
	c.clientOpts = options.Client().
		SetHosts([]string{host}).
		SetDirect(true).
		SetAppName("coroot-mongodb-agent").
		SetServerSelectionTimeout(collectTimeout).
		SetConnectTimeout(collectTimeout)
	if username != "" {
		c.clientOpts.SetAuth(options.Credential{
			Username: username,
			Password: password,
		})
	}
	if tlsOpt == "true" {
		c.clientOpts.SetTLSConfig(&tls.Config{
			ServerName: sni,
			MinVersion: tls.VersionTLS12,
		})
	}
	return c
}

func (c *Collector) connectAndPing(ctx context.Context) error {
	c.clientLock.Lock()
	defer c.clientLock.Unlock()
	var err error
	if c.client == nil {
		c.logger.Info("connecting to mongodb")
		if c.client, err = mongo.Connect(ctx, c.clientOpts); err != nil {
			return err
		}
	}
	if err = c.client.Ping(ctx, nil); err != nil {
		_ = c.client.Disconnect(ctx)
		c.client = nil
		return err
	}
	return nil
}

func (c *Collector) Close() error {
	c.clientLock.Lock()
	defer c.clientLock.Unlock()
	if c.client != nil {
		err := c.client.Disconnect(context.Background())
		c.client = nil
		return err
	}
	return nil
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), c.collectTimeout)
	defer cancelFunc()

	if err := c.connectAndPing(ctx); err != nil {
		c.logger.Warning(err)
		ch <- common.Gauge(dUp, 0)
		ch <- common.Gauge(dScrapeError, 1, err.Error(), "")
		return
	}
	ch <- common.Gauge(dUp, 1)

	scrapeErrors := map[string]bool{}

	if err := c.buildInfo(ctx, ch); err != nil {
		c.logger.Warning(err)
		scrapeErrors[err.Error()] = true
	}
	if err := c.replStatus(ctx, ch); err != nil {
		c.logger.Warning(err)
		scrapeErrors[err.Error()] = true
	}

	if len(scrapeErrors) > 0 {
		for e := range scrapeErrors {
			ch <- common.Gauge(dScrapeError, 1, "", e)
		}
	} else {
		ch <- common.Gauge(dScrapeError, 0, "", "")
	}
}

type BuildInfo struct {
	Version string `bson:"version"`
}

func (c *Collector) buildInfo(ctx context.Context, ch chan<- prometheus.Metric) error {
	res := c.client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: "1"}})
	var bi BuildInfo
	err := res.Decode(&bi)
	if bi.Version != "" {
		ch <- common.Gauge(dInfo, 1, bi.Version)
	}
	return err
}

type Member struct {
	State       string             `bson:"stateStr"`
	Self        bool               `bson:"self"`
	LastApplied primitive.DateTime `bson:"lastAppliedWallTime"`
}

type ReplStatus struct {
	ReplicaSet string   `bson:"set"`
	Members    []Member `bson:"members"`
}

func (c *Collector) replStatus(ctx context.Context, ch chan<- prometheus.Metric) error {
	res := c.client.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: "1"}})
	var s ReplStatus
	err := res.Decode(&s)
	if err != nil {
		if e, ok := err.(mongo.CommandError); ok {
			switch e.Code {
			case StatusReplicationNotEnabled, StatusReplicationNotYetInitialized:
				return nil
			}
		}
		return err
	}
	if s.ReplicaSet == "" {
		return nil
	}
	for _, m := range s.Members {
		if !m.Self {
			continue
		}
		ch <- common.Gauge(dRsStatus, 1, s.ReplicaSet, m.State)
		if v := float64(m.LastApplied.Time().UnixMilli()); v > 0 {
			ch <- common.Gauge(dRsLastApplied, v)
		}
	}
	return nil
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- dUp
	ch <- dScrapeError
	ch <- dInfo
	ch <- dRsStatus
}
