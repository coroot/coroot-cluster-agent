package mongo

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
	"github.com/coroot/coroot-cluster-agent/schema"
	"github.com/coroot/logger"
	"github.com/pmezard/go-difflib/difflib"
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

	dDbSize               = common.Desc("mongo_database_size_bytes", "Total size of the database in bytes", "db")
	dCollectionSize       = common.Desc("mongo_collection_size_bytes", "Total size of the collection in bytes", "db", "collection")
	dCollectionSizeGrowth = common.Desc("mongo_collection_size_growth_bytes_per_second", "Collection size growth rate in bytes per second", "db", "collection")
)

const (
	StatusReplicationNotEnabled        = 76
	StatusReplicationNotYetInitialized = 94
)

type Collector struct {
	ctx        context.Context
	cancelFunc context.CancelFunc

	host       string
	client     *mongo.Client
	clientOpts *options.ClientOptions
	clientLock sync.Mutex
	logger     logger.Logger

	scrapeInterval time.Duration
	collectTimeout time.Duration

	lock         sync.RWMutex
	scrapeErrors map[string]bool

	serverVersion string
	rsStatus      *ReplStatus

	dbTracker        *databaseTracker
	emitter          dbtracker.ChangeEmitter
	targetAddr       string
	prevSettingsText string
}

func New(host, username, password string, scrapeInterval, collectTimeout time.Duration,
	logger logger.Logger, emitter dbtracker.ChangeEmitter, targetAddr string,
	maxTablesPerDB int, trackSizes bool) *Collector {

	ctx, cancelFunc := context.WithCancel(context.Background())
	c := &Collector{
		ctx:            ctx,
		cancelFunc:     cancelFunc,
		host:           host,
		logger:         logger,
		scrapeInterval: scrapeInterval,
		collectTimeout: collectTimeout,
		scrapeErrors:   map[string]bool{},
		emitter:        emitter,
		targetAddr:     targetAddr,
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
	trackSchema := c.emitter != nil
	if trackSchema || trackSizes {
		c.dbTracker = newDatabaseTracker(maxTablesPerDB, trackSchema, trackSizes, logger)
	}
	go func() {
		ticker := time.NewTicker(scrapeInterval)
		c.snapshot()
		for {
			select {
			case <-ticker.C:
				c.snapshot()
			case <-ctx.Done():
				c.logger.Info("stopping mongo collector")
				return
			}
		}
	}()
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
	c.cancelFunc()
	c.clientLock.Lock()
	defer c.clientLock.Unlock()
	if c.client != nil {
		err := c.client.Disconnect(context.Background())
		c.client = nil
		return err
	}
	return nil
}

func (c *Collector) snapshot() {
	timeout := c.scrapeInterval - time.Second
	if timeout <= 0 {
		timeout = time.Second
	}

	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()

	if err := c.connectAndPing(ctx); err != nil {
		c.logger.Warning(err)
		c.lock.Lock()
		c.scrapeErrors = map[string]bool{err.Error(): true}
		c.serverVersion = ""
		c.rsStatus = nil
		c.lock.Unlock()
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	c.scrapeErrors = map[string]bool{}

	if version, err := c.collectBuildInfo(ctx); err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
	} else {
		c.serverVersion = version
	}

	if rs, err := c.collectReplStatus(ctx); err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
	} else {
		c.rsStatus = rs
	}

	if c.emitter != nil {
		c.trackSettingsChanges(ctx)
	}
	if c.dbTracker != nil {
		c.dbTracker.client = c.client
		c.dbTracker.Track(ctx, c.emitter, c.targetAddr)
	}
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancelFunc := context.WithTimeout(c.ctx, c.collectTimeout)
	defer cancelFunc()

	if err := c.connectAndPing(ctx); err != nil {
		c.logger.Warning(err)
		ch <- common.Gauge(dUp, 0)
		ch <- common.Gauge(dScrapeError, 1, err.Error(), "")
		return
	}
	ch <- common.Gauge(dUp, 1)

	c.lock.RLock()
	defer c.lock.RUnlock()

	if c.serverVersion != "" {
		ch <- common.Gauge(dInfo, 1, c.serverVersion)
	}

	if c.rsStatus != nil && c.rsStatus.ReplicaSet != "" {
		for _, m := range c.rsStatus.Members {
			if !m.Self {
				continue
			}
			ch <- common.Gauge(dRsStatus, 1, c.rsStatus.ReplicaSet, m.State)
			if v := float64(m.LastApplied.Time().UnixMilli()); v > 0 {
				ch <- common.Gauge(dRsLastApplied, v)
			}
		}
	}

	c.sizesMetrics(ch)

	if len(c.scrapeErrors) > 0 {
		for e := range c.scrapeErrors {
			ch <- common.Gauge(dScrapeError, 1, "", e)
		}
	} else {
		ch <- common.Gauge(dScrapeError, 0, "", "")
	}
}

type BuildInfo struct {
	Version string `bson:"version"`
}

func (c *Collector) collectBuildInfo(ctx context.Context) (string, error) {
	res := c.client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: "1"}})
	var bi BuildInfo
	if err := res.Decode(&bi); err != nil {
		return "", err
	}
	return bi.Version, nil
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

func (c *Collector) collectReplStatus(ctx context.Context) (*ReplStatus, error) {
	res := c.client.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: "1"}})
	var s ReplStatus
	if err := res.Decode(&s); err != nil {
		if e, ok := err.(mongo.CommandError); ok {
			switch e.Code {
			case StatusReplicationNotEnabled, StatusReplicationNotYetInitialized:
				return nil, nil
			}
		}
		return nil, err
	}
	return &s, nil
}

var settingsMetadataKeys = map[string]bool{
	"ok":            true,
	"$clusterTime":  true,
	"operationTime": true,
}

func (c *Collector) trackSettingsChanges(ctx context.Context) {
	res := c.client.Database("admin").RunCommand(ctx, bson.D{{Key: "getParameter", Value: "*"}})
	var params bson.M
	if err := res.Decode(&params); err != nil {
		c.logger.Warning("getParameter:", err)
		return
	}

	keys := make([]string, 0, len(params))
	for k := range params {
		if !settingsMetadataKeys[k] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	var buf strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&buf, "%s = %v\n", k, params[k])
	}
	curr := buf.String()

	if c.prevSettingsText != "" && curr != c.prevSettingsText {
		diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A:        difflib.SplitLines(c.prevSettingsText),
			B:        difflib.SplitLines(curr),
			FromFile: "parameters",
			ToFile:   "parameters",
			Context:  3,
		})
		c.emitter.Emit(schema.Change{
			Object: "parameters",
			Type:   schema.ChangeTypeChanged,
			Diff:   diff,
		}, "mongodb", c.targetAddr)
	}
	c.prevSettingsText = curr
}

func (c *Collector) sizesMetrics(ch chan<- prometheus.Metric) {
	if c.dbTracker == nil || !c.dbTracker.trackSizes {
		return
	}
	for dbName, snap := range c.dbTracker.DBSizes {
		ch <- common.Gauge(dDbSize, snap.DatabaseSize, dbName)
		for _, t := range snap.Tables {
			ch <- common.Gauge(dCollectionSize, t.Size, dbName, t.Table)
		}
	}
	for _, g := range c.dbTracker.TableGrowth {
		ch <- common.Gauge(dCollectionSizeGrowth, g.Growth, g.DB, g.Table)
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- dUp
	ch <- dScrapeError
	ch <- dInfo
	ch <- dRsStatus
	ch <- dRsLastApplied
	ch <- dDbSize
	ch <- dCollectionSize
	ch <- dCollectionSizeGrowth
}
