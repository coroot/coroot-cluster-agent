package mysql

import (
	"context"
	"database/sql"
	"strconv"
	"sync"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"

	"github.com/coroot/logger"
	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	picoSeconds = 1e12
)

var (
	dUp          = common.Desc("mysql_up", "")
	dScrapeError = common.Desc("mysql_scrape_error", "", "error", "warning")
	dInfo        = common.Desc("mysql_info", "", "server_version", "server_id", "server_uuid")

	dQueryCalls     = common.Desc("mysql_top_query_calls_per_second", "", "schema", "query")
	dQueryTotalTime = common.Desc("mysql_top_query_time_per_second", "", "schema", "query")
	dQueryLockTime  = common.Desc("mysql_top_query_lock_time_per_second", "", "schema", "query")

	dReplicationIORunning  = common.Desc("mysql_replication_io_status", "", "source_server_id", "source_server_uuid", "state", "last_error")
	dReplicationSQLRunning = common.Desc("mysql_replication_sql_status", "", "source_server_id", "source_server_uuid", "state", "last_error")
	dReplicationLag        = common.Desc("mysql_replication_lag_seconds", "", "source_server_id", "source_server_uuid")

	dConnectionsMax     = common.Desc("mysql_connections_max", "")
	dConnectionsCurrent = common.Desc("mysql_connections_current", "")
	dConnectionsTotal   = common.Desc("mysql_connections_total", "")
	dConnectionsAborted = common.Desc("mysql_connections_aborted_total", "")

	dBytesReceived = common.Desc("mysql_traffic_received_bytes_total", "")
	dBytesSent     = common.Desc("mysql_traffic_sent_bytes_total", "")

	dQueries     = common.Desc("mysql_queries_total", "")
	dSlowQueries = common.Desc("mysql_slow_queries_total", "")

	dIOTime = common.Desc("mysql_top_table_io_wait_time_per_second", "", "schema", "table", "operation")
)

type Collector struct {
	host         string
	db           *sql.DB
	logger       logger.Logger
	topN         int
	cancelFunc   context.CancelFunc
	lock         sync.RWMutex
	scrapeErrors map[string]bool

	globalVariables map[string]string
	globalStatus    map[string]string
	perfschemaPrev  *statementsSummarySnapshot
	perfschemaCurr  *statementsSummarySnapshot
	replicaStatuses []*ReplicaStatus
	ioByTablePrev   *ioByTableSnapshot
	ioByTableCurr   *ioByTableSnapshot

	invalidQueries map[string]bool
}

func New(dsn string, logger logger.Logger, scrapeInterval time.Duration) (*Collector, error) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	c := &Collector{
		logger:          logger,
		cancelFunc:      cancelFunc,
		globalStatus:    map[string]string{},
		globalVariables: map[string]string{},
		invalidQueries:  map[string]bool{},
	}
	var err error
	c.db, err = sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	c.db.SetMaxOpenConns(1)
	if err := c.db.Ping(); err != nil {
		c.logger.Warning("probe failed:", err)
	}
	go func() {
		ticker := time.NewTicker(scrapeInterval)
		c.snapshot()
		for {
			select {
			case <-ticker.C:
				c.snapshot()
			case <-ctx.Done():
				c.logger.Info("stopping mysql collector")
				return
			}
		}
	}()

	return c, nil
}

func (c *Collector) Close() error {
	c.cancelFunc()
	return c.db.Close()
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	if err := c.db.Ping(); err != nil {
		c.logger.Warning("probe failed:", err)
		ch <- common.Gauge(dUp, 0)
		ch <- common.Gauge(dScrapeError, 1, err.Error(), "")
		return
	}
	ch <- common.Gauge(dUp, 1)
	c.lock.RLock()
	defer c.lock.RUnlock()
	if version := c.globalVariables["version"]; version != "" {
		ch <- common.Gauge(dInfo, 1, version, c.globalVariables["server_id"], c.globalVariables["server_uuid"])
	}

	if len(c.scrapeErrors) > 0 {
		for e := range c.scrapeErrors {
			ch <- common.Gauge(dScrapeError, 1, "", e)
		}
	} else {
		ch <- common.Gauge(dScrapeError, 0, "", "")
	}
	c.queryMetrics(ch, 20)
	c.ioMetrics(ch, 20)
	c.replicationMetrics(ch)
	metricFromVariable(ch, dConnectionsMax, "max_connections", prometheus.GaugeValue, c.globalVariables)
	metricFromVariable(ch, dConnectionsCurrent, "Threads_connected", prometheus.GaugeValue, c.globalStatus)
	metricFromVariable(ch, dConnectionsTotal, "Connections", prometheus.CounterValue, c.globalStatus)
	metricFromVariable(ch, dConnectionsAborted, "Aborted_connects", prometheus.CounterValue, c.globalStatus)
	metricFromVariable(ch, dBytesReceived, "Bytes_received", prometheus.CounterValue, c.globalStatus)
	metricFromVariable(ch, dBytesSent, "Bytes_sent", prometheus.CounterValue, c.globalStatus)
	metricFromVariable(ch, dQueries, "Questions", prometheus.CounterValue, c.globalStatus)
	metricFromVariable(ch, dSlowQueries, "Slow_queries", prometheus.CounterValue, c.globalStatus)
}

func (c *Collector) snapshot() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.scrapeErrors = map[string]bool{}

	if err := c.updateVariables("SHOW GLOBAL VARIABLES", c.globalVariables); err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		return
	}
	if err := c.updateVariables("SHOW GLOBAL STATUS", c.globalStatus); err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		return
	}
	if err := c.updateReplicationStatus(); err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		return
	}
	c.perfschemaPrev = c.perfschemaCurr
	var err error
	c.perfschemaCurr, err = c.queryStatementsSummary(c.perfschemaPrev)
	if err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		return
	}
	c.ioByTablePrev = c.ioByTableCurr
	c.ioByTableCurr, err = c.queryTableIOWaits()
	if err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		return
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- dUp
	ch <- dScrapeError
	ch <- dInfo
	ch <- dQueryCalls
	ch <- dQueryTotalTime
	ch <- dQueryLockTime
	ch <- dReplicationIORunning
	ch <- dReplicationSQLRunning
	ch <- dReplicationLag
	ch <- dConnectionsMax
	ch <- dConnectionsCurrent
	ch <- dConnectionsTotal
	ch <- dConnectionsAborted
	ch <- dBytesReceived
	ch <- dBytesSent
	ch <- dQueries
	ch <- dSlowQueries
	ch <- dIOTime
}

func metricFromVariable(ch chan<- prometheus.Metric, desc *prometheus.Desc, name string, typ prometheus.ValueType, variables map[string]string) {
	v, ok := variables[name]
	if !ok {
		return
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		ch <- prometheus.MustNewConstMetric(desc, typ, f)
	}
}
