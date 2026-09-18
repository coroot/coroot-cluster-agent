package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
	"github.com/coroot/coroot-cluster-agent/schema"

	"github.com/coroot/logger"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	picoSeconds = 1e12
)

var reVersion = regexp.MustCompile(`^(\d+)\.(\d+)`)

type Collector struct {
	ctx          context.Context
	host         string
	db           *sql.DB
	logger       logger.Logger
	errorLog     *ErrorLogReader
	topN         int
	cancelFunc   context.CancelFunc
	lock         sync.RWMutex
	scrapeErrors map[string]bool
	isUp         bool

	scrapeInterval time.Duration
	collectTimeout time.Duration

	globalVariables  map[string]string
	globalStatus     map[string]string
	perfschemaPrev   *statementsSummarySnapshot
	perfschemaCurr   *statementsSummarySnapshot
	activePrev       *activeStatementsSnapshot
	activeCurr       *activeStatementsSnapshot
	lockWaits        *lockWaits
	innodbTrx        *innodbTrx
	innodbCounters   *innodbCounters
	binlogStats      *binlogStats
	groupReplication *groupReplication
	replicaStatuses  []*ReplicaStatus
	ioByTablePrev    *ioByTableSnapshot
	ioByTableCurr    *ioByTableSnapshot

	invalidQueries   map[string]bool
	excludeDatabases map[string]bool

	dbTracker          *databaseTracker
	emitter            dbtracker.ChangeEmitter
	targetAddr         string
	prevSettingsText   string
	isMariaDB          bool
	isGalera           bool
	hasUndoTablespaces bool
	writableVariables  map[string]bool
}

func New(dsn string, logger logger.Logger, scrapeInterval, collectTimeout time.Duration,
	emitter dbtracker.ChangeEmitter, targetAddr string, maxTablesPerDB int,
	trackSizes bool, excludeDatabases []string) (*Collector, error) {

	ctx, cancelFunc := context.WithCancel(context.Background())
	exclude := make(map[string]bool, len(excludeDatabases))
	for _, db := range excludeDatabases {
		exclude[db] = true
	}
	c := &Collector{
		ctx:            ctx,
		logger:         logger,
		cancelFunc:     cancelFunc,
		scrapeInterval: scrapeInterval,
		collectTimeout: collectTimeout,
		emitter:        emitter,
		targetAddr:     targetAddr,

		globalStatus:     map[string]string{},
		globalVariables:  map[string]string{},
		invalidQueries:   map[string]bool{},
		excludeDatabases: exclude,
	}
	var err error
	c.db, err = sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	c.db.SetMaxOpenConns(1)
	trackSchema := c.emitter != nil
	if trackSchema || trackSizes {
		c.dbTracker = newDatabaseTracker(c.db, maxTablesPerDB, trackSchema, trackSizes, excludeDatabases, logger)
	}
	pingCtx, pingCancelFunc := context.WithTimeout(ctx, collectTimeout)
	defer pingCancelFunc()
	if err := c.db.PingContext(pingCtx); err != nil {
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
	if c.errorLog != nil {
		c.errorLog.Stop()
	}
	return c.db.Close()
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if !c.isUp {
		ch <- common.Gauge(dUp, 0)
		for e := range c.scrapeErrors {
			ch <- common.Gauge(dScrapeError, 1, e, "")
		}
		return
	}
	ch <- common.Gauge(dUp, 1)
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
	if c.lockWaits != nil {
		for _, q := range c.lockWaits.locked {
			ch <- common.Gauge(dLockedQueries, q.count, q.schema, q.query)
		}
		for _, q := range c.lockWaits.awaiting {
			ch <- common.Gauge(dLockAwaitingQueries, q.count, q.schema, q.query)
		}
	}
	c.innodbTrxMetrics(ch)
	c.replicationMetrics(ch)
	c.tableSizeMetrics(ch)
	metricFromVariable(ch, dConnectionsMax, "max_connections", prometheus.GaugeValue, c.globalVariables)
	metricFromVariable(ch, dConnectionsCurrent, "Threads_connected", prometheus.GaugeValue, c.globalStatus)
	metricFromVariable(ch, dConnectionsTotal, "Connections", prometheus.CounterValue, c.globalStatus)
	metricFromVariable(ch, dConnectionsAborted, "Aborted_connects", prometheus.CounterValue, c.globalStatus)
	metricFromVariable(ch, dConnectionErrorsMaxConnections, "Connection_errors_max_connections", prometheus.CounterValue, c.globalStatus)
	metricFromVariable(ch, dThreadsRunning, "Threads_running", prometheus.GaugeValue, c.globalStatus)
	metricFromVariable(ch, dTmpDiskTables, "Created_tmp_disk_tables", prometheus.CounterValue, c.globalStatus)
	c.galeraMetrics(ch)
	c.groupReplicationMetrics(ch)
	c.innodbMetrics(ch)
	c.innodbCountersMetrics(ch)
	c.binlogMetrics(ch)
	metricFromVariable(ch, dTableLocksWaited, "Table_locks_waited", prometheus.CounterValue, c.globalStatus)
	metricFromVariable(ch, dTableLocksImmediate, "Table_locks_immediate", prometheus.CounterValue, c.globalStatus)
	metricFromVariable(ch, dBytesReceived, "Bytes_received", prometheus.CounterValue, c.globalStatus)
	metricFromVariable(ch, dBytesSent, "Bytes_sent", prometheus.CounterValue, c.globalStatus)
	metricFromVariable(ch, dQueries, "Questions", prometheus.CounterValue, c.globalStatus)
	metricFromVariable(ch, dSlowQueries, "Slow_queries", prometheus.CounterValue, c.globalStatus)
}

func (c *Collector) snapshot() {
	timeout := c.scrapeInterval - time.Second
	if timeout <= 0 {
		timeout = time.Second
	}

	ctx, cancelFunc := context.WithTimeout(c.ctx, timeout)
	defer cancelFunc()

	c.lock.Lock()
	defer c.lock.Unlock()

	c.scrapeErrors = map[string]bool{}

	if err := c.updateVariables(ctx, "SHOW GLOBAL VARIABLES", c.globalVariables); err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		c.isUp = false
		return
	}
	c.isUp = true
	c.isMariaDB = strings.Contains(strings.ToLower(c.globalVariables["version"]), "mariadb")
	c.hasUndoTablespaces = !c.isMariaDB && versionAtLeast(c.globalVariables["version"], 8, 0)
	if err := c.updateVariables(ctx, "SHOW GLOBAL STATUS", c.globalStatus); err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		return
	}
	c.isGalera = false
	if _, ok := c.globalStatus["wsrep_cluster_size"]; ok {
		c.isGalera = wsrepEnabled(c.globalVariables)
	}
	if err := c.updateReplicationStatus(ctx); err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		return
	}
	c.perfschemaPrev = c.perfschemaCurr
	var err error
	c.perfschemaCurr, err = c.queryStatementsSummary(ctx, c.perfschemaPrev)
	if err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		return
	}
	c.activePrev = c.activeCurr
	if c.activeCurr, err = c.queryActiveStatements(ctx, c.activePrev); err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		c.activeCurr = nil
	}
	c.ioByTablePrev = c.ioByTableCurr
	c.ioByTableCurr, err = c.queryTableIOWaits(ctx)
	if err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		return
	}

	c.lockWaitsSnapshot(ctx)
	c.innodbTrxSnapshot(ctx)
	c.innodbCountersSnapshot(ctx)
	c.binlogSnapshot(ctx)

	if c.globalVariables["group_replication_group_name"] != "" {
		c.groupReplicationSnapshot(ctx)
	} else {
		c.groupReplication = nil
	}

	if c.emitter != nil {
		c.trackSettingsChanges(ctx)
	}
	if c.dbTracker != nil {
		c.dbTracker.Track(ctx, c.emitter, c.targetAddr)
	}
}

func (c *Collector) loadWritableVariableNames(ctx context.Context) error {
	var query string
	if c.isMariaDB {
		query = "SELECT VARIABLE_NAME FROM information_schema.SYSTEM_VARIABLES WHERE READ_ONLY = 'NO'"
	} else {
		query = "SELECT VARIABLE_NAME FROM performance_schema.variables_info WHERE VARIABLE_SOURCE != 'COMPILED'"
	}
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	c.writableVariables = map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			c.logger.Warning(err)
			continue
		}
		c.writableVariables[name] = true
	}
	return nil
}

func (c *Collector) trackSettingsChanges(ctx context.Context) {
	if err := c.loadWritableVariableNames(ctx); err != nil {
		c.logger.Warning("failed to load writable variable names:", err)
		return
	}

	names := make([]string, 0, len(c.writableVariables))
	for name := range c.writableVariables {
		names = append(names, name)
	}
	sort.Strings(names)
	var buf strings.Builder
	for _, name := range names {
		fmt.Fprintf(&buf, "%s = %s\n", name, c.globalVariables[name])
	}
	curr := buf.String()
	if c.prevSettingsText != "" && curr != c.prevSettingsText {
		diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A:        difflib.SplitLines(c.prevSettingsText),
			B:        difflib.SplitLines(curr),
			FromFile: "global_variables",
			ToFile:   "global_variables",
			Context:  3,
		})
		c.emitter.Emit(schema.Change{
			Object: "global_variables",
			Type:   schema.ChangeTypeChanged,
			Diff:   diff,
		}, "mysql", c.targetAddr)
	}
	c.prevSettingsText = curr
}

func (c *Collector) tableSizeMetrics(ch chan<- prometheus.Metric) {
	if c.dbTracker == nil || !c.dbTracker.trackSizes {
		return
	}
	for dbName, snap := range c.dbTracker.DBSizes {
		ch <- common.Gauge(dDbSize, snap.DatabaseSize, dbName)
		for _, t := range snap.Tables {
			ch <- common.Gauge(dTableSize, t.Size, dbName, t.Table)
		}
	}
	for _, g := range c.dbTracker.TableGrowth {
		ch <- common.Gauge(dTableSizeGrowth, g.Growth, g.DB, g.Table)
	}
}

func versionAtLeast(version string, major, minor int) bool {
	m := reVersion.FindStringSubmatch(version)
	if m == nil {
		return false
	}
	maj, err := strconv.Atoi(m[1])
	if err != nil {
		return false
	}
	min, err := strconv.Atoi(m[2])
	if err != nil {
		return false
	}
	return maj > major || (maj == major && min >= minor)
}

func metricFromVariable(ch chan<- prometheus.Metric, desc *prometheus.Desc, name string, typ prometheus.ValueType, variables map[string]string, convert ...func(float64) float64) bool {
	v, ok := variables[name]
	if !ok {
		return false
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return false
	}
	for _, c := range convert {
		if c != nil {
			f = c(f)
		}
	}
	ch <- prometheus.MustNewConstMetric(desc, typ, f)
	return true
}
