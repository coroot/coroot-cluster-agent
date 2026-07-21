package postgres

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blang/semver"
	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
	"github.com/coroot/logger"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	topQueriesN        = 20
	hardQuerySizeLimit = 4096
)

var (
	dUp          = desc("pg_up", "Is the server reachable")
	dProbe       = desc("pg_probe_seconds", "Empty query execution time")
	dScrapeError = desc("pg_scrape_error", "Scrape error", "error", "warning")

	dInfo     = desc("pg_info", "Server info", "server_version")
	dSettings = desc("pg_setting", "Value of the pg_setting variable", "name", "unit")

	dConnections = desc("pg_connections", "Number of database connections", "db", "user", "state", "wait_event_type", "query")

	dAutovacuumWorkers = desc("pg_autovacuum_workers", "Number of running autovacuum worker processes")

	dLatency = desc("pg_latency_seconds", "Query execution time", "summary")

	dDbQueries = desc("pg_db_queries_per_second", "Number of queries executed in the database per second", "db")

	dTopQueryCalls  = desc("pg_top_query_calls_per_second", "Number of times the query was executed", "db", "user", "query")
	dTopQueryTime   = desc("pg_top_query_time_per_second", "Time spent executing the query", "db", "user", "query")
	dTopQueryIOTime = desc("pg_top_query_io_time_per_second", "Time the query spent awaiting IO", "db", "user", "query")

	dLockAwaitingQueries = desc("pg_lock_awaiting_queries", "Number of queries awaiting a lock", "db", "user", "blocking_query")

	dWalReceiverStatus = desc("pg_wal_receiver_status", "WAL receiver status: 1 if the receiver is connected, otherwise 0", "sender_host", "sender_port")
	dWalReplayPaused   = desc("pg_wal_replay_paused", "Whether WAL replay paused or not")
	dWalCurrentLsn     = desc("pg_wal_current_lsn", "Current WAL sequence number")
	dWalReceiveLsn     = desc("pg_wal_receive_lsn", "WAL sequence number that has been received and synced to disk by streaming replication")
	dWalReplyLsn       = desc("pg_wal_reply_lsn", "WAL sequence number that has been replayed during recovery")

	dCheckpointsScheduled    = desc("pg_checkpoints_scheduled_total", "Number of scheduled checkpoints, including skipped ones", "type")
	dCheckpoints             = desc("pg_checkpoints_total", "Number of checkpoints that have been completed")
	dRestartpoints           = desc("pg_restartpoints_total", "Number of restartpoints that have been completed on a standby")
	dBuffersWritten          = desc("pg_buffers_written_total", "Total number of dirty buffers flushed to disk", "source")
	dTimeSinceLastCheckpoint = desc("pg_time_since_last_checkpoint_seconds", "Seconds since the last checkpoint observed by the agent")
	dWalSinceLastCheckpoint  = desc("pg_wal_since_last_checkpoint_bytes", "Amount of WAL written since the last completed checkpoint (to be replayed in the case of a crash)")
	dWalSize                 = desc("pg_wal_size_bytes", "Size of the WAL directory")
	dReplicationSlotRetained = desc("pg_replication_slot_retained_wal_bytes", "Amount of WAL retained for the replication slot", "slot", "active", "wal_status")
	dXidAge                  = desc("pg_xid_age", "Transactions since the oldest unfrozen transaction ID (age of datfrozenxid)", "db")
	dMultixactAge            = desc("pg_multixact_age", "Multixacts since the oldest unfrozen multixact ID (age of datminmxid)", "db")
	dOldestXminAge           = desc("pg_oldest_xmin_age", "Age, in transactions, of the oldest transaction ID held back from freezing, by holder", "holder")
	dWalArchivedSegments     = desc("pg_wal_archived_segments_total", "Number of WAL files successfully archived")
	dWalArchiveFailures      = desc("pg_wal_archive_failures_total", "Number of failed attempts to archive WAL files")
	dWalArchivingStatus      = desc("pg_wal_archiving_status", "1 if the last WAL archive attempt succeeded, 0 if it failed")

	dDbSize          = desc("pg_database_size_bytes", "Total size of the database in bytes", "db")
	dTableSize       = desc("pg_table_size_bytes", "Total size of the table in bytes including indexes and TOAST", "db", "schema", "table")
	dTableSizeGrowth = desc("pg_table_size_growth_bytes_per_second", "Table size growth rate in bytes per second", "db", "schema", "table")

	dDbTableBloat = desc("pg_db_table_bloat_bytes", "Estimated wasted space across all tables of the database", "db")
	dDbIndexBloat = desc("pg_db_index_bloat_bytes", "Estimated wasted space across all indexes of the database", "db")
	dTableBloat   = desc("pg_table_bloat_bytes", "Estimated wasted space in the table heap", "db", "schema", "table")
	dIndexBloat   = desc("pg_index_bloat_bytes", "Estimated wasted space in the index", "db", "schema", "table", "index")

	dTableDeadTupleBytes = desc("pg_table_dead_tuple_bytes", "Estimated size of dead tuples not yet reclaimed by vacuum", "db", "schema", "table")
	dTableDeadTuples     = desc("pg_table_dead_tuples", "Number of dead tuples not yet reclaimed by vacuum", "db", "schema", "table")
	dTableLiveTuples     = desc("pg_table_live_tuples", "Estimated number of live tuples", "db", "schema", "table")

	dTableSecondsSinceAutovacuum = desc("pg_table_seconds_since_last_autovacuum", "Seconds since the last autovacuum of the table", "db", "schema", "table")
	dTableVacuumInProgress       = desc("pg_table_vacuum_in_progress", "1 if a vacuum is currently running on the table; not reported otherwise", "db", "schema", "table")
	dTableVacuumThrottled        = desc("pg_table_vacuum_throttled", "1 if the running vacuum is sleeping on the cost-based delay (VacuumDelay) at scrape time, 0 otherwise; reported only while a vacuum is in progress", "db", "schema", "table")

	dTableModsSinceAnalyze    = desc("pg_table_mods_since_analyze", "Rows modified (inserted/updated/deleted) since the table's statistics were last analyzed", "db", "schema", "table")
	dTableReltuples           = desc("pg_table_reltuples", "Planner's estimate of the number of live rows in the table (pg_class.reltuples)", "db", "schema", "table")
	dTableSecondsSinceAnalyze = desc("pg_table_seconds_since_last_analyze", "Seconds since the table's planner statistics were last refreshed (ANALYZE or autoanalyze)", "db", "schema", "table")
	dTableSetting             = desc("pg_table_setting", "Per-table autovacuum/autoanalyze reloption overrides carried as labels (empty = not overridden); reported only for monitored tables that override a setting", "db", "schema", "table", "autovacuum_disabled", "autovacuum_vacuum_scale_factor", "autovacuum_vacuum_threshold", "autovacuum_vacuum_cost_delay", "autovacuum_vacuum_cost_limit", "autovacuum_analyze_scale_factor", "autovacuum_analyze_threshold")
)

type QueryKey struct {
	Query string
	DB    string
	User  string
}

func (k QueryKey) EqualByQueryPrefix(other QueryKey) bool {
	if k.User != other.User || k.DB != other.DB {
		return false
	}
	if strings.HasPrefix(k.Query, other.Query) {
		return true
	}
	return false
}

type ConnectionKey struct {
	QueryKey
	State         string
	WaitEventType string
}

type Collector struct {
	ctx           context.Context
	ctxCancelFunc context.CancelFunc

	scrapeInterval time.Duration
	collectTimeout time.Duration

	db          *sql.DB
	origVersion string

	statsDumpInterval time.Duration
	ssCurr            *ssSnapshot
	ssPrev            *ssSnapshot
	saCurr            *saSnapshot
	saPrev            *saSnapshot
	settings          []Setting
	replicationStatus *replicationStatus

	cpPrev           *checkpointStats
	cpTimed          float64
	cpRequested      float64
	cpDone           float64
	cpRestartsDone   float64
	cpBuffers        float64
	cpWalBytes       sql.Null[float64]
	lastCheckpointAt time.Time

	walSize          sql.Null[float64]
	replicationSlots []replicationSlot
	archPrev         *archiverStats
	archStats        *archiverStats
	archArchived     float64
	archFailed       float64
	wraparound       *wraparoundStats

	scrapeErrors map[string]bool

	dbTracker        *databaseTracker
	emitter          dbtracker.ChangeEmitter
	targetAddr       string
	prevSettingsText string

	lock   sync.RWMutex
	logger logger.Logger
}

func New(dsn string, scrapeInterval, collectTimeout time.Duration, logger logger.Logger, emitter dbtracker.ChangeEmitter, targetAddr string, maxTablesPerDB int, trackSizes, trackBloat bool, excludeDatabases []string) (*Collector, error) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	c := &Collector{
		ctx:            ctx,
		logger:         logger,
		ctxCancelFunc:  cancelFunc,
		scrapeErrors:   map[string]bool{},
		scrapeInterval: scrapeInterval,
		collectTimeout: collectTimeout,
		targetAddr:     targetAddr,
		emitter:        emitter,
	}
	var err error
	c.db, err = sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	c.db.SetMaxOpenConns(1)
	trackSchema := c.emitter != nil
	if trackSchema || trackSizes || trackBloat {
		c.dbTracker = newDatabaseTracker(c.db, dsn, maxTablesPerDB, trackSchema, trackSizes, trackBloat, excludeDatabases, logger)
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
				c.logger.Info("stopping pg collector")
				return
			}
		}
	}()
	return c, nil
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

	c.origVersion = ""
	var version semver.Version
	var rawVersion string
	err := c.db.QueryRowContext(ctx, `SELECT setting FROM pg_settings WHERE name='server_version'`).Scan(&rawVersion)
	if err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		return
	}
	c.origVersion, version, err = parsePgVersion(rawVersion)
	if err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		return
	}

	if c.settings, err = c.getSettings(ctx); err != nil {
		c.scrapeErrors[err.Error()] = true
		c.logger.Warning(err)
	}

	if c.replicationStatus, err = c.getReplicationStatus(ctx, version); err != nil {
		c.scrapeErrors[err.Error()] = true
		c.logger.Warning(err)
	}

	if err = c.getCheckpointStats(ctx, version); err != nil {
		c.scrapeErrors[err.Error()] = true
		c.logger.Warning(err)
	}

	if err = c.getWalStats(ctx, version); err != nil {
		c.scrapeErrors[err.Error()] = true
		c.logger.Warning(err)
	}

	if err = c.getWraparoundStats(ctx, version); err != nil {
		c.scrapeErrors[err.Error()] = true
		c.logger.Warning(err)
	}

	querySizeLimit := 0
	for _, s := range c.settings {
		if s.Name == "track_activity_query_size" {
			switch s.Unit {
			case "B":
				querySizeLimit = int(s.Value)
			case "kB":
				querySizeLimit = int(s.Value) * 1024
			default:
				querySizeLimit = int(s.Value)
			}
			break
		}
	}
	if querySizeLimit == 0 || querySizeLimit > hardQuerySizeLimit {
		querySizeLimit = hardQuerySizeLimit
	}

	c.ssPrev = c.ssCurr
	c.saPrev = c.saCurr
	prevStatements := map[statementId]ssRow{}
	if c.ssPrev != nil {
		prevStatements = c.ssPrev.rows
	}
	c.ssCurr, err = c.getStatStatements(ctx, version, querySizeLimit, prevStatements)
	if err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		return
	}
	c.saCurr, err = c.getPgStatActivity(ctx, version, querySizeLimit)
	if err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		return
	}

	if c.emitter != nil {
		c.trackSettingsChanges()
	}
	if c.dbTracker != nil {
		c.dbTracker.Track(ctx, c.emitter, c.targetAddr)
	}
}

func (c *Collector) summaries() (map[QueryKey]*QuerySummary, time.Duration) {
	if c.saCurr == nil || c.saPrev == nil || c.ssCurr == nil || c.ssPrev == nil {
		return nil, 0
	}
	res := map[QueryKey]*QuerySummary{}
	getOrCreateSummary := func(k QueryKey, searchByPrefix bool) *QuerySummary {
		s := res[k]
		if s == nil && searchByPrefix {
			for qk, ss := range res {
				if qk.EqualByQueryPrefix(k) {
					s = ss
					break
				}
			}
		}
		if s == nil {
			s = &QuerySummary{}
			res[k] = s
		}
		return s
	}

	for id, r := range c.ssCurr.rows {
		getOrCreateSummary(r.QueryKey(id), false).updateFromStatStatements(r, c.ssPrev.rows[id])
	}
	for _, conn := range c.saCurr.connections {
		getOrCreateSummary(conn.QueryKey(), true).updateFromStatActivity(c.saPrev.ts, c.saCurr.ts, conn)
	}
	for pid, prev := range c.saPrev.connections {
		if !prev.IsClientBackend() || prev.State.String != "active" {
			continue
		}
		curr, ok := c.saCurr.connections[pid]
		if ok && curr.State.String == "active" && curr.QueryStart.Time.Equal(prev.QueryStart.Time) { // still executing
			continue
		}
		// prev query finished
		getOrCreateSummary(prev.QueryKey(), true).correctFromPrevStatActivity(c.saPrev.ts, prev)
	}
	return res, c.ssCurr.ts.Sub(c.ssPrev.ts)
}

func (c *Collector) connectionMetrics(ch chan<- prometheus.Metric) {
	if c.saCurr == nil {
		return
	}
	byPid := map[int]QueryKey{}
	awaitingQueriesByBlockingPid := map[int]float64{}
	connectionsByKey := map[ConnectionKey]float64{}

	for pid, conn := range c.saCurr.connections {
		queryKey := conn.QueryKey()
		byPid[pid] = queryKey
		if conn.BlockingPid.Int32 > 0 {
			awaitingQueriesByBlockingPid[int(conn.BlockingPid.Int32)]++
		}
		key := ConnectionKey{
			QueryKey:      queryKey,
			State:         conn.State.String,
			WaitEventType: conn.WaitEventType.String,
		}
		connectionsByKey[key]++
	}

	for k, count := range connectionsByKey {
		ch <- gauge(dConnections, count, k.DB, k.User, k.State, k.WaitEventType, k.Query)
	}
	ch <- gauge(dAutovacuumWorkers, c.saCurr.autovacuumWorkers)

	awaitingQueriesByBlockingQuery := map[QueryKey]float64{}
	for blockingPid, awaitingQueries := range awaitingQueriesByBlockingPid {
		blockingQuery, ok := byPid[blockingPid]
		if !ok {
			continue
		}
		awaitingQueriesByBlockingQuery[blockingQuery] += awaitingQueries
	}
	for blockingQuery, awaitingQueries := range awaitingQueriesByBlockingQuery {
		ch <- gauge(dLockAwaitingQueries, awaitingQueries, blockingQuery.DB, blockingQuery.User, blockingQuery.Query)
	}
}

func (c *Collector) queryMetrics(ch chan<- prometheus.Metric) {
	summaries, interval := c.summaries()
	if summaries == nil {
		c.logger.Warning("no summaries")
		return
	}

	latency := NewLatencySummary()
	queriesByDB := map[string]float64{}
	for k, summary := range summaries {
		latency.Add(summary.TotalTime, uint64(summary.Queries))
		queriesByDB[k.DB] += summary.Queries
	}
	for s, v := range latency.GetSummaries(50, 75, 95, 99) {
		ch <- gauge(dLatency, v, s)
	}

	for db, queries := range queriesByDB {
		ch <- gauge(dDbQueries, queries/interval.Seconds(), db)
	}

	for k, summary := range top(summaries, topQueriesN) {
		ch <- gauge(dTopQueryCalls, summary.Queries/interval.Seconds(), k.DB, k.User, k.Query)
		ch <- gauge(dTopQueryTime, summary.TotalTime/interval.Seconds(), k.DB, k.User, k.Query)
		ch <- gauge(dTopQueryIOTime, summary.IOTime/interval.Seconds(), k.DB, k.User, k.Query)
	}
}

func (c *Collector) tableSizeMetrics(ch chan<- prometheus.Metric) {
	if c.dbTracker == nil {
		return
	}
	if c.dbTracker.trackSizes {
		for dbName, snap := range c.dbTracker.DBSizes {
			ch <- gauge(dDbSize, snap.DatabaseSize, dbName)
			for _, t := range snap.Tables {
				ch <- gauge(dTableSize, t.Size, dbName, t.Schema, t.Table)
			}
		}
		for _, g := range c.dbTracker.TableGrowth {
			ch <- gauge(dTableSizeGrowth, g.Growth, g.DB, g.Schema, g.Table)
		}
	}
	for dbName, b := range c.dbTracker.bloat {
		ch <- gauge(dDbTableBloat, b.TableTotal, dbName)
		ch <- gauge(dDbIndexBloat, b.IndexTotal, dbName)
		for _, t := range b.TopTables {
			ch <- gauge(dTableBloat, t.Bytes, dbName, t.Schema, t.Table)
		}
		for _, ix := range b.TopIndexes {
			ch <- gauge(dIndexBloat, ix.Bytes, dbName, ix.Schema, ix.Table, ix.Index)
		}
	}
	for dbName, entries := range c.dbTracker.tableStats {
		for _, e := range entries {
			ch <- gauge(dTableReltuples, e.Reltuples, dbName, e.Schema, e.Table)
			if e.DeadBytes > 0 {
				ch <- gauge(dTableDeadTupleBytes, e.DeadBytes, dbName, e.Schema, e.Table)
				ch <- gauge(dTableDeadTuples, e.DeadTuples, dbName, e.Schema, e.Table)
				ch <- gauge(dTableLiveTuples, e.LiveTuples, dbName, e.Schema, e.Table)
			}
			if e.AutovacuumAge.Valid {
				ch <- gauge(dTableSecondsSinceAutovacuum, e.AutovacuumAge.Float64, dbName, e.Schema, e.Table)
			}
			if e.ModsSinceAnalyze > 0 {
				ch <- gauge(dTableModsSinceAnalyze, e.ModsSinceAnalyze, dbName, e.Schema, e.Table)
			}
			if e.AnalyzeAge.Valid {
				ch <- gauge(dTableSecondsSinceAnalyze, e.AnalyzeAge.Float64, dbName, e.Schema, e.Table)
			}
			if e.Reloptions.Valid && e.Reloptions.String != "" {
				if s := parseTableSettings(e.Reloptions.String); len(s) > 0 {
					ch <- gauge(dTableSetting, 1, dbName, e.Schema, e.Table,
						s["autovacuum_disabled"], s["autovacuum_vacuum_scale_factor"], s["autovacuum_vacuum_threshold"],
						s["autovacuum_vacuum_cost_delay"], s["autovacuum_vacuum_cost_limit"],
						s["autovacuum_analyze_scale_factor"], s["autovacuum_analyze_threshold"])
				}
			}
		}
	}
	for dbName, entries := range c.dbTracker.vacuumProgress {
		for _, v := range entries {
			ch <- gauge(dTableVacuumInProgress, 1, dbName, v.Schema, v.Table)
			throttled := 0.0
			if v.Throttled {
				throttled = 1
			}
			ch <- gauge(dTableVacuumThrottled, throttled, dbName, v.Schema, v.Table)
		}
	}
}

func (c *Collector) Close() error {
	c.ctxCancelFunc()
	return c.db.Close()
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancelFunc := context.WithTimeout(c.ctx, c.collectTimeout)
	defer cancelFunc()
	now := time.Now()
	if err := c.db.PingContext(ctx); err != nil {
		c.logger.Warning("probe failed:", err)
		ch <- gauge(dUp, 0)
		ch <- gauge(dScrapeError, 1, err.Error(), "")
		return
	}
	ch <- gauge(dUp, 1)
	ch <- gauge(dProbe, time.Since(now).Seconds())
	if c.origVersion != "" {
		ch <- gauge(dInfo, 1, c.origVersion)
	}

	c.lock.RLock()
	defer c.lock.RUnlock()

	if len(c.scrapeErrors) > 0 {
		for e := range c.scrapeErrors {
			ch <- gauge(dScrapeError, 1, "", e)
		}
	} else {
		ch <- gauge(dScrapeError, 0, "", "")
	}

	c.connectionMetrics(ch)
	c.queryMetrics(ch)
	c.tableSizeMetrics(ch)
	for _, s := range c.settings {
		if s.IsMetric {
			ch <- gauge(dSettings, s.Value, s.Name, s.Unit)
		}
	}

	if c.replicationStatus != nil {
		rs := c.replicationStatus
		if rs.isInRecovery {
			if rs.receiveLsn.Valid {
				ch <- counter(dWalReceiveLsn, float64(rs.receiveLsn.Int64))
			}
			if rs.replyLsn.Valid {
				ch <- counter(dWalReplyLsn, float64(rs.replyLsn.Int64))
			}
			isReplayPaused := 0.0
			if rs.isReplayPaused {
				isReplayPaused = 1.0
			}
			ch <- gauge(dWalReplayPaused, isReplayPaused)
			host, port, err := rs.primaryHostPort()
			if err != nil {
				c.logger.Warning(err)
			}
			ch <- gauge(dWalReceiverStatus, float64(rs.walReceiverStatus), host, port)
		} else {
			if rs.currentLsn.Valid {
				ch <- counter(dWalCurrentLsn, float64(rs.currentLsn.Int64))
			}
		}
	}

	ch <- counter(dCheckpointsScheduled, c.cpTimed, "timed")
	ch <- counter(dCheckpointsScheduled, c.cpRequested, "requested")
	ch <- counter(dCheckpoints, c.cpDone)
	ch <- counter(dRestartpoints, c.cpRestartsDone)
	ch <- counter(dBuffersWritten, c.cpBuffers, "checkpointer")
	if c.cpWalBytes.Valid {
		ch <- gauge(dWalSinceLastCheckpoint, c.cpWalBytes.V)
	}
	if c.walSize.Valid {
		ch <- gauge(dWalSize, c.walSize.V)
	}
	for _, s := range c.replicationSlots {
		if s.retained.Valid {
			ch <- gauge(dReplicationSlotRetained, s.retained.V, s.name, strconv.FormatBool(s.active), s.walStatus)
		}
	}
	ch <- counter(dWalArchivedSegments, c.archArchived)
	ch <- counter(dWalArchiveFailures, c.archFailed)
	if w := c.wraparound; w != nil {
		for db, v := range w.xidAge {
			ch <- gauge(dXidAge, v, db)
		}
		for db, v := range w.multixactAge {
			ch <- gauge(dMultixactAge, v, db)
		}
		for holder, v := range w.xminAgeByHolder {
			ch <- gauge(dOldestXminAge, v, holder)
		}
	}
	if a := c.archStats; a != nil {
		if a.lastArchived.Valid || a.lastFailed.Valid {
			failing := a.lastFailed.Valid && (!a.lastArchived.Valid || a.lastFailed.V.After(a.lastArchived.V))
			status := 1.0
			if failing {
				status = 0.0
			}
			ch <- gauge(dWalArchivingStatus, status)
		}
	}
	if !c.lastCheckpointAt.IsZero() {
		ch <- gauge(dTimeSinceLastCheckpoint, time.Since(c.lastCheckpointAt).Seconds())
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- dUp
	ch <- dProbe
	ch <- dScrapeError
	ch <- dInfo
	ch <- dConnections
	ch <- dAutovacuumWorkers
	ch <- dTableVacuumInProgress
	ch <- dTableVacuumThrottled
	ch <- dLatency
	ch <- dLockAwaitingQueries
	ch <- dSettings
	ch <- dTopQueryCalls
	ch <- dTopQueryTime
	ch <- dTopQueryIOTime
	ch <- dDbQueries
	ch <- dWalReceiverStatus
	ch <- dWalReplayPaused
	ch <- dWalCurrentLsn
	ch <- dWalReceiveLsn
	ch <- dWalReplyLsn
	ch <- dCheckpointsScheduled
	ch <- dCheckpoints
	ch <- dRestartpoints
	ch <- dBuffersWritten
	ch <- dTimeSinceLastCheckpoint
	ch <- dWalSinceLastCheckpoint
	ch <- dWalSize
	ch <- dReplicationSlotRetained
	ch <- dWalArchivedSegments
	ch <- dWalArchiveFailures
	ch <- dXidAge
	ch <- dMultixactAge
	ch <- dOldestXminAge
	ch <- dWalArchivingStatus
	ch <- dDbSize
	ch <- dTableSize
	ch <- dTableSizeGrowth
	ch <- dDbTableBloat
	ch <- dDbIndexBloat
	ch <- dTableBloat
	ch <- dIndexBloat
	ch <- dTableDeadTupleBytes
	ch <- dTableDeadTuples
	ch <- dTableLiveTuples
	ch <- dTableSecondsSinceAutovacuum
	ch <- dTableModsSinceAnalyze
	ch <- dTableReltuples
	ch <- dTableSecondsSinceAnalyze
	ch <- dTableSetting
}

func desc(name, help string, labels ...string) *prometheus.Desc {
	return prometheus.NewDesc(name, help, labels, nil)
}

func gauge(desc *prometheus.Desc, value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
}

func counter(desc *prometheus.Desc, value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(desc, prometheus.CounterValue, value, labels...)
}
