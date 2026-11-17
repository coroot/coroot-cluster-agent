package dbtracker

import (
	"context"
	"sort"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/schema"
	"github.com/coroot/logger"
)

const (
	TrackMinInterval      = 60 * time.Second
	TopTablesN            = 20
	SchemaRefreshInterval = time.Hour
	SchemaEmitBudget      = 200
)

type Emitter interface {
	EmitChange(change schema.Change, dbSystem, targetAddr string)
	EmitSchema(rec schema.TableRecord, dbSystem, targetAddr string)
}

type TableSizeEntry struct {
	schema.TableKey
	Size        float64
	StorageSize float64
	FreeStorage float64
	Documents   float64
}

type TableGrowthEntry struct {
	schema.TableKey
	Growth float64
}

type DBSizeSnapshot struct {
	DatabaseSize float64
	Tables       []TableSizeEntry
}

type CollectFunc func(ctx context.Context) (schema.Snapshot, map[string]*DBSizeSnapshot, error)

type Tracker struct {
	dbSystem    string
	trackSchema bool
	trackSizes  bool
	logger      logger.Logger
	collect     CollectFunc

	TableStats map[schema.TableKey]schema.TableStats

	prev           schema.Snapshot
	schemaEmitted  map[schema.TableKey]time.Time
	lastTracked    time.Time
	DBSizes        map[string]*DBSizeSnapshot
	prevTableSizes map[schema.TableKey]float64
	TableGrowth    []TableGrowthEntry
}

func NewTracker(dbSystem string, trackSchema, trackSizes bool, collect CollectFunc, logger logger.Logger) *Tracker {
	return &Tracker{
		dbSystem:    dbSystem,
		trackSchema: trackSchema,
		trackSizes:  trackSizes,
		collect:     collect,
		logger:      logger,
	}
}

func (t *Tracker) Track(ctx context.Context, emitter Emitter, targetAddr string) {
	if time.Since(t.lastTracked) < TrackMinInterval {
		return
	}
	prevTracked := t.lastTracked
	t.lastTracked = time.Now()

	curr, dbSizes, err := t.collect(ctx)
	if err != nil {
		t.logger.Warning("database tracking:", err)
		return
	}
	t.DBSizes = dbSizes

	if t.trackSizes {
		t.computeTableGrowth(dbSizes, t.lastTracked.Sub(prevTracked))
		trimTopTables(dbSizes, TopTablesN)
	}

	if t.trackSchema {
		for _, c := range schema.Diff(t.prev, curr) {
			emitter.EmitChange(c, t.dbSystem, targetAddr)
			delete(t.schemaEmitted, schema.TableKey{DB: c.Database, Schema: c.Schema, Table: c.Object})
		}
		t.prev = curr
		t.emitSchema(emitter, targetAddr, curr, t.lastTracked)
	}
}

func (t *Tracker) SchemaStatsNeeded(tables schema.Snapshot) bool {
	for key, ddl := range tables {
		if prev, ok := t.prev[key]; !ok || prev != ddl {
			return true
		}
		if last, ok := t.schemaEmitted[key]; !ok || t.lastTracked.Sub(last) >= SchemaRefreshInterval {
			return true
		}
	}
	return false
}

func (t *Tracker) emitSchema(emitter Emitter, targetAddr string, curr schema.Snapshot, now time.Time) {
	if t.schemaEmitted == nil {
		t.schemaEmitted = map[schema.TableKey]time.Time{}
	}
	budget := SchemaEmitBudget

	dbs := map[string]bool{}
	for key := range curr {
		dbs[key.DB] = true
	}
	var dropped []schema.TableKey
	for key := range t.schemaEmitted {
		if _, ok := curr[key]; ok {
			continue
		}
		if !dbs[key.DB] {
			delete(t.schemaEmitted, key)
			continue
		}
		dropped = append(dropped, key)
	}
	sort.Slice(dropped, func(i, j int) bool { return tableKeyLess(dropped[i], dropped[j]) })
	for _, key := range dropped {
		if budget <= 0 {
			break
		}
		budget--
		emitter.EmitSchema(schema.TableRecord{Key: key, Dropped: true}, t.dbSystem, targetAddr)
		delete(t.schemaEmitted, key)
	}

	type dueTable struct {
		key  schema.TableKey
		last time.Time
	}
	var due []dueTable
	for key := range curr {
		last, ok := t.schemaEmitted[key]
		if !ok || now.Sub(last) >= SchemaRefreshInterval {
			due = append(due, dueTable{key: key, last: last})
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if !due[i].last.Equal(due[j].last) {
			return due[i].last.Before(due[j].last)
		}
		return tableKeyLess(due[i].key, due[j].key)
	})
	for _, d := range due {
		if budget <= 0 {
			break
		}
		budget--
		emitter.EmitSchema(schema.TableRecord{Key: d.key, DDL: curr[d.key], Stats: t.TableStats[d.key]}, t.dbSystem, targetAddr)
		t.schemaEmitted[d.key] = now
	}
}

func tableKeyLess(a, b schema.TableKey) bool {
	if a.DB != b.DB {
		return a.DB < b.DB
	}
	if a.Schema != b.Schema {
		return a.Schema < b.Schema
	}
	return a.Table < b.Table
}

func (t *Tracker) computeTableGrowth(dbSizes map[string]*DBSizeSnapshot, elapsed time.Duration) {
	currSizes := map[schema.TableKey]float64{}
	for _, snap := range dbSizes {
		for _, te := range snap.Tables {
			currSizes[te.TableKey] = te.Size
		}
	}

	t.TableGrowth = nil
	if t.prevTableSizes != nil && elapsed > 0 {
		var all []TableGrowthEntry
		for key, currSize := range currSizes {
			if prevSize, ok := t.prevTableSizes[key]; ok {
				growth := (currSize - prevSize) / elapsed.Seconds()
				if growth > 0 {
					all = append(all, TableGrowthEntry{TableKey: key, Growth: growth})
				}
			}
		}
		t.TableGrowth = common.TopN(all, TopTablesN, func(a, b TableGrowthEntry) bool { return a.Growth > b.Growth })
	}
	t.prevTableSizes = currSizes
}

func trimTopTables(dbSizes map[string]*DBSizeSnapshot, n int) {
	var all []TableSizeEntry
	for _, snap := range dbSizes {
		all = append(all, snap.Tables...)
	}
	all = common.TopN(all, n, func(a, b TableSizeEntry) bool { return a.Size > b.Size })
	byDB := make(map[string][]TableSizeEntry, len(dbSizes))
	for _, te := range all {
		byDB[te.DB] = append(byDB[te.DB], te)
	}
	for db, snap := range dbSizes {
		snap.Tables = byDB[db]
	}
}
