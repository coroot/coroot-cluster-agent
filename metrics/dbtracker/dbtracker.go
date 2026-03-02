package dbtracker

import (
	"context"
	"database/sql"
	"sort"
	"time"

	"github.com/coroot/coroot-cluster-agent/schema"
	"github.com/coroot/logger"
)

const (
	TrackMinInterval = 60 * time.Second
	TopTablesN       = 20
)

type ChangeEmitter interface {
	Emit(change schema.Change, dbSystem, targetAddr string)
}

type TableKey struct {
	DB     string
	Schema string
	Table  string
}

type TableSizeEntry struct {
	TableKey
	Size float64
}

type TableGrowthEntry struct {
	TableKey
	Growth float64
}

type DBSizeSnapshot struct {
	DatabaseSize float64
	Tables       []TableSizeEntry
}

type CollectFunc func(ctx context.Context, db *sql.DB) (schema.Snapshot, map[string]*DBSizeSnapshot, error)

type Tracker struct {
	dbSystem    string
	trackSchema bool
	trackSizes  bool
	logger      logger.Logger
	collect     CollectFunc

	prev           schema.Snapshot
	lastTracked    time.Time
	DBSizes        map[string]*DBSizeSnapshot
	prevTableSizes map[TableKey]float64
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

func (t *Tracker) Track(ctx context.Context, db *sql.DB, emitter ChangeEmitter, targetAddr string) {
	if time.Since(t.lastTracked) < TrackMinInterval {
		return
	}
	prevTracked := t.lastTracked
	t.lastTracked = time.Now()

	curr, dbSizes, err := t.collect(ctx, db)
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
			emitter.Emit(c, t.dbSystem, targetAddr)
		}
		t.prev = curr
	}
}

func (t *Tracker) computeTableGrowth(dbSizes map[string]*DBSizeSnapshot, elapsed time.Duration) {
	currSizes := map[TableKey]float64{}
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
		sort.Slice(all, func(i, j int) bool { return all[i].Growth > all[j].Growth })
		if len(all) > TopTablesN {
			all = all[:TopTablesN]
		}
		t.TableGrowth = all
	}
	t.prevTableSizes = currSizes
}

func trimTopTables(dbSizes map[string]*DBSizeSnapshot, n int) {
	var all []TableSizeEntry
	for _, snap := range dbSizes {
		all = append(all, snap.Tables...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Size > all[j].Size })
	if len(all) > n {
		all = all[:n]
	}
	for _, snap := range dbSizes {
		snap.Tables = nil
	}
	for _, te := range all {
		snap := dbSizes[te.DB]
		snap.Tables = append(snap.Tables, te)
	}
}
