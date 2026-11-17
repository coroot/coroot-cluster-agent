package dbtracker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/coroot/coroot-cluster-agent/schema"
	"github.com/stretchr/testify/assert"
)

func Test_TrimTopTables(t *testing.T) {
	dbSizes := map[string]*DBSizeSnapshot{
		"db1": {Tables: []TableSizeEntry{
			{TableKey: schema.TableKey{DB: "db1", Table: "big"}, Size: 1000},
			{TableKey: schema.TableKey{DB: "db1", Table: "small"}, Size: 10},
		}},
		"db2": {Tables: []TableSizeEntry{
			{TableKey: schema.TableKey{DB: "db2", Table: "medium"}, Size: 500},
		}},
	}

	trimTopTables(dbSizes, 2)

	var allTables []TableSizeEntry
	for _, snap := range dbSizes {
		allTables = append(allTables, snap.Tables...)
	}
	assert.Equal(t, 2, len(allTables))
}

type fakeEmitter struct {
	changes []schema.Change
	records []schema.TableRecord
}

func (e *fakeEmitter) EmitChange(change schema.Change, dbSystem, targetAddr string) {
	e.changes = append(e.changes, change)
}

func (e *fakeEmitter) EmitSchema(rec schema.TableRecord, dbSystem, targetAddr string) {
	e.records = append(e.records, rec)
}

func Test_EmitSchema(t *testing.T) {
	key := func(table string) schema.TableKey { return schema.TableKey{DB: "db", Schema: "public", Table: table} }
	tr := NewTracker("postgresql", true, false, nil, nil)
	tr.TableStats = map[schema.TableKey]schema.TableStats{key("a"): {schema.AttrRows: "10"}}
	now := time.Unix(1000000, 0)
	emit := func(curr schema.Snapshot) []schema.TableRecord {
		e := &fakeEmitter{}
		for _, c := range schema.Diff(tr.prev, curr) {
			delete(tr.schemaEmitted, schema.TableKey{DB: c.Database, Schema: c.Schema, Table: c.Object})
		}
		tr.prev = curr
		tr.emitSchema(e, "127.0.0.1:5432", curr, now)
		return e.records
	}

	// the first pass reports all the tables
	recs := emit(schema.Snapshot{key("a"): "ddl a", key("b"): "ddl b"})
	assert.Equal(t, []schema.TableRecord{
		{Key: key("a"), DDL: "ddl a", Stats: schema.TableStats{schema.AttrRows: "10"}},
		{Key: key("b"), DDL: "ddl b"},
	}, recs)

	// nothing changed
	now = now.Add(time.Minute)
	assert.Empty(t, emit(schema.Snapshot{key("a"): "ddl a", key("b"): "ddl b"}))

	// a changed table and a new one are reported immediately
	now = now.Add(time.Minute)
	recs = emit(schema.Snapshot{key("a"): "ddl a", key("b"): "ddl b2", key("c"): "ddl c"})
	assert.Equal(t, []schema.TableRecord{{Key: key("b"), DDL: "ddl b2"}, {Key: key("c"), DDL: "ddl c"}}, recs)

	// a dropped table
	now = now.Add(time.Minute)
	recs = emit(schema.Snapshot{key("a"): "ddl a", key("b"): "ddl b2"})
	assert.Equal(t, []schema.TableRecord{{Key: key("c"), Dropped: true}}, recs)

	// a database that is temporarily missing isn't reported as dropped, but is re-reported when it's back
	now = now.Add(time.Minute)
	assert.Empty(t, emit(schema.Snapshot{}))
	now = now.Add(time.Minute)
	assert.Len(t, emit(schema.Snapshot{key("a"): "ddl a", key("b"): "ddl b2"}), 2)

	// periodic refresh
	now = now.Add(SchemaRefreshInterval)
	assert.Len(t, emit(schema.Snapshot{key("a"): "ddl a", key("b"): "ddl b2"}), 2)

	// the budget is respected and the rest is reported on the next passes
	big := schema.Snapshot{}
	for i := 0; i < SchemaEmitBudget+50; i++ {
		big[schema.TableKey{DB: "big", Table: fmt.Sprintf("t%04d", i)}] = "ddl"
	}
	tr = NewTracker("postgresql", true, false, nil, nil)
	assert.Len(t, emit(big), SchemaEmitBudget)
	now = now.Add(time.Minute)
	assert.Len(t, emit(big), 50)
	now = now.Add(time.Minute)
	assert.Empty(t, emit(big))

	// the order is stable: the first pass covers the first SchemaEmitBudget tables
	tr = NewTracker("postgresql", true, false, nil, nil)
	recs = emit(big)
	assert.Equal(t, "t0000", recs[0].Key.Table)
	assert.Equal(t, fmt.Sprintf("t%04d", SchemaEmitBudget-1), recs[len(recs)-1].Key.Table)

	// the same applies to the dropped tables
	tr = NewTracker("postgresql", true, false, nil, nil)
	for len(emit(big)) > 0 {
		now = now.Add(time.Minute)
	}
	keep := schema.Snapshot{schema.TableKey{DB: "big", Table: "keep"}: "ddl"}
	recs = emit(keep)
	assert.Len(t, recs, SchemaEmitBudget)
	assert.True(t, recs[0].Dropped)
	assert.Equal(t, "t0000", recs[0].Key.Table)
	assert.Equal(t, fmt.Sprintf("t%04d", SchemaEmitBudget-1), recs[len(recs)-1].Key.Table)
	now = now.Add(time.Minute)
	assert.Len(t, emit(keep), 51) // the remaining 50 dropped tables and the new one
}

func Test_SchemaStatsNeeded(t *testing.T) {
	a := schema.TableKey{DB: "db", Table: "a"}
	b := schema.TableKey{DB: "db", Table: "b"}
	snapshot := schema.Snapshot{a: "ddl a", b: "ddl b"}
	var needed []bool
	tr := NewTracker("mysql", true, false, nil, nil)
	tr.collect = func(ctx context.Context) (schema.Snapshot, map[string]*DBSizeSnapshot, error) {
		needed = append(needed, tr.SchemaStatsNeeded(snapshot))
		return snapshot, nil, nil
	}
	e := &fakeEmitter{}
	track := func(shift time.Duration) {
		tr.lastTracked = tr.lastTracked.Add(-shift)
		for k, ts := range tr.schemaEmitted {
			tr.schemaEmitted[k] = ts.Add(-shift)
		}
		tr.Track(context.Background(), e, "127.0.0.1:3306")
	}
	track(0)               // new tables
	track(2 * time.Minute) // nothing to report
	snapshot = schema.Snapshot{a: "ddl a2", b: "ddl b"}
	track(2 * time.Minute)       // a changed
	track(2 * time.Minute)       // nothing to report
	track(SchemaRefreshInterval) // refresh
	track(2 * time.Minute)       // nothing to report
	assert.Equal(t, []bool{true, false, true, false, true, false}, needed)
}
