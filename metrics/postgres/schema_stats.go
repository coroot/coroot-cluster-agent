package postgres

import (
	"context"
	"database/sql"
	"sort"

	"github.com/coroot/coroot-cluster-agent/schema"
	"github.com/lib/pq"
)

var systemSchemas = []string{"pg_catalog", "information_schema"}

func (dt *databaseTracker) excludeSchemas() []string {
	res := append([]string{}, systemSchemas...)
	for name := range dt.excludeDatabases {
		res = append(res, name)
	}
	sort.Strings(res)
	return res
}

func (dt *databaseTracker) collectSchemaStats(ctx context.Context, db *sql.DB, dbName string, stats map[schema.TableKey]schema.TableStats) {
	log := dt.logger
	excludeSchemas := pq.Array(dt.excludeSchemas())
	var statsReset sql.NullFloat64
	if err := db.QueryRowContext(ctx, `SELECT EXTRACT(EPOCH FROM stats_reset) FROM pg_stat_database WHERE datname = current_database()`).Scan(&statsReset); err != nil {
		log.Warning("schema stats: stats_reset:", err)
	}

	rows, err := db.QueryContext(ctx, `
SELECT
    n.nspname, c.relname, c.reltuples::bigint,
    pg_table_size(c.oid), pg_indexes_size(c.oid),
    s.seq_scan, s.seq_tup_read, s.idx_scan, s.idx_tup_fetch,
    s.n_tup_ins, s.n_tup_upd, s.n_tup_del,
    EXTRACT(EPOCH FROM GREATEST(s.last_analyze, s.last_autoanalyze))::bigint
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_stat_user_tables s ON s.relid = c.oid
WHERE c.relkind = 'r'
    AND n.nspname <> ALL($1)`, excludeSchemas)
	if err != nil {
		log.Warning("schema stats: tables:", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		key := schema.TableKey{DB: dbName}
		var reltuples int64
		// the size functions return NULL for a relation dropped while the query is running
		var tableSize, indexesSize sql.NullInt64
		var seqScan, seqTupRead, idxScan, idxTupFetch, ins, upd, del, lastAnalyze sql.NullInt64
		if err = rows.Scan(&key.Schema, &key.Table, &reltuples, &tableSize, &indexesSize,
			&seqScan, &seqTupRead, &idxScan, &idxTupFetch, &ins, &upd, &del, &lastAnalyze); err != nil {
			log.Warning("schema stats: scan tables:", err)
			return
		}
		ts := schema.TableStats{}
		if reltuples >= 0 {
			ts.SetInt(schema.AttrRows, reltuples)
		}
		for attr, v := range map[string]sql.NullInt64{
			schema.AttrDataSize:         tableSize,
			schema.AttrIndexesSize:      indexesSize,
			schema.AttrSeqScans:         seqScan,
			schema.AttrSeqRowsRead:      seqTupRead,
			schema.AttrIndexScans:       idxScan,
			schema.AttrIndexRowsFetched: idxTupFetch,
			schema.AttrRowsInserted:     ins,
			schema.AttrRowsUpdated:      upd,
			schema.AttrRowsDeleted:      del,
			schema.AttrLastAnalyze:      lastAnalyze,
		} {
			if v.Valid {
				ts.SetInt(attr, v.Int64)
			}
		}
		if statsReset.Valid {
			ts.SetInt(schema.AttrStatsSince, int64(statsReset.Float64))
		}
		stats[key] = ts
	}
	if err = rows.Err(); err != nil {
		log.Warning("schema stats: tables:", err)
		return
	}

	idxRows, err := db.QueryContext(ctx, `
SELECT
    n.nspname, t.relname, i.relname,
    pg_relation_size(i.oid),
    x.indisprimary, x.indisunique, x.indisvalid,
    s.idx_scan, s.idx_tup_read, s.idx_tup_fetch,
    am.amname,
    ARRAY(SELECT COALESCE(pg_get_indexdef(x.indexrelid, k, true), '') FROM generate_series(1, x.indnatts) k ORDER BY k),
    COALESCE((to_jsonb(x)->>'indnkeyatts')::int, x.indnatts),
    pg_get_expr(x.indpred, x.indrelid, true)
FROM pg_catalog.pg_index x
JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid
JOIN pg_catalog.pg_am am ON am.oid = i.relam
JOIN pg_catalog.pg_class t ON t.oid = x.indrelid
JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
LEFT JOIN pg_catalog.pg_stat_user_indexes s ON s.indexrelid = x.indexrelid
WHERE t.relkind = 'r'
    AND n.nspname <> ALL($1)
ORDER BY n.nspname, t.relname, i.relname`, excludeSchemas)
	if err != nil {
		log.Warning("schema stats: indexes:", err)
		return
	}
	defer idxRows.Close()
	indexes := map[schema.TableKey][]schema.Index{}
	for idxRows.Next() {
		key := schema.TableKey{DB: dbName}
		var idx schema.Index
		var size sql.NullInt64
		var valid bool
		var scans, tupRead, tupFetch sql.NullInt64
		var columns pq.StringArray
		var keyColumns int
		var predicate sql.NullString
		if err = idxRows.Scan(&key.Schema, &key.Table, &idx.Name, &size, &idx.Primary, &idx.Unique, &valid, &scans, &tupRead, &tupFetch,
			&idx.Method, &columns, &keyColumns, &predicate); err != nil {
			log.Warning("schema stats: scan indexes:", err)
			return
		}
		if keyColumns < 0 || keyColumns > len(columns) {
			keyColumns = len(columns)
		}
		idx.Columns = columns[:keyColumns]
		if keyColumns < len(columns) {
			idx.Include = columns[keyColumns:]
		}
		idx.Predicate = predicate.String
		if size.Valid {
			idx.SizeBytes = &size.Int64
		}
		idx.Invalid = !valid
		if scans.Valid {
			idx.Scans = &scans.Int64
		}
		if tupRead.Valid {
			idx.RowsRead = &tupRead.Int64
		}
		if tupFetch.Valid {
			idx.RowsFetched = &tupFetch.Int64
		}
		indexes[key] = append(indexes[key], idx)
	}
	if err = idxRows.Err(); err != nil {
		log.Warning("schema stats: indexes:", err)
		return
	}
	for key, idxs := range indexes {
		if ts := stats[key]; ts != nil {
			ts.SetIndexes(idxs)
		}
	}
}
