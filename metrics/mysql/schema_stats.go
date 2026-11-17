package mysql

import (
	"context"
	"database/sql"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/coroot/coroot-cluster-agent/schema"
)

func (dt *databaseTracker) collectSchemaStats(ctx context.Context, db *sql.DB) map[schema.TableKey]schema.TableStats {
	stats := map[schema.TableKey]schema.TableStats{}

	rows, err := db.QueryContext(ctx, `
SELECT table_schema, table_name, table_rows, data_length, index_length
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'`+dt.excludeClause("table_schema"), dt.excludeArgs()...)
	if err != nil {
		dt.logger.Warning("schema stats: tables:", err)
		return stats
	}
	defer rows.Close()
	for rows.Next() {
		var key schema.TableKey
		var tableRows, dataLength, indexLength sql.NullInt64
		if err = rows.Scan(&key.DB, &key.Table, &tableRows, &dataLength, &indexLength); err != nil {
			dt.logger.Warning("schema stats: scan tables:", err)
			return stats
		}
		ts := schema.TableStats{}
		if tableRows.Valid {
			ts.SetInt(schema.AttrRows, tableRows.Int64)
		}
		if dataLength.Valid {
			ts.SetInt(schema.AttrDataSize, dataLength.Int64)
		}
		if indexLength.Valid {
			ts.SetInt(schema.AttrIndexesSize, indexLength.Int64)
		}
		stats[key] = ts
	}
	if err = rows.Err(); err != nil {
		dt.logger.Warning("schema stats: tables:", err)
		return stats
	}

	indexes := map[schema.TableKey]map[string]*schema.Index{}
	idxRows, err := db.QueryContext(ctx, `
SELECT table_schema, table_name, index_name, non_unique, column_name, cardinality, index_type
FROM information_schema.statistics
WHERE true`+dt.excludeClause("table_schema")+`
ORDER BY table_schema, table_name, index_name, seq_in_index`, dt.excludeArgs()...)
	if err != nil {
		dt.logger.Warning("schema stats: indexes:", err)
		return stats
	}
	defer idxRows.Close()
	for idxRows.Next() {
		var key schema.TableKey
		var name, indexType string
		var nonUnique int
		var column sql.NullString
		var cardinality sql.NullInt64
		if err = idxRows.Scan(&key.DB, &key.Table, &name, &nonUnique, &column, &cardinality, &indexType); err != nil {
			dt.logger.Warning("schema stats: scan indexes:", err)
			return stats
		}
		if stats[key] == nil {
			continue
		}
		if indexes[key] == nil {
			indexes[key] = map[string]*schema.Index{}
		}
		idx := indexes[key][name]
		if idx == nil {
			idx = &schema.Index{Name: name, Primary: name == "PRIMARY", Unique: nonUnique == 0, Method: strings.ToLower(indexType)}
			indexes[key][name] = idx
		}
		if column.Valid {
			idx.Columns = append(idx.Columns, column.String)
		} else { // a functional key part has no column name
			idx.Columns = append(idx.Columns, "(expression)")
		}
		// the cardinality of the last key part is the cardinality of the whole index
		if cardinality.Valid {
			c := cardinality.Int64
			idx.Cardinality = &c
		}
	}
	if err = idxRows.Err(); err != nil {
		dt.logger.Warning("schema stats: indexes:", err)
		return stats
	}

	dt.collectIndexUsage(ctx, db, stats, indexes)

	for key, byName := range indexes {
		idxs := make([]schema.Index, 0, len(byName))
		for _, idx := range byName {
			idxs = append(idxs, *idx)
		}
		sortIndexStats(idxs)
		stats[key].SetIndexes(idxs)
	}
	return stats
}

func (dt *databaseTracker) collectIndexUsage(ctx context.Context, db *sql.DB, stats map[schema.TableKey]schema.TableStats, indexes map[schema.TableKey]map[string]*schema.Index) {
	rows, err := db.QueryContext(ctx, `
SELECT object_schema, object_name, index_name, count_read, count_insert, count_update, count_delete
FROM performance_schema.table_io_waits_summary_by_index_usage
WHERE object_type = 'TABLE'`+dt.excludeClause("object_schema"), dt.excludeArgs()...)
	if err != nil {
		dt.logger.Warning("schema stats: index usage:", err)
		return
	}
	defer rows.Close()

	type usage struct {
		seqRowsRead, indexRowsRead, inserted, updated, deleted int64
	}
	byTable := map[schema.TableKey]*usage{}
	for rows.Next() {
		var key schema.TableKey
		var indexName sql.NullString
		var read, ins, upd, del int64
		if err = rows.Scan(&key.DB, &key.Table, &indexName, &read, &ins, &upd, &del); err != nil {
			dt.logger.Warning("schema stats: scan index usage:", err)
			return
		}
		if stats[key] == nil {
			continue
		}
		u := byTable[key]
		if u == nil {
			u = &usage{}
			byTable[key] = u
		}
		u.inserted += ins
		u.updated += upd
		u.deleted += del
		if !indexName.Valid { // rows read without using an index
			u.seqRowsRead += read
			continue
		}
		u.indexRowsRead += read
		if idx := indexes[key][indexName.String]; idx != nil {
			r := read
			idx.RowsRead = &r
		}
	}
	if err = rows.Err(); err != nil {
		dt.logger.Warning("schema stats: index usage:", err)
		return
	}

	var statsSince int64
	var name, uptime string
	if err = db.QueryRowContext(ctx, `SHOW GLOBAL STATUS LIKE 'Uptime'`).Scan(&name, &uptime); err == nil {
		if seconds, err := strconv.ParseInt(uptime, 10, 64); err == nil {
			statsSince = time.Now().Unix() - seconds
		}
	}

	for key, u := range byTable {
		ts := stats[key]
		ts.SetInt(schema.AttrSeqRowsRead, u.seqRowsRead)
		ts.SetInt(schema.AttrIndexRowsFetched, u.indexRowsRead)
		ts.SetInt(schema.AttrRowsInserted, u.inserted)
		ts.SetInt(schema.AttrRowsUpdated, u.updated)
		ts.SetInt(schema.AttrRowsDeleted, u.deleted)
		if statsSince > 0 {
			ts.SetInt(schema.AttrStatsSince, statsSince)
		}
	}
}

func sortIndexStats(idxs []schema.Index) {
	sort.Slice(idxs, func(i, j int) bool { return idxs[i].Name < idxs[j].Name })
}

func (dt *databaseTracker) excludeClause(column string) string {
	if len(dt.excludeDatabases) == 0 {
		return ""
	}
	return " AND " + column + " NOT IN (?" + strings.Repeat(", ?", len(dt.excludeDatabases)-1) + ")"
}

func (dt *databaseTracker) excludeArgs() []any {
	args := make([]any, 0, len(dt.excludeDatabases))
	for name := range dt.excludeDatabases {
		args = append(args, name)
	}
	return args
}
