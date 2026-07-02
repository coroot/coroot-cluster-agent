package postgres

import (
	"context"
	"database/sql"
	"sort"

	"github.com/coroot/logger"
)

const bloatTopN = 20

type bloatEntry struct {
	Schema string
	Table  string
	Index  string
	Bytes  float64
}

type dbBloat struct {
	TableTotal float64
	IndexTotal float64
	TopTables  []bloatEntry
	TopIndexes []bloatEntry
}

// bloat = actual size − expected size, where
// expected = live tuples * (per-tuple overhead (24 B) + average row width from pg_stats)
const tableBloatQuery = `
SELECT n.nspname, c.relname, greatest(0,
	pg_relation_size(c.oid) - (c.reltuples * (24 + coalesce(w.width, 0)))::bigint)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN (
	SELECT schemaname, tablename, sum((1 - coalesce(null_frac, 0)) * coalesce(avg_width, 0)) AS width
	FROM pg_stats GROUP BY 1, 2
) w ON w.schemaname = n.nspname AND w.tablename = c.relname
WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') AND c.reltuples > 0`

const indexBloatQuery = `
SELECT n.nspname, ct.relname, ci.relname, greatest(0,
	pg_relation_size(ci.oid) - (ci.reltuples * (12 + coalesce(k.width, 0)))::bigint)
FROM pg_index i
JOIN pg_class ci ON ci.oid = i.indexrelid
JOIN pg_class ct ON ct.oid = i.indrelid
JOIN pg_namespace n ON n.oid = ct.relnamespace
JOIN pg_am am ON am.oid = ci.relam AND am.amname = 'btree'
LEFT JOIN LATERAL (
	SELECT sum((1 - coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 0)) AS width
	FROM unnest(string_to_array(i.indkey::text, ' ')::int[]) AS col(attnum)
	JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = col.attnum
	LEFT JOIN pg_stats s ON s.schemaname = n.nspname AND s.tablename = ct.relname AND s.attname = a.attname
	WHERE col.attnum > 0
) k ON true
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') AND ci.reltuples > 0`

func collectBloat(ctx context.Context, db *sql.DB, log logger.Logger) *dbBloat {
	b := &dbBloat{}

	if rows, err := db.QueryContext(ctx, tableBloatQuery); err != nil {
		log.Warning("table bloat estimation:", err)
	} else {
		for rows.Next() {
			var e bloatEntry
			if err := rows.Scan(&e.Schema, &e.Table, &e.Bytes); err != nil {
				log.Warning("scan table bloat:", err)
				continue
			}
			b.TableTotal += e.Bytes
			if e.Bytes > 0 {
				b.TopTables = append(b.TopTables, e)
			}
		}
		rows.Close()
	}
	b.TopTables = topBloat(b.TopTables)

	if rows, err := db.QueryContext(ctx, indexBloatQuery); err != nil {
		log.Warning("index bloat estimation:", err)
	} else {
		for rows.Next() {
			var e bloatEntry
			if err := rows.Scan(&e.Schema, &e.Table, &e.Index, &e.Bytes); err != nil {
				log.Warning("scan index bloat:", err)
				continue
			}
			b.IndexTotal += e.Bytes
			if e.Bytes > 0 {
				b.TopIndexes = append(b.TopIndexes, e)
			}
		}
		rows.Close()
	}
	b.TopIndexes = topBloat(b.TopIndexes)

	return b
}

func topBloat(entries []bloatEntry) []bloatEntry {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Bytes > entries[j].Bytes })
	if len(entries) > bloatTopN {
		entries = entries[:bloatTopN]
	}
	return entries
}
