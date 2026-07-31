package postgres

import (
	"context"
	"database/sql"
	"sort"
	"strings"

	"github.com/coroot/logger"
)

const topTablesPerStat = 20

type tableStatEntry struct {
	Schema           string
	Table            string
	DeadTuples       float64
	LiveTuples       float64
	DeadBytes        float64
	AutovacuumAge    sql.NullFloat64
	ModsSinceAnalyze float64
	Reltuples        float64
	AnalyzeAge       sql.NullFloat64
	Reloptions       sql.NullString
}

func (e tableStatEntry) analyzePressure() float64 {
	return e.ModsSinceAnalyze / (50 + 0.1*max(e.Reltuples, 0))
}

func collectTableStats(ctx context.Context, db *sql.DB, log logger.Logger) []tableStatEntry {
	rows, err := db.QueryContext(ctx, `
		SELECT s.schemaname, s.relname,
			s.n_dead_tup,
			s.n_live_tup,
			COALESCE((s.n_dead_tup::float8 / NULLIF(s.n_live_tup + s.n_dead_tup, 0) * pg_relation_size(s.relid))::bigint, 0),
			EXTRACT(EPOCH FROM now() - s.last_autovacuum),
			s.n_mod_since_analyze,
			c.reltuples,
			EXTRACT(EPOCH FROM now() - GREATEST(s.last_analyze, s.last_autoanalyze)),
			array_to_string(c.reloptions, ',')
		FROM pg_stat_user_tables s
		JOIN pg_class c ON c.oid = s.relid
		WHERE s.n_dead_tup > 0 OR s.n_mod_since_analyze > 0`,
	)
	if err != nil {
		log.Warning("table stats:", err)
		return nil
	}
	defer rows.Close()
	var all []tableStatEntry
	for rows.Next() {
		var e tableStatEntry
		if err := rows.Scan(&e.Schema, &e.Table, &e.DeadTuples, &e.LiveTuples, &e.DeadBytes, &e.AutovacuumAge, &e.ModsSinceAnalyze, &e.Reltuples, &e.AnalyzeAge, &e.Reloptions); err != nil {
			log.Warning("scan table stats:", err)
			continue
		}
		all = append(all, e)
	}
	return topTables(all)
}

func topTables(all []tableStatEntry) []tableStatEntry {
	type key struct{ schema, table string }
	keep := map[key]bool{}

	byDead := append([]tableStatEntry(nil), all...)
	sort.Slice(byDead, func(i, j int) bool { return byDead[i].DeadBytes > byDead[j].DeadBytes })
	for i, e := range byDead {
		if i >= topTablesPerStat || e.DeadTuples == 0 {
			break
		}
		keep[key{e.Schema, e.Table}] = true
	}

	byAnalyze := append([]tableStatEntry(nil), all...)
	sort.Slice(byAnalyze, func(i, j int) bool { return byAnalyze[i].analyzePressure() > byAnalyze[j].analyzePressure() })
	for i, e := range byAnalyze {
		if i >= topTablesPerStat || e.ModsSinceAnalyze == 0 {
			break
		}
		keep[key{e.Schema, e.Table}] = true
	}

	var result []tableStatEntry
	for _, e := range all {
		if keep[key{e.Schema, e.Table}] {
			result = append(result, e)
		}
	}
	return result
}

func parseTableSettings(reloptions string) map[string]string {
	out := map[string]string{}
	for _, opt := range strings.Split(reloptions, ",") {
		kv := strings.SplitN(opt, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "autovacuum_enabled":
			if kv[1] == "false" || kv[1] == "off" {
				out["autovacuum_disabled"] = "1"
			}
		case "autovacuum_vacuum_scale_factor", "autovacuum_vacuum_threshold",
			"autovacuum_vacuum_cost_delay", "autovacuum_vacuum_cost_limit",
			"autovacuum_analyze_scale_factor", "autovacuum_analyze_threshold":
			out[kv[0]] = kv[1]
		}
	}
	return out
}
