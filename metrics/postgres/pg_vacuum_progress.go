package postgres

import (
	"context"
	"database/sql"

	"github.com/coroot/logger"
)

type vacuumProgressEntry struct {
	Schema    string
	Table     string
	Throttled bool
}

func collectVacuumProgress(ctx context.Context, db *sql.DB, log logger.Logger) []vacuumProgressEntry {
	rows, err := db.QueryContext(ctx, `
		SELECT n.nspname, c.relname, COALESCE(a.wait_event = 'VacuumDelay', false)
			FROM pg_stat_progress_vacuum p
			JOIN pg_class c ON c.oid = p.relid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			LEFT JOIN pg_stat_activity a ON a.pid = p.pid`,
	)
	if err != nil {
		log.Warning("vacuum progress:", err)
		return nil
	}
	defer rows.Close()
	var result []vacuumProgressEntry
	for rows.Next() {
		var e vacuumProgressEntry
		if err := rows.Scan(&e.Schema, &e.Table, &e.Throttled); err != nil {
			log.Warning("scan vacuum progress:", err)
			continue
		}
		result = append(result, e)
	}
	return result
}
