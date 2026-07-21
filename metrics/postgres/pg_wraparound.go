package postgres

import (
	"context"
	"database/sql"

	"github.com/blang/semver"
)

type wraparoundStats struct {
	xidAge          map[string]float64
	multixactAge    map[string]float64
	xminAgeByHolder map[string]float64
}

func (c *Collector) getWraparoundStats(ctx context.Context, version semver.Version) error {
	ws := &wraparoundStats{
		xidAge:          map[string]float64{},
		multixactAge:    map[string]float64{},
		xminAgeByHolder: map[string]float64{},
	}
	rows, err := c.db.QueryContext(ctx, `SELECT datname, age(datfrozenxid), mxid_age(datminmxid) FROM pg_database`)
	if err != nil {
		c.wraparound = nil
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var db string
		var xid, mxid sql.Null[int64]
		if err := rows.Scan(&db, &xid, &mxid); err != nil {
			c.logger.Warning(err)
			continue
		}
		if xid.Valid {
			ws.xidAge[db] = float64(xid.V)
		}
		if mxid.Valid {
			ws.multixactAge[db] = float64(mxid.V)
		}
	}

	if semver.MustParseRange(">=10.0.0")(version) {
		var running, standby, slot, prepared sql.Null[int64]
		if err := c.db.QueryRowContext(ctx, `SELECT
			(SELECT COALESCE(max(age(backend_xmin)), 0) FROM pg_stat_activity WHERE backend_xmin IS NOT NULL AND backend_type = 'client backend'),
			(SELECT COALESCE(max(age(backend_xmin)), 0) FROM pg_stat_activity WHERE backend_xmin IS NOT NULL AND backend_type = 'walsender'),
			(SELECT COALESCE(max(GREATEST(age(xmin), age(catalog_xmin))), 0) FROM pg_replication_slots),
			(SELECT COALESCE(max(age(transaction)), 0) FROM pg_prepared_xacts)`,
		).Scan(&running, &standby, &slot, &prepared); err != nil {
			c.logger.Warning(err)
		} else {
			ws.xminAgeByHolder["running_transaction"] = float64(running.V)
			ws.xminAgeByHolder["standby_feedback"] = float64(standby.V)
			ws.xminAgeByHolder["replication_slot"] = float64(slot.V)
			ws.xminAgeByHolder["prepared_transaction"] = float64(prepared.V)
		}
	}

	c.wraparound = ws
	return nil
}
