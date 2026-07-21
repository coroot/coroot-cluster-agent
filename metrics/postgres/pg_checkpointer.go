package postgres

import (
	"context"
	"database/sql"
	"time"

	"github.com/blang/semver"
)

type checkpointStats struct {
	timed          sql.Null[int64]
	requested      sql.Null[int64]
	done           sql.Null[int64]
	restartsDone   sql.Null[int64]
	buffersWritten sql.Null[int64]
}

func (s *checkpointStats) completed() int64 {
	var t int64
	if s.done.Valid {
		t = s.done.V
	} else {
		for _, v := range []sql.Null[int64]{s.timed, s.requested} {
			if v.Valid {
				t += v.V
			}
		}
	}
	if s.restartsDone.Valid {
		t += s.restartsDone.V
	}
	return t
}

func (c *Collector) getCheckpointStats(ctx context.Context, version semver.Version) error {
	query := `SELECT checkpoints_timed, checkpoints_req, NULL, NULL, buffers_checkpoint FROM pg_stat_bgwriter`
	switch {
	case semver.MustParseRange(">=18.0.0")(version):
		query = `SELECT num_timed, num_requested, num_done, restartpoints_done, buffers_written FROM pg_stat_checkpointer`
	case semver.MustParseRange(">=17.0.0")(version):
		query = `SELECT num_timed, num_requested, NULL, restartpoints_done, buffers_written FROM pg_stat_checkpointer`
	}
	s := &checkpointStats{}
	if err := c.db.QueryRowContext(ctx, query).Scan(&s.timed, &s.requested, &s.done, &s.restartsDone, &s.buffersWritten); err != nil {
		return err
	}
	if err := c.db.QueryRowContext(ctx,
		`SELECT pg_wal_lsn_diff(CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END, redo_lsn) FROM pg_control_checkpoint()`,
	).Scan(&c.cpWalBytes); err != nil {
		c.cpWalBytes = sql.Null[float64]{}
		c.logger.Warning(err)
	}
	if c.cpPrev != nil {
		c.cpTimed += delta(c.cpPrev.timed, s.timed)
		c.cpRequested += delta(c.cpPrev.requested, s.requested)
		if s.done.Valid {
			c.cpDone += delta(c.cpPrev.done, s.done)
		} else {
			c.cpDone += delta(c.cpPrev.timed, s.timed) + delta(c.cpPrev.requested, s.requested)
		}
		c.cpRestartsDone += delta(c.cpPrev.restartsDone, s.restartsDone)
		c.cpBuffers += delta(c.cpPrev.buffersWritten, s.buffersWritten)
		if s.completed() > c.cpPrev.completed() {
			c.lastCheckpointAt = time.Now()
		}
	}
	c.cpPrev = s
	return nil
}

func delta[T int64 | float64](prev, cur sql.Null[T]) float64 {
	if prev.Valid && cur.Valid && cur.V >= prev.V {
		return float64(cur.V - prev.V)
	}
	return 0
}
