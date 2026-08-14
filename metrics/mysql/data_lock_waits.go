package mysql

import (
	"context"

	"github.com/coroot/coroot-cluster-agent/obfuscate"
)

type lockWaits struct {
	locked   []queryCount // waiting queries, keyed by the waiting query
	awaiting []queryCount // waiting queries, keyed by the blocking query
}

type queryCount struct {
	schema string
	query  string
	count  float64
}

type lockGroupKey struct {
	schema string
	query  string
}

func (c *Collector) queryLockWaits(ctx context.Context) (*lockWaits, error) {
	q := `
	SELECT
	    IFNULL(rt.PROCESSLIST_DB, ''),
	    IFNULL(res.DIGEST_TEXT, ''),
	    IFNULL(bt.PROCESSLIST_DB, ''),
	    IFNULL(bes.DIGEST_TEXT, ''),
	    w.REQUESTING_ENGINE_TRANSACTION_ID
	FROM performance_schema.data_lock_waits w
	LEFT JOIN performance_schema.threads rt
	    ON rt.THREAD_ID = w.REQUESTING_THREAD_ID
	LEFT JOIN performance_schema.events_statements_current res
	    ON res.THREAD_ID = w.REQUESTING_THREAD_ID
	LEFT JOIN performance_schema.threads bt
	    ON bt.THREAD_ID = w.BLOCKING_THREAD_ID
	LEFT JOIN performance_schema.events_statements_current bes
	    ON bes.THREAD_ID = w.BLOCKING_THREAD_ID`

	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	lockedTx := map[lockGroupKey]map[uint64]struct{}{}
	awaitingTx := map[lockGroupKey]map[uint64]struct{}{}
	add := func(m map[lockGroupKey]map[uint64]struct{}, k lockGroupKey, tx uint64) {
		s := m[k]
		if s == nil {
			s = map[uint64]struct{}{}
			m[k] = s
		}
		s[tx] = struct{}{}
	}

	for rows.Next() {
		var reqSchema, reqDigest, blkSchema, blkDigest string
		var reqTx uint64
		if err := rows.Scan(&reqSchema, &reqDigest, &blkSchema, &blkDigest, &reqTx); err != nil {
			c.logger.Warning(err)
			continue
		}
		add(lockedTx, lockGroupKey{schema: reqSchema, query: obfuscate.Sql(reqDigest)}, reqTx)
		add(awaitingTx, lockGroupKey{schema: blkSchema, query: obfuscate.Sql(blkDigest)}, reqTx)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &lockWaits{
		locked:   toQueryCounts(lockedTx),
		awaiting: toQueryCounts(awaitingTx),
	}, nil
}

func toQueryCounts(m map[lockGroupKey]map[uint64]struct{}) []queryCount {
	res := make([]queryCount, 0, len(m))
	for k, txs := range m {
		res = append(res, queryCount{schema: k.schema, query: k.query, count: float64(len(txs))})
	}
	return res
}

func (c *Collector) lockWaitsSnapshot(ctx context.Context) {
	if c.isMariaDB {
		return
	}
	lw, err := c.queryLockWaits(ctx)
	if err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		return
	}
	c.lockWaits = lw
}
