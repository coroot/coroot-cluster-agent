package mysql

import (
	"context"
	"sort"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"

	"github.com/coroot/coroot-pg-agent/obfuscate"
	"github.com/prometheus/client_golang/prometheus"
)

type queryKey struct {
	schema string
	query  string
}

type statementsSummaryRow struct {
	obfuscatedQueryText string
	calls               uint64
	totalTime           uint64
	lockTime            uint64
}

type digestKey struct {
	schema string
	digest string
}

type statementsSummarySnapshot struct {
	ts   time.Time
	rows map[digestKey]statementsSummaryRow
}

func (c *Collector) queryStatementsSummary(ctx context.Context, prev *statementsSummarySnapshot) (*statementsSummarySnapshot, error) {
	snapshot := &statementsSummarySnapshot{ts: time.Now(), rows: map[digestKey]statementsSummaryRow{}}
	q := `
	SELECT
	    ifnull(SCHEMA_NAME, ''),
	    DIGEST,
	    DIGEST_TEXT,
	    COUNT_STAR,
	    SUM_TIMER_WAIT,
	    SUM_LOCK_TIME
	FROM 
		performance_schema.events_statements_summary_by_digest
	WHERE 
	    DIGEST IS NOT NULL AND 
	    DIGEST_TEXT IS NOT NULL`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var digestText string
	for rows.Next() {
		var k digestKey
		var r statementsSummaryRow
		if err := rows.Scan(&k.schema, &k.digest, &digestText, &r.calls, &r.totalTime, &r.lockTime); err != nil {
			c.logger.Warning(err)
			continue
		}
		if prev != nil {
			if p, ok := prev.rows[k]; ok {
				r.obfuscatedQueryText = p.obfuscatedQueryText
			}
		}
		if r.obfuscatedQueryText == "" {
			r.obfuscatedQueryText = obfuscate.Sql(digestText)
		}
		snapshot.rows[k] = r
	}
	return snapshot, nil
}

type queryStats struct {
	callsPerSecond     float64
	totalTimePerSecond float64
	lockTimePerSecond  float64
}

type statsWithKey struct {
	k queryKey
	s *queryStats
}

func (c *Collector) queryMetrics(ch chan<- prometheus.Metric, n int) {
	if c.perfschemaPrev == nil || c.perfschemaCurr == nil {
		return
	}
	res := map[queryKey]*queryStats{}

	interval := c.perfschemaCurr.ts.Sub(c.perfschemaPrev.ts).Seconds()

	for k, s := range c.perfschemaCurr.rows {
		prev := c.perfschemaPrev.rows[k]
		qk := queryKey{schema: k.schema, query: s.obfuscatedQueryText}

		r := res[qk]
		if r == nil {
			r = &queryStats{}
			res[qk] = r
		}
		if calls := s.calls - prev.calls; calls > 0 {
			r.callsPerSecond += float64(calls) / interval
		}
		if totalTime := s.totalTime - prev.totalTime; totalTime > 0 {
			r.totalTimePerSecond += float64(totalTime) / picoSeconds / interval
		}
		if lockTime := s.lockTime - prev.lockTime; lockTime > 0 {
			r.lockTimePerSecond += float64(lockTime) / picoSeconds / interval
		}
	}

	withKeys := make([]statsWithKey, 0, len(res))
	for k, s := range res {
		if s.callsPerSecond == 0 {
			continue
		}
		withKeys = append(withKeys, statsWithKey{k: k, s: s})
	}
	sort.Slice(withKeys, func(i, j int) bool {
		return withKeys[i].s.totalTimePerSecond > withKeys[j].s.totalTimePerSecond
	})
	if n > len(withKeys) {
		n = len(withKeys)
	}
	for _, i := range withKeys[:n] {
		ch <- common.Gauge(dQueryCalls, i.s.callsPerSecond, i.k.schema, i.k.query)
		ch <- common.Gauge(dQueryTotalTime, i.s.totalTimePerSecond, i.k.schema, i.k.query)
		ch <- common.Gauge(dQueryLockTime, i.s.lockTimePerSecond, i.k.schema, i.k.query)
	}

}
