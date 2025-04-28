package mysql

import (
	"context"
	"sort"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"

	"github.com/prometheus/client_golang/prometheus"
)

type tableKey struct {
	schema string
	table  string
}

type ioSummary struct {
	readTotalTime  uint64
	writeTotalTime uint64
}

type ioByTableSnapshot struct {
	ts   time.Time
	rows map[tableKey]ioSummary
}

func (c *Collector) queryTableIOWaits(ctx context.Context) (*ioByTableSnapshot, error) {
	snapshot := &ioByTableSnapshot{ts: time.Now(), rows: map[tableKey]ioSummary{}}
	q := `
	SELECT
    	OBJECT_SCHEMA, 
    	OBJECT_NAME, 
    	SUM_TIMER_READ, 
    	SUM_TIMER_WRITE 
	FROM performance_schema.table_io_waits_summary_by_table 
	WHERE 
	    OBJECT_SCHEMA is not null AND 
	    OBJECT_NAME is not null`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var k tableKey
		var r ioSummary
		if err := rows.Scan(&k.schema, &k.table, &r.readTotalTime, &r.writeTotalTime); err != nil {
			c.logger.Warning(err)
			continue
		}
		snapshot.rows[k] = r
	}
	return snapshot, nil
}

type ioStats struct {
	readTimePerSecond  float64
	writeTimePerSecond float64
	totalTimePerSecond float64
}

type ioStatsWithKey struct {
	k tableKey
	s *ioStats
}

func (c *Collector) ioMetrics(ch chan<- prometheus.Metric, n int) {
	if c.ioByTablePrev == nil || c.ioByTableCurr == nil {
		return
	}
	res := map[tableKey]*ioStats{}

	withKeys := make([]ioStatsWithKey, 0, len(res))

	interval := c.ioByTableCurr.ts.Sub(c.ioByTablePrev.ts).Seconds()
	for k, s := range c.ioByTableCurr.rows {
		prev := c.ioByTablePrev.rows[k]
		stats := &ioStats{}
		if v := s.readTotalTime - prev.readTotalTime; v > 0 {
			stats.readTimePerSecond = float64(v) / picoSeconds / interval
		}
		if v := s.writeTotalTime - prev.writeTotalTime; v > 0 {
			stats.writeTimePerSecond = float64(v) / picoSeconds / interval
		}
		stats.totalTimePerSecond = stats.readTimePerSecond + stats.writeTimePerSecond
		if stats.totalTimePerSecond > 0 {
			withKeys = append(withKeys, ioStatsWithKey{k: k, s: stats})
		}
	}
	sort.Slice(withKeys, func(i, j int) bool {
		return withKeys[i].s.totalTimePerSecond > withKeys[j].s.totalTimePerSecond
	})
	if n > len(withKeys) {
		n = len(withKeys)
	}
	for _, i := range withKeys[:n] {
		ch <- common.Gauge(dIOTime, i.s.readTimePerSecond, i.k.schema, i.k.table, "read")
		ch <- common.Gauge(dIOTime, i.s.writeTimePerSecond, i.k.schema, i.k.table, "write")
	}

}
