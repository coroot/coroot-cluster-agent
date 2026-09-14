package mysql

import (
	"context"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/obfuscate"
	"github.com/prometheus/client_golang/prometheus"
)

const topTransactionsN = 20
const minLongTransactionSeconds = 10

type innodbTrx struct {
	byQuery map[string]float64 // obfuscated query shape -> max age (seconds)
}

func (c *Collector) innodbTrxSnapshot(ctx context.Context) {
	trx, err := c.queryInnodbTrx(ctx)
	if err != nil {
		c.logger.Warning(err)
		c.scrapeErrors[err.Error()] = true
		return
	}
	c.innodbTrx = trx
}

func (c *Collector) queryInnodbTrx(ctx context.Context) (*innodbTrx, error) {
	res := &innodbTrx{byQuery: map[string]float64{}}

	rows, err := c.db.QueryContext(ctx, `
		SELECT
		    TIMESTAMPDIFF(SECOND, trx_started, NOW()),
		    trx_state,
		    IFNULL(trx_query, '')
		FROM information_schema.innodb_trx`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var age float64
		var state, query string
		if err := rows.Scan(&age, &state, &query); err != nil {
			c.logger.Warning(err)
			continue
		}
		if age < minLongTransactionSeconds {
			continue
		}
		label := obfuscate.Sql(query)
		if label == "" {
			label = "(idle in transaction)"
		}
		if age > res.byQuery[label] {
			res.byQuery[label] = age
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collector) innodbTrxMetrics(ch chan<- prometheus.Metric) {
	if c.innodbTrx == nil {
		return
	}
	for query, age := range common.TopNMapByValue(c.innodbTrx.byQuery, topTransactionsN) {
		ch <- common.Gauge(dInnodbTransactionSeconds, age, query)
	}
}
