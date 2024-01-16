package profiles

import (
	"context"
	"fmt"

	"github.com/ClickHouse/ch-go"
)

const (
	stacksTable = `
CREATE TABLE IF NOT EXISTS profiling_stacks (
	ServiceName LowCardinality(String) CODEC(ZSTD(1)),
	Hash UInt64 CODEC(ZSTD(1)),
	LastSeen DateTime64(9) CODEC(Delta, ZSTD(1)),
	Stack Array(String) CODEC(ZSTD(1))
) 
ENGINE ReplacingMergeTree()
PRIMARY KEY (ServiceName, Hash)
TTL toDateTime(LastSeen) + toIntervalDay(%d)
PARTITION BY toDate(LastSeen)
ORDER BY (ServiceName, Hash)
`

	samplesTable = `
CREATE TABLE IF NOT EXISTS profiling_samples (
	ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    Type LowCardinality(String) CODEC(ZSTD(1)),
	Start DateTime64(9) CODEC(Delta, ZSTD(1)),
	End DateTime64(9) CODEC(Delta, ZSTD(1)),
	Labels Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	StackHash UInt64 CODEC(ZSTD(1)),
	Value Int64 CODEC(ZSTD(1))
) ENGINE MergeTree()
TTL toDateTime(Start) + toIntervalDay(%d)
PARTITION BY toDate(Start)
ORDER BY (ServiceName, Type, toUnixTimestamp(Start), toUnixTimestamp(End))
`
	profilesTable = `
CREATE TABLE IF NOT EXISTS profiling_profiles (
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    Type LowCardinality(String) CODEC(ZSTD(1)),
    LastSeen DateTime64(9) CODEC(Delta, ZSTD(1))
)
ENGINE ReplacingMergeTree()
PRIMARY KEY (ServiceName, Type)
TTL toDateTime(LastSeen) + toIntervalDay(%d)
PARTITION BY toDate(LastSeen)
`
	profilesView = `
CREATE MATERIALIZED VIEW IF NOT EXISTS profiling_profiles_mv TO profiling_profiles AS
SELECT ServiceName, Type, max(End) AS LastSeen FROM profiling_samples group by ServiceName, Type
`
)

func (ps *Profiles) createTables() error {
	for _, q := range []string{stacksTable, samplesTable, profilesTable, profilesView} {
		switch q {
		case stacksTable, samplesTable, profilesTable:
			q = fmt.Sprintf(q, ps.cfg.TTLDays)
		}
		err := ps.clickhouse.Do(context.Background(), ch.Query{Body: q})
		if err != nil {
			return err
		}
	}
	return nil
}
