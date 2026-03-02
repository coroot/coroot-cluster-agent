package dbtracker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_TrimTopTables(t *testing.T) {
	dbSizes := map[string]*DBSizeSnapshot{
		"db1": {Tables: []TableSizeEntry{
			{TableKey: TableKey{DB: "db1", Table: "big"}, Size: 1000},
			{TableKey: TableKey{DB: "db1", Table: "small"}, Size: 10},
		}},
		"db2": {Tables: []TableSizeEntry{
			{TableKey: TableKey{DB: "db2", Table: "medium"}, Size: 500},
		}},
	}

	trimTopTables(dbSizes, 2)

	var allTables []TableSizeEntry
	for _, snap := range dbSizes {
		allTables = append(allTables, snap.Tables...)
	}
	assert.Equal(t, 2, len(allTables))
}
