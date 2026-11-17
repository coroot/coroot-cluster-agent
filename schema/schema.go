package schema

import (
	"encoding/json"
	"strconv"

	"github.com/pmezard/go-difflib/difflib"
)

type TableKey struct {
	DB     string
	Schema string
	Table  string
}

type Snapshot map[TableKey]string

const (
	ChangeTypeChanged = "changed"
)

type Change struct {
	Database string
	Schema   string
	Object   string
	Type     string
	Diff     string
}

func Diff(prev, curr Snapshot) []Change {
	if prev == nil {
		return nil
	}
	var changes []Change
	for key, oldText := range prev {
		newText, ok := curr[key]
		if ok && newText != oldText {
			changes = append(changes, Change{
				Database: key.DB,
				Schema:   key.Schema,
				Object:   key.Table,
				Type:     ChangeTypeChanged,
				Diff:     unifiedDiff(key, oldText, newText),
			})
		}
	}
	return changes
}

func unifiedDiff(key TableKey, old, new string) string {
	name := key.DB + "/" + key.Table
	if key.Schema != "" {
		name = key.DB + "/" + key.Schema + "/" + key.Table
	}
	diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:        difflib.SplitLines(old),
		B:        difflib.SplitLines(new),
		FromFile: name,
		ToFile:   name,
		Context:  3,
	})
	return diff
}

const (
	AttrSchema           = "db.schema"
	AttrTable            = "db.table"
	AttrDropped          = "db.table.dropped"
	AttrRows             = "db.table.rows"
	AttrDataSize         = "db.table.data_size_bytes"
	AttrIndexesSize      = "db.table.indexes_size_bytes"
	AttrSeqScans         = "db.table.seq_scans"
	AttrSeqRowsRead      = "db.table.seq_rows_read"
	AttrIndexScans       = "db.table.index_scans"
	AttrIndexRowsFetched = "db.table.index_rows_fetched"
	AttrRowsInserted     = "db.table.rows_inserted"
	AttrRowsUpdated      = "db.table.rows_updated"
	AttrRowsDeleted      = "db.table.rows_deleted"
	AttrLastAnalyze      = "db.table.last_analyze"
	AttrStatsSince       = "db.table.stats_since"
	AttrColumns          = "db.table.columns"
	AttrIndexes          = "db.table.indexes"

	maxJSONAttrSize = 32 * 1024
)

type Column struct {
	Name     string  `json:"name"`
	Type     string  `json:"type"`
	Nullable bool    `json:"nullable,omitempty"`
	Default  *string `json:"default,omitempty"`
	Extra    string  `json:"extra,omitempty"`
}

type Index struct {
	Name string `json:"name"`
	// Columns are the key columns or expressions in the index order.
	Columns []string `json:"columns,omitempty"`
	// Include are the non-key columns stored in the index (covering indexes).
	Include []string `json:"include,omitempty"`
	// Method is the index type: btree, gist, gin, hash, fulltext, ...
	Method string `json:"method,omitempty"`
	// Predicate is the condition of a partial index.
	Predicate string `json:"predicate,omitempty"`

	SizeBytes   *int64 `json:"size_bytes,omitempty"`
	Scans       *int64 `json:"scans,omitempty"`
	RowsRead    *int64 `json:"rows_read,omitempty"`
	RowsFetched *int64 `json:"rows_fetched,omitempty"`
	Cardinality *int64 `json:"cardinality,omitempty"`
	Primary     bool   `json:"primary,omitempty"`
	Unique      bool   `json:"unique,omitempty"`
	Invalid     bool   `json:"invalid,omitempty"`
}

type TableStats map[string]string

func (s TableStats) SetInt(attr string, v int64) {
	s[attr] = strconv.FormatInt(v, 10)
}

func (s TableStats) SetColumns(columns []Column) {
	s.setJSON(AttrColumns, columns, len(columns))
}

func (s TableStats) SetIndexes(indexes []Index) {
	s.setJSON(AttrIndexes, indexes, len(indexes))
}

func (s TableStats) setJSON(attr string, v any, n int) {
	if n == 0 {
		return
	}
	if data, err := json.Marshal(v); err == nil && len(data) <= maxJSONAttrSize {
		s[attr] = string(data)
	}
}

type TableRecord struct {
	Key     TableKey
	DDL     string
	Stats   TableStats
	Dropped bool
}
