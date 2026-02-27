package schema

import (
	"sort"
	"strings"

	"github.com/pmezard/go-difflib/difflib"
	"golang.org/x/exp/maps"
)

// Snapshot maps "database/schema.table" -> DDL text.
type Snapshot map[string]string

type Change struct {
	Database string
	Table    string // "schema.table"
	Diff     string
	IsCreate bool
	IsDrop   bool
}

func Diff(prev, curr Snapshot) []Change {
	if prev == nil {
		return nil
	}
	var changes []Change

	keys := map[string]struct{}{}
	for k := range prev {
		keys[k] = struct{}{}
	}
	for k := range curr {
		keys[k] = struct{}{}
	}
	sorted := maps.Keys(keys)
	sort.Strings(sorted)

	for _, key := range sorted {
		db, table := splitKey(key)
		oldDDL, inPrev := prev[key]
		newDDL, inCurr := curr[key]

		switch {
		case !inPrev && inCurr:
			changes = append(changes, Change{
				Database: db,
				Table:    table,
				Diff:     unifiedDiff(key, "", newDDL),
				IsCreate: true,
			})
		case inPrev && !inCurr:
			changes = append(changes, Change{
				Database: db,
				Table:    table,
				Diff:     unifiedDiff(key, oldDDL, ""),
				IsDrop:   true,
			})
		case oldDDL != newDDL:
			changes = append(changes, Change{
				Database: db,
				Table:    table,
				Diff:     unifiedDiff(key, oldDDL, newDDL),
			})
		}
	}
	return changes
}

func splitKey(key string) (database, table string) {
	i := strings.IndexByte(key, '/')
	if i < 0 {
		return "", key
	}
	return key[:i], key[i+1:]
}

func unifiedDiff(name, old, new string) string {
	diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:        difflib.SplitLines(old),
		B:        difflib.SplitLines(new),
		FromFile: name,
		ToFile:   name,
		Context:  3,
	})
	return diff
}
