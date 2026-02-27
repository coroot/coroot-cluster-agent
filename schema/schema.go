package schema

import (
	"sort"
	"strings"

	"github.com/pmezard/go-difflib/difflib"
	"golang.org/x/exp/maps"
)

// Snapshot maps "database/schema.table" (or any key) -> text content.
type Snapshot map[string]string

const (
	ChangeTypeCreated = "created"
	ChangeTypeDropped = "dropped"
	ChangeTypeChanged = "changed"
)

type Change struct {
	Database string
	Object   string // "schema.table", "pg_settings", etc.
	Type     string // ChangeTypeCreated, ChangeTypeDropped, ChangeTypeChanged
	Diff     string
}

// Diff compares prev and curr snapshots and returns detected changes.
// Returns nil if prev is nil (first run).
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
		db, object := splitKey(key)
		oldText, inPrev := prev[key]
		newText, inCurr := curr[key]

		switch {
		case !inPrev && inCurr:
			changes = append(changes, Change{
				Database: db,
				Object:   object,
				Type:     ChangeTypeCreated,
				Diff:     unifiedDiff(key, "", newText),
			})
		case inPrev && !inCurr:
			changes = append(changes, Change{
				Database: db,
				Object:   object,
				Type:     ChangeTypeDropped,
				Diff:     unifiedDiff(key, oldText, ""),
			})
		case oldText != newText:
			changes = append(changes, Change{
				Database: db,
				Object:   object,
				Type:     ChangeTypeChanged,
				Diff:     unifiedDiff(key, oldText, newText),
			})
		}
	}
	return changes
}

func splitKey(key string) (database, object string) {
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
