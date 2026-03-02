package schema

import (
	"strings"

	"github.com/pmezard/go-difflib/difflib"
)

// Snapshot maps "database/schema.table" (or any key) -> text content.
type Snapshot map[string]string

const (
	ChangeTypeChanged = "changed"
)

type Change struct {
	Database string
	Object   string // "schema.table", "pg_settings", etc.
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
			db, object := splitKey(key)
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
