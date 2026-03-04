package schema

import (
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
