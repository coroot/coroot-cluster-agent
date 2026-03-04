package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/coroot/coroot-cluster-agent/schema"
	"github.com/pmezard/go-difflib/difflib"
)

type Setting struct {
	Name     string
	Unit     string
	Value    float64
	RawValue string
	Source   string // default, configuration file, command line, environment variable, global, override, session, client, etc.
	IsMetric bool   // true for integer, real, bool vartypes
}

func (c *Collector) getSettings(ctx context.Context) ([]Setting, error) {
	rows, err := c.db.QueryContext(ctx, `SELECT name, setting, unit, vartype, source FROM pg_settings ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []Setting
	for rows.Next() {
		var name, value, unit, vartype, source sql.NullString
		if err := rows.Scan(&name, &value, &unit, &vartype, &source); err != nil {
			c.logger.Warning("failed to scan pg_settings row:", err)
			continue
		}
		s := Setting{
			Name:     name.String,
			Unit:     unit.String,
			RawValue: value.String,
			Source:   source.String,
		}
		switch vartype.String {
		case "integer", "real":
			v, err := strconv.ParseFloat(value.String, 64)
			if err != nil {
				c.logger.Warningf("failed to parse value for %s=%s setting: %s", name.String, value.String, err)
				continue
			}
			s.Value = v
			s.IsMetric = true
		case "bool":
			if value.String == "on" {
				s.Value = 1
			}
			s.IsMetric = true
		}
		res = append(res, s)
	}
	return res, nil
}

func settingsToText(settings []Setting) string {
	var buf strings.Builder
	for _, s := range settings {
		switch s.Source {
		case "override", "session", "client":
			continue
		}
		fmt.Fprintf(&buf, "%s = %s\n", s.Name, s.RawValue)
	}
	return buf.String()
}

func (c *Collector) trackSettingsChanges() {
	curr := settingsToText(c.settings)
	if c.prevSettingsText != "" && curr != c.prevSettingsText {
		diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A:        difflib.SplitLines(c.prevSettingsText),
			B:        difflib.SplitLines(curr),
			FromFile: "pg_settings",
			ToFile:   "pg_settings",
			Context:  3,
		})
		c.emitter.Emit(schema.Change{
			Object: "pg_settings",
			Type:   schema.ChangeTypeChanged,
			Diff:   diff,
		}, "postgresql", c.targetAddr)
	}
	c.prevSettingsText = curr
}
