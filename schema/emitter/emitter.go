package emitter

import (
	"context"
	"time"
	"unicode/utf8"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/schema"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
)

const (
	maxDDLSize = 16 * 1024
)

type Emitter struct {
	logger       log.Logger
	schemaLogger log.Logger
}

func NewEmitter(logs *common.LogsPipeline, resourceAttrs ...attribute.KeyValue) (*Emitter, error) {
	provider, err := logs.NewLoggerProvider("DatabaseChanges", resourceAttrs...)
	if err != nil {
		return nil, err
	}
	schemaProvider, err := logs.NewLoggerProvider("DatabaseSchema", resourceAttrs...)
	if err != nil {
		return nil, err
	}
	return &Emitter{
		logger:       provider.Logger("coroot-cluster-agent"),
		schemaLogger: schemaProvider.Logger("coroot-cluster-agent"),
	}, nil
}

func (e *Emitter) EmitSchema(rec schema.TableRecord, dbSystem, targetAddr string) {
	record := log.Record{}
	record.SetTimestamp(time.Now())
	record.SetSeverity(log.SeverityInfo)
	record.SetSeverityText("Info")
	ddl := rec.DDL
	if len(ddl) > maxDDLSize {
		ddl = truncateUTF8(ddl, maxDDLSize) + "\n-- truncated\n"
	}
	record.SetBody(log.StringValue(ddl))
	record.AddAttributes(
		log.String("db.system", dbSystem),
		log.String("db.target", targetAddr),
		log.String("db.name", rec.Key.DB),
		log.String(schema.AttrTable, rec.Key.Table),
	)
	if rec.Key.Schema != "" {
		record.AddAttributes(log.String(schema.AttrSchema, rec.Key.Schema))
	}
	if rec.Dropped {
		record.AddAttributes(log.String(schema.AttrDropped, "true"))
	}
	for k, v := range rec.Stats {
		record.AddAttributes(log.String(k, v))
	}
	e.schemaLogger.Emit(context.TODO(), record)
}

func (e *Emitter) EmitChange(change schema.Change, dbSystem, targetAddr string) {
	record := log.Record{}
	record.SetTimestamp(time.Now())
	record.SetSeverity(log.SeverityInfo)
	record.SetSeverityText("Info")
	record.SetBody(log.StringValue(change.Diff))
	record.AddAttributes(
		log.String("db.system", dbSystem),
		log.String("db.target", targetAddr),
		log.String("db.name", change.Database),
		log.String("db_change.object", change.Object),
		log.String("db_change.type", change.Type),
	)
	e.logger.Emit(context.TODO(), record)
}

func truncateUTF8(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	for limit > 0 && !utf8.RuneStart(s[limit]) {
		limit--
	}
	return s[:limit]
}
