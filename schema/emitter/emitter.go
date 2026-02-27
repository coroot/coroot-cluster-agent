package emitter

import (
	"context"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/flags"
	"github.com/coroot/coroot-cluster-agent/schema"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/log"
	sdk "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.32.0"
)

type ChangeEmitter struct {
	logger log.Logger
}

func NewChangeEmitter() *ChangeEmitter {
	opts := []otlploghttp.Option{
		otlploghttp.WithEndpointURL((*flags.CorootURL).JoinPath("/v1/logs").String()),
		otlploghttp.WithHeaders(common.AuthHeaders(*flags.APIKey)),
	}
	if *flags.InsecureSkipVerify {
		opts = append(opts, otlploghttp.WithInsecure())
	}
	exporter, _ := otlploghttp.New(context.Background(), opts...)
	batcher := sdk.NewBatchProcessor(exporter)
	provider := sdk.NewLoggerProvider(
		sdk.WithProcessor(batcher),
		sdk.WithResource(resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName("DatabaseSchemaChanges"),
		)),
	)
	return &ChangeEmitter{logger: provider.Logger("coroot-cluster-agent")}
}

func (e *ChangeEmitter) Emit(change schema.Change, dbSystem, targetAddr string) {
	record := log.Record{}
	record.SetTimestamp(time.Now())
	record.SetSeverity(log.SeverityInfo)
	record.SetSeverityText("Info")

	changeType := "changed"
	if change.IsCreate {
		changeType = "created"
	} else if change.IsDrop {
		changeType = "dropped"
	}

	record.SetBody(log.StringValue(change.Diff))
	record.AddAttributes(
		log.String("db.system", dbSystem),
		log.String("db.target", targetAddr),
		log.String("db.name", change.Database),
		log.String("schema_change.table", change.Table),
		log.String("schema_change.type", changeType),
	)
	e.logger.Emit(context.TODO(), record)
}
