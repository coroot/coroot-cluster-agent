package k8s

import (
	"context"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/flags"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/log"
	sdk "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.32.0"
	corev1 "k8s.io/api/core/v1"
)

type EventsLogger struct {
	logger log.Logger
}

func NewEventsLogger() *EventsLogger {
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
			semconv.ServiceName("KubernetesEvents"),
		)),
	)
	return &EventsLogger{logger: provider.Logger("coroot-cluster-agent")}
}

func (l *EventsLogger) EmitEvent(event *corev1.Event) {
	record := log.Record{}
	record.SetTimestamp(event.LastTimestamp.Time)
	record.SetSeverityText(event.Type)
	switch event.Type {
	case corev1.EventTypeNormal:
		record.SetSeverity(log.SeverityInfo)
	case corev1.EventTypeWarning:
		record.SetSeverity(log.SeverityWarn)
	}
	record.SetBody(log.StringValue(event.Message))
	record.AddAttributes(
		log.String("event.name", event.Name),
		log.String("event.namespace", event.Namespace),
		log.String("event.reason", event.Reason),
		log.String("object.kind", event.InvolvedObject.Kind),
		log.String("object.name", event.InvolvedObject.Name),
		log.String("object.namespace", event.InvolvedObject.Namespace),
		log.String("source.component", event.Source.Component),
		log.String("source.host", event.Source.Host),
	)
	l.logger.Emit(context.TODO(), record)
}
