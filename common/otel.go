package common

import (
	"context"
	"sync"

	"github.com/coroot/coroot-cluster-agent/flags"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	sdk "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.32.0"
)

var (
	logsOnce      sync.Once
	logsProcessor *sdk.BatchProcessor
	logsErr       error
)

func logsPipeline() (*sdk.BatchProcessor, error) {
	logsOnce.Do(func() {
		opts := []otlploghttp.Option{
			otlploghttp.WithEndpointURL((*flags.CorootURL).JoinPath("/v1/logs").String()),
			otlploghttp.WithHeaders(AuthHeaders(*flags.APIKey)),
		}
		if (*flags.CorootURL).Scheme == "https" {
			opts = append(opts, otlploghttp.WithTLSClientConfig(TlsConfig()))
		}
		exporter, err := otlploghttp.New(context.Background(), opts...)
		if err != nil {
			logsErr = err
			return
		}
		logsProcessor = sdk.NewBatchProcessor(exporter)
	})
	return logsProcessor, logsErr
}

func NewLoggerProvider(serviceName string, resourceAttrs ...attribute.KeyValue) (*sdk.LoggerProvider, error) {
	processor, err := logsPipeline()
	if err != nil {
		return nil, err
	}
	attrs := append([]attribute.KeyValue{semconv.ServiceName(serviceName)}, resourceAttrs...)
	return sdk.NewLoggerProvider(
		sdk.WithProcessor(processor),
		sdk.WithResource(resource.NewWithAttributes(semconv.SchemaURL, attrs...)),
	), nil
}

func ShutdownLogs(ctx context.Context) error {
	if logsProcessor == nil {
		return nil
	}
	return logsProcessor.Shutdown(ctx)
}
