package common

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	sdk "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.32.0"
)

type LogsPipeline struct {
	processor *sdk.BatchProcessor
}

func NewLogsPipeline(endpoint *url.URL, headers map[string]string, tlsConfig *tls.Config) (*LogsPipeline, error) {
	if endpoint == nil {
		return nil, fmt.Errorf("logs endpoint is not configured")
	}
	opts := []otlploghttp.Option{
		otlploghttp.WithEndpointURL(endpoint.String()),
		otlploghttp.WithHeaders(headers),
	}
	if endpoint.Scheme == "https" {
		opts = append(opts, otlploghttp.WithTLSClientConfig(tlsConfig))
	}
	exporter, err := otlploghttp.New(context.Background(), opts...)
	if err != nil {
		return nil, err
	}
	return &LogsPipeline{processor: sdk.NewBatchProcessor(exporter)}, nil
}

func (p *LogsPipeline) NewLoggerProvider(serviceName string, resourceAttrs ...attribute.KeyValue) (*sdk.LoggerProvider, error) {
	if p == nil {
		return nil, fmt.Errorf("logs forwarding is not configured")
	}
	attrs := append([]attribute.KeyValue{semconv.ServiceName(serviceName)}, resourceAttrs...)
	return sdk.NewLoggerProvider(
		sdk.WithProcessor(p.processor),
		sdk.WithResource(resource.NewWithAttributes(semconv.SchemaURL, attrs...)),
	), nil
}

func (p *LogsPipeline) Shutdown(ctx context.Context) error {
	if p == nil {
		return nil
	}
	return p.processor.Shutdown(ctx)
}
