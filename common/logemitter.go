package common

import (
	"context"
	"time"

	"github.com/coroot/logparser"
	"go.opentelemetry.io/otel/log"
	sdk "go.opentelemetry.io/otel/sdk/log"
	semconv "go.opentelemetry.io/otel/semconv/v1.32.0"
	"k8s.io/klog"
)

const (
	MultilineCollectorTimeout = time.Second
	LogPatternsPerLevel       = 256
)

type LogEmitter struct {
	provider *sdk.LoggerProvider
	logger   log.Logger
}

func NewLogEmitter(serviceName, hostName string) (*LogEmitter, error) {
	provider, err := NewLoggerProvider(serviceName, semconv.HostName(hostName))
	if err != nil {
		return nil, err
	}
	return &LogEmitter{provider: provider, logger: provider.Logger("coroot-cluster-agent")}, nil
}

func (e *LogEmitter) Callback() logparser.OnMsgCallbackF {
	return func(ts time.Time, level logparser.Level, patternHash string, msg string, attributes map[string]string) {
		if ts.IsZero() {
			ts = time.Now()
		}
		record := log.Record{}
		record.SetTimestamp(ts)
		record.SetSeverityText(level.String())
		record.SetSeverity(severity(level))
		record.SetBody(log.StringValue(msg))
		if patternHash != "" {
			record.AddAttributes(log.String("pattern.hash", patternHash))
		}
		for k, v := range attributes {
			record.AddAttributes(log.String(k, v))
		}
		e.logger.Emit(context.TODO(), record)
	}
}

func (e *LogEmitter) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.provider.ForceFlush(ctx); err != nil {
		klog.Warningln("failed to flush logs:", err)
	}
}

func severity(level logparser.Level) log.Severity {
	switch level {
	case logparser.LevelCritical:
		return log.SeverityFatal
	case logparser.LevelError:
		return log.SeverityError
	case logparser.LevelWarning:
		return log.SeverityWarn
	case logparser.LevelInfo:
		return log.SeverityInfo
	case logparser.LevelDebug:
		return log.SeverityDebug
	}
	return log.SeverityUndefined
}
