package common

import (
	"context"
	"time"

	"github.com/coroot/logparser"
	"go.opentelemetry.io/otel/log"
	sdk "go.opentelemetry.io/otel/sdk/log"
	"k8s.io/klog"
)

type LogEmitter struct {
	provider *sdk.LoggerProvider
	logger   log.Logger
}

func NewLogEmitter(serviceName string) (*LogEmitter, error) {
	provider, err := NewLoggerProvider(serviceName)
	if err != nil {
		return nil, err
	}
	return &LogEmitter{provider: provider, logger: provider.Logger("coroot-cluster-agent")}, nil
}

func (e *LogEmitter) Callback() logparser.OnMsgCallbackF {
	return func(ts time.Time, level logparser.Level, patternHash string, msg string) {
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
