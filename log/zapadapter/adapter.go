package zapadapter

import (
	"context"

	"github.com/lzap/dejq/log"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger struct {
	logger *zap.Logger
}

func NewLogger(logger *zap.Logger) *Logger {
	return &Logger{logger: logger.WithOptions(zap.AddCallerSkip(1))}
}

func (pl *Logger) Log(ctx context.Context, level log.LogLevel, msg string, data map[string]interface{}) {
	fields := make([]zapcore.Field, len(data))
	i := 0
	for k, v := range data {
		fields[i] = zap.Any(k, v)
		i++
	}

	switch level {
	case log.LogLevelTrace:
		pl.logger.Debug(msg, append(fields, zap.Stringer("ORIG_LOG_LEVEL", level))...)
	case log.LogLevelDebug:
		pl.logger.Debug(msg, fields...)
	case log.LogLevelInfo:
		pl.logger.Info(msg, fields...)
	case log.LogLevelWarn:
		pl.logger.Warn(msg, fields...)
	case log.LogLevelError:
		pl.logger.Error(msg, fields...)
	default:
		pl.logger.Error(msg, append(fields, zap.Stringer("ORIG_LOG_LEVEL", level))...)
	}
}
