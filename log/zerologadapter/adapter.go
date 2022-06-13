package zerologadapter

import (
	"context"

	"github.com/lzap/dejq/log"
	"github.com/rs/zerolog"
)

type Logger struct {
	logger      zerolog.Logger
	withFunc    func(context.Context, zerolog.Context) zerolog.Context
	fromContext bool
	skipModule  bool
}

type option func(logger *Logger)

// NewLogger accepts a zerolog.Logger as input and returns a new custom log
// logging facade as output.
func NewLogger(logger zerolog.Logger, options ...option) *Logger {
	l := Logger{
		logger: logger,
	}
	l.init(options)
	return &l
}

// NewContextLogger creates logger that extracts the zerolog.Logger from the
// context.Context by using `zerolog.Ctx`. The zerolog.DefaultContextLogger will
// be used if no logger is associated with the context.
func NewContextLogger(options ...option) *Logger {
	l := Logger{
		fromContext: true,
	}
	l.init(options)
	return &l
}

func (pl *Logger) init(options []option) {
	for _, opt := range options {
		opt(pl)
	}
	if !pl.skipModule {
		pl.logger = pl.logger.With().Str("module", "dejq").Logger()
	}
}

func (pl *Logger) Log(ctx context.Context, level log.LogLevel, msg string, data map[string]interface{}) {
	var zlevel zerolog.Level
	switch level {
	case log.LogLevelNone:
		zlevel = zerolog.NoLevel
	case log.LogLevelError:
		zlevel = zerolog.ErrorLevel
	case log.LogLevelWarn:
		zlevel = zerolog.WarnLevel
	case log.LogLevelInfo:
		zlevel = zerolog.InfoLevel
	case log.LogLevelDebug:
		zlevel = zerolog.DebugLevel
	default:
		zlevel = zerolog.DebugLevel
	}

	var zctx zerolog.Context
	if pl.fromContext {
		logger := zerolog.Ctx(ctx)
		zctx = logger.With()
	} else {
		zctx = pl.logger.With()
	}
	if pl.withFunc != nil {
		zctx = pl.withFunc(ctx, zctx)
	}

	zlog := zctx.Logger()
	event := zlog.WithLevel(zlevel)
	if event.Enabled() {
		if pl.fromContext && !pl.skipModule {
			event.Str("module", "dejq")
		}
		event.Fields(data).Msg(msg)
	}
}
