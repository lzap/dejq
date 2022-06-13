package log

import (
	"context"
	"errors"
	"fmt"
)

// The values for log levels are chosen such that the zero value means that no
// log level was specified.
const (
	LogLevelTrace = 6
	LogLevelDebug = 5
	LogLevelInfo  = 4
	LogLevelWarn  = 3
	LogLevelError = 2
	LogLevelNone  = 1
)

// LogLevel represents the library logging level. See LogLevel* constants for
// possible values.
type LogLevel int

func (ll LogLevel) String() string {
	switch ll {
	case LogLevelTrace:
		return "trace"
	case LogLevelDebug:
		return "debug"
	case LogLevelInfo:
		return "info"
	case LogLevelWarn:
		return "warn"
	case LogLevelError:
		return "error"
	case LogLevelNone:
		return "none"
	default:
		return fmt.Sprintf("invalid level %d", ll)
	}
}

// Logger is the interface used to get logging from the library internals.
type Logger interface {
	// Log a message at the given level with data key/value pairs. data may be nil.
	Log(ctx context.Context, level LogLevel, msg string, data map[string]interface{})
}

// LogLevelFromString converts log level string to a constant.
func LogLevelFromString(s string) (LogLevel, error) {
	switch s {
	case "trace":
		return LogLevelTrace, nil
	case "debug":
		return LogLevelDebug, nil
	case "info":
		return LogLevelInfo, nil
	case "warn":
		return LogLevelWarn, nil
	case "error":
		return LogLevelError, nil
	case "none":
		return LogLevelNone, nil
	default:
		return 0, errors.New("invalid log level")
	}
}
