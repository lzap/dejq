package stdoutadapter

import (
	"context"
	"fmt"

	"github.com/aws/smithy-go/logging"
	"github.com/lzap/dejq/log"
)

type stdoutAdapter struct{}

func NewLogger() *stdoutAdapter {
	return &stdoutAdapter{}
}

func (a *stdoutAdapter) Log(_ context.Context, _ log.LogLevel, msg string, _ map[string]interface{}) {
	fmt.Println(msg)
}

func (a *stdoutAdapter) Logf(_ logging.Classification, format string, msg ...interface{}) {
	fmt.Printf(format, msg)
}
