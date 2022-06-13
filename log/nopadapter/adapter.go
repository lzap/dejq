package nopadapter

import (
	"context"

	"github.com/aws/smithy-go/logging"
	"github.com/lzap/dejq/log"
)

type nop struct{}

func NewLogger() *nop {
	return &nop{}
}

func (_ *nop) Log(_ context.Context, _ log.LogLevel, _ string, _ map[string]interface{}) {
	// no operation
}

func (_ *nop) Logf(_ logging.Classification, _ string, _ ...interface{}) {
	// no operation
}
