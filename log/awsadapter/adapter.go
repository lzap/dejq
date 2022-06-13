package awsadapter

import (
	"context"
	"fmt"

	"github.com/aws/smithy-go/logging"
	"github.com/lzap/dejq/log"
)

type awsAdapter struct {
	log.Logger
}

func NewLogger(logger log.Logger) *awsAdapter {
	return &awsAdapter{Logger: logger}
}

func (a *awsAdapter) Logf(_ logging.Classification, format string, msg ...interface{}) {
	a.Logger.Log(context.Background(), log.LogLevelDebug, fmt.Sprintf(format, msg), nil)
}
