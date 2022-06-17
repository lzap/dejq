package sqs

import (
	"fmt"

	"github.com/aws/smithy-go/logging"
	"github.com/go-logr/logr"
)

type awsAdapter struct {
	logr.Logger
}

func NewAWSLogrAdapter(logger logr.Logger) *awsAdapter {
	return &awsAdapter{Logger: logger}
}

func (a *awsAdapter) Logf(_ logging.Classification, format string, msg ...interface{}) {
	a.Logger.Info(fmt.Sprintf(format, msg))
}
