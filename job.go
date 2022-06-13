package dejq

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Job interface {
	Type() string
	Decode(out interface{}) error
	Attribute(key string) string
}

type sqsJob struct {
	*types.Message
	err chan error
}

func newJob(m *types.Message) *sqsJob {
	return &sqsJob{m, make(chan error, 1)}
}

func (m *sqsJob) body() []byte {
	return []byte(*m.Message.Body)
}

func (m *sqsJob) Type() string {
	return *m.MessageAttributes["type"].StringValue
}

func (m *sqsJob) Decode(out interface{}) error {
	return json.Unmarshal(m.body(), &out)
}

func (m *sqsJob) Attribute(key string) string {
	id, ok := m.MessageAttributes[key]
	if !ok {
		return ""
	}

	return *id.StringValue
}

// ErrorResponse is used to determine for error handling within the handler. When an error occurs,
// this function should be returned.
func (m *sqsJob) ErrorResponse(ctx context.Context, err error) error {
	go func() {
		m.err <- err
	}()
	return err
}

// Success is used to determine that a handler was successful in processing the job and the job should
// now be consumed. This will delete the job from the queue
func (m *sqsJob) Success(ctx context.Context) error {
	go func() {
		m.err <- nil
	}()

	return nil
}
