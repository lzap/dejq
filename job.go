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

func (j *sqsJob) body() []byte {
	return []byte(*j.Message.Body)
}

func (j *sqsJob) Type() string {
	return j.Attribute("job_type")
}

func (j *sqsJob) Decode(out interface{}) error {
	return json.Unmarshal(j.body(), &out)
}

func (j *sqsJob) Attribute(key string) string {
	id, ok := j.MessageAttributes[key]
	if !ok {
		return ""
	}

	return *id.StringValue
}

// ErrorResponse is used to determine for error handling within the handler. When an error occurs,
// this function should be returned.
func (j *sqsJob) ErrorResponse(ctx context.Context, err error) error {
	go func() {
		j.err <- err
	}()
	return err
}

// Success is used to determine that a handler was successful in processing the job and the job should
// now be consumed. This will delete the job from the queue
func (j *sqsJob) Success(ctx context.Context) {
	go func() {
		j.err <- nil
	}()
}
