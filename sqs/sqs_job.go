package sqs

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type sqsJob struct {
	*types.Message
	errorChannel chan error
}

func newJob(m *types.Message) *sqsJob {
	return &sqsJob{m, make(chan error, 1)}
}

func (j *sqsJob) body() []byte {
	return []byte(*j.Message.Body)
}

func (j *sqsJob) Type() string {
	return j.attribute("job_type")
}

func (j *sqsJob) Id() string {
	return j.Id()
}

func (j *sqsJob) Decode(out interface{}) error {
	return json.Unmarshal(j.body(), &out)
}

func (j *sqsJob) attribute(key string) string {
	id, ok := j.MessageAttributes[key]
	if !ok {
		return ""
	}

	return *id.StringValue
}

// ErrorResponse is used to determine for error handling within the handler. When an error occurs,
// this function should be returned.
func (j *sqsJob) ErrorResponse(_ context.Context, err error) error {
	go func() {
		j.errorChannel <- err
	}()
	return err
}

// Success is used to determine that a handler was successful in processing the job and the job should
// now be consumed. This will delete the job from the queue
func (j *sqsJob) Success(_ context.Context) {
	go func() {
		j.errorChannel <- nil
	}()
}
