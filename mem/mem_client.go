package mem

import (
	"context"

	"github.com/go-logr/logr"
	"github.com/lzap/dejq"
)

type memJob struct {
	*dejq.PendingJob
}

func newJob(job *dejq.PendingJob) *memJob {
	return &memJob{PendingJob: job}
}

func (j *memJob) Type() string {
	return j.PendingJob.Type
}

func (j *memJob) Decode(out interface{}) error {
	out = j.PendingJob.Body
	return nil
}

type client struct {
	logger   logr.Logger
	handlers map[string]dejq.Handler
	todo     chan dejq.Job
}

func NewClient(_ context.Context, logger logr.Logger) (*client, error) {
	return &client{
		logger:   logger,
		handlers: make(map[string]dejq.Handler),
		todo:     make(chan dejq.Job),
	}, nil
}

func (c *client) RegisterHandler(name string, h dejq.Handler) {
	c.handlers[name] = h
}

func (c *client) Enqueue(_ context.Context, jobs ...dejq.PendingJob) error {
	for _, job := range jobs {
		c.logger.Info("enqueuing job", "type", job.Type)
		j := newJob(&job)
		c.todo <- j
	}
	return nil
}

func (c *client) Stop() {
	c.logger.Info("sending stop signal")
	close(c.todo)
}

func (c *client) DequeueLoop(ctx context.Context) {
	for job := range c.todo {
		c.logger.Info("dequeuing job", "type", job.Type())
		if h, ok := c.handlers[job.Type()]; ok {
			if err := h(ctx, job); err != nil {
				panic(err)
			}
		}
	}
}
