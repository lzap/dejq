package mem

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/lzap/dejq"
)

type memJob struct {
	TypeData string
	JSONData []byte
}

func newJob(job *dejq.PendingJob) (*memJob, error) {
	buffer, err := json.Marshal(job.Body)
	if err != nil {
		return nil, err
	}
	return &memJob{TypeData: job.Type, JSONData: buffer}, nil
}

func (j *memJob) Type() string {
	return j.TypeData
}

func (j *memJob) Decode(out interface{}) error {
	return json.Unmarshal(j.JSONData, &out)
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
		c.logger.Info("enqueuing job", "type", job.Type, "args", fmt.Sprintf("%+v", job.Body))
		j, err := newJob(&job)
		if err != nil {
			c.logger.Error(err, "unable to marshal job data")
		}
		c.todo <- j
	}
	return nil
}

func (c *client) Stop() {
	c.logger.Info("sending stop signal")
	close(c.todo)
}

func (c *client) DequeueLoop(ctx context.Context) {
	go c.dequeueLoop(ctx)
}

func (c *client) dequeueLoop(ctx context.Context) {
	for job := range c.todo {
		c.logger.Info("dequeuing job", "type", job.Type())
		if h, ok := c.handlers[job.Type()]; ok {
			if err := h(ctx, job); err != nil {
				c.logger.Error(err, "job handler returned an error, memory queue panics now")
				panic(err)
			}
		}
	}
}
