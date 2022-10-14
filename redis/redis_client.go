package redis

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/go-logr/logr"
	"github.com/go-redis/redis/v8"
	"github.com/lzap/dejq"
)

type job struct {
	PayloadType string `json:"t"`
	Payload     string `json:"b"`
}

func newJob(pj *dejq.PendingJob) (*job, error) {
	buffer, err := json.Marshal(pj.Body)
	if err != nil {
		return nil, err
	}
	return &job{
		PayloadType: pj.Type,
		Payload:     string(buffer),
	}, nil
}

func (j *job) Type() string {
	return j.PayloadType
}

func (j *job) Decode(out interface{}) error {
	return json.Unmarshal([]byte(j.Payload), &out)
}

type client struct {
	logger     logr.Logger
	handlers   map[string]dejq.Handler
	client     *redis.Client
	listName   string
	subscriber *redis.PubSub
	workerWG   sync.WaitGroup
}

func NewClient(_ context.Context, logger logr.Logger, address, username, password string, db int, listName string) (*client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     address,
		Username: username,
		Password: password,
		DB:       db,
	})
	return &client{
		logger:   logger,
		handlers: make(map[string]dejq.Handler),
		client:   rdb,
		listName: listName,
	}, nil
}

func (c *client) RegisterHandler(name string, h dejq.Handler) {
	c.handlers[name] = h
}

func (c *client) marshalJobs(jobs []dejq.PendingJob) ([]*job, error) {
	var err error
	result := make([]*job, len(jobs))
	for i, job := range jobs {
		result[i], err = newJob(&job)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (c *client) Enqueue(ctx context.Context, jobs ...dejq.PendingJob) error {
	redisJobs, err := c.marshalJobs(jobs)
	if err != nil {
		c.logger.Error(err, "unable to marshal job data")
		return err
	}

	buffer, err := json.Marshal(redisJobs)
	if err != nil {
		return err
	}

	cmd := c.client.Publish(ctx, c.listName, buffer)
	if cmd.Err() != nil {
		c.logger.Error(cmd.Err(), "unable to publish job", "payload", string(buffer))
		return cmd.Err()
	}

	c.logger.Info("enqueued job", "payload", string(buffer), "result", cmd.String())
	return nil
}

func (c *client) Stop() {
	c.logger.Info("closing Redis subscriber and waiting for all workers to finish")
	err := c.subscriber.Close()
	if err != nil {
		c.logger.Error(err, "unable to close subscriber")
	}
	c.workerWG.Wait()
}

func (c *client) DequeueLoop(ctx context.Context) {
	c.subscriber = c.client.Subscribe(ctx, c.listName)
	go c.dequeueLoop(ctx)
}

func (c *client) dequeueLoop(ctx context.Context) {
	for msg := range c.subscriber.Channel() {
		jobs := make([]*job, 1)
		err := json.Unmarshal([]byte(msg.Payload), &jobs)
		if err != nil {
			c.logger.Error(err, "unable to unmarshal payload, skipping", "payload", msg.Payload)
		}
		go c.processJobs(ctx, jobs)
	}
}

func (c *client) processJobs(ctx context.Context, jobs []*job) {
	c.workerWG.Add(1)
	defer c.workerWG.Done()

	for _, job := range jobs {
		c.logger.Info("dequeued job", "type", job.Type())
		if h, ok := c.handlers[job.Type()]; ok {
			if err := h(ctx, job); err != nil {
				c.logger.Error(err, "job handler returned an error")
			}
		}
	}
}
