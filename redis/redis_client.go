package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

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
	logger    logr.Logger
	handlers  map[string]dejq.Handler
	client    *redis.Client
	queueName string
	workerWG  sync.WaitGroup
	closeCh   chan interface{}
}

func NewClient(_ context.Context, logger logr.Logger, address, username, password string, db int, queueName string) (*client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     address,
		Username: username,
		Password: password,
		DB:       db,
	})
	return &client{
		logger:    logger,
		handlers:  make(map[string]dejq.Handler),
		client:    rdb,
		queueName: queueName,
		closeCh:   make(chan interface{}),
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

	cmd := c.client.LPush(ctx, c.queueName, buffer)
	if cmd.Err() != nil {
		c.logger.Error(cmd.Err(), "unable to push job", "payload", string(buffer))
		return cmd.Err()
	}

	c.logger.Info("enqueued job", "payload", string(buffer), "result", cmd.String())
	return nil
}

func (c *client) Stop() {
	close(c.closeCh)
	c.logger.Info("waiting for all workers to finish")
	c.workerWG.Wait()
}

func (c *client) DequeueLoop(ctx context.Context) {
	go c.dequeueLoop(ctx)
}

func (c *client) dequeueLoop(ctx context.Context) {
	for {
		select {
		case <-c.closeCh:
			c.logger.Info("shutting down consumer (stop)...")
			return
		case <-ctx.Done():
			c.logger.Info("shutting down consumer (cancel)...")
			return
		default:
			res, err := c.client.BLPop(ctx, 10*time.Second, c.queueName).Result()
			if err != nil && err.Error() != "redis: nil" {
				c.logger.Error(err, "error consuming from redis queue")
			} else if errors.Is(err, redis.Nil) {
				// no tasks to consume (time out)
			} else {
				jobs := make([]*job, 1)
				err = json.Unmarshal([]byte(res[1]), &jobs)
				if err != nil {
					c.logger.Error(err, "unable to unmarshal payload, skipping", "payload", res[1])
				}
				go c.processJobs(ctx, jobs)
			}
		}
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

func (c *client) Stats(ctx context.Context) (dejq.Stats, error) {
	count, err := c.client.LLen(ctx, c.queueName).Result()
	if err != nil {
		return dejq.Stats{}, fmt.Errorf("unable to get queue len: %w", err)
	}

	return dejq.Stats{
		EnqueuedJobs: uint64(count),
	}, nil
}
