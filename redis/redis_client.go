package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/lzap/dejq"
)

type job struct {
	UUID        string `json:"i"`
	PayloadType string `json:"t"`
	Payload     string `json:"b"`
}

func newJob(pj *dejq.PendingJob) (*job, error) {
	buffer, err := json.Marshal(pj.Body)
	if err != nil {
		return nil, err
	}
	return &job{
		UUID:        uuid.NewString(),
		PayloadType: pj.Type,
		Payload:     string(buffer),
	}, nil
}

func (j *job) Id() string {
	return j.UUID
}

func (j *job) Type() string {
	return j.PayloadType
}

func (j *job) Decode(out interface{}) error {
	return json.Unmarshal([]byte(j.Payload), &out)
}

type client struct {
	logger      logr.Logger
	handlers    map[string]dejq.Handler
	client      *redis.Client
	queueName   string
	readTimeout time.Duration
	readWG      sync.WaitGroup
	workerWG    sync.WaitGroup
	closeCh     chan interface{}
}

func NewClient(_ context.Context, logger logr.Logger, address, username, password string, db int, queueName string, readTimeout time.Duration) (*client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     address,
		Username: username,
		Password: password,
		DB:       db,
	})
	return &client{
		logger:      logger,
		handlers:    make(map[string]dejq.Handler),
		client:      rdb,
		queueName:   queueName,
		readTimeout: readTimeout,
		closeCh:     make(chan interface{}),
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

func (c *client) Enqueue(ctx context.Context, jobs ...dejq.PendingJob) ([]string, error) {
	ids := make([]string, len(jobs))
	redisJobs, err := c.marshalJobs(jobs)
	if err != nil {
		c.logger.Error(err, "unable to marshal job data")
		return ids, err
	}
	for i, j := range redisJobs {
		str := fmt.Sprintf("%+v", j)
		c.logger.Info("enqueueing pending job", "payload", str, "job_id", j.UUID)
		ids[i] = j.UUID
	}

	buffer, err := json.Marshal(redisJobs)
	if err != nil {
		return ids, err
	}

	cmd := c.client.LPush(ctx, c.queueName, buffer)
	if cmd.Err() != nil {
		c.logger.Error(cmd.Err(), "unable to push job", "payload", string(buffer))
		return ids, cmd.Err()
	}

	c.logger.Info("enqueued job", "payload", string(buffer), "result", cmd.String())
	return ids, nil
}

func (c *client) Stop() {
	close(c.closeCh)
	c.logger.Info("waiting for all workers to finish")
	c.readWG.Wait()
	c.workerWG.Wait()
}

func (c *client) DequeueLoop(ctx context.Context) {
	// randomize start of dequeue loop (random delay up to one read timeout)
	randMs := rand.Int63n(c.readTimeout.Milliseconds())
	c.logger.Info("start sleep (ms): " + strconv.FormatInt(randMs, 10))
	time.Sleep(time.Duration(randMs) * time.Millisecond)
	go c.dequeueLoop(ctx)
}

func (c *client) dequeueLoop(ctx context.Context) {
	timeouts := 0
	for {
		select {
		case <-c.closeCh:
			c.logger.Info("shutting down consumer (stop)...")
			return
		case <-ctx.Done():
			c.logger.Info("shutting down consumer (cancel)...")
			return
		default:
			c.readWG.Add(1)
			res, err := c.client.BLPop(ctx, c.readTimeout, c.queueName).Result()
			c.readWG.Done()
			if errors.Is(err, redis.Nil) {
				// no tasks to consume (time out)
				timeouts += 1

				// sporadically print stats into logs
				if timeouts%30 == 0 {
					stats, _ := c.Stats(ctx)
					str := fmt.Sprintf("redis queue length: %d", stats.EnqueuedJobs)
					c.logger.Info(str, stats.EnqueuedJobs)
				}
			} else if err != nil {
				c.logger.Error(err, "error consuming from redis queue")
			} else {
				jobs := make([]*job, 1)
				err = json.Unmarshal([]byte(res[1]), &jobs)
				if err != nil {
					c.logger.Error(err, "unable to unmarshal payload, skipping", "payload", res[1])
				}
				go c.processJobs(ctx, jobs, timeouts)
				timeouts = 0
			}
		}
	}
}

func (c *client) processJobs(ctx context.Context, jobs []*job, timeouts int) {
	c.workerWG.Add(1)
	defer c.workerWG.Done()

	for _, job := range jobs {
		c.logger.Info("dequeued job", "type", job.Type(), "job_id", job.Id(), "timeouts", timeouts)
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
