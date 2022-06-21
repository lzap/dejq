package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"github.com/lzap/dejq"
	dbjobqueue "github.com/lzap/dejq/postgres/jobqueue"
)

type dbJob struct {
	ID, Token    uuid.UUID
	Deps         []uuid.UUID
	JobType      string
	Args         json.RawMessage
	errorChannel chan error
}

func newJob(jid, jtoken uuid.UUID, jdeps []uuid.UUID, jtype string, jargs json.RawMessage) *dbJob {
	return &dbJob{
		ID:      jid,
		Token:   jtoken,
		Deps:    jdeps,
		JobType: jtype,
		Args:    jargs,
	}
}

func (j *dbJob) Type() string {
	return j.JobType
}

func (j *dbJob) Decode(out interface{}) error {
	return json.Unmarshal(j.Args, &out)
}

// ErrorResponse is used to determine for error handling within the handler. When an error occurs,
// this function should be returned.
func (j *dbJob) ErrorResponse(_ context.Context, err error) error {
	go func() {
		j.errorChannel <- err
	}()
	return err
}

// Success is used to determine that a handler was successful in processing the job and the job should
// now be consumed. This will delete the job from the queue
func (j *dbJob) Success(_ context.Context) {
	go func() {
		j.errorChannel <- nil
	}()
}

type client struct {
	jq         *dbjobqueue.DBJobQueue
	logger     logr.Logger
	handlers   map[string]dejq.Handler
	workerPool int
	pollerWG   sync.WaitGroup
	heartbeat  time.Duration
	maxBeats   int
	cancelFn   context.CancelFunc
}

func NewClient(_ context.Context, logger logr.Logger, db *sql.DB, workers int, heartbeat time.Duration, mxBeats int) (*client, error) {
	var err error
	c := client{
		logger:     logger,
		handlers:   make(map[string]dejq.Handler),
		workerPool: workers,
		heartbeat:  heartbeat,
		maxBeats:   mxBeats,
	}
	c.jq, err = dbjobqueue.NewFromStdlib(db)
	if err != nil {
		return nil, dejq.ErrCreateClient.Context(err)
	}
	return &c, nil
}

func (c *client) RegisterHandler(name string, h dejq.Handler) {
	c.handlers[name] = h
}

func (c *client) Enqueue(_ context.Context, jobs ...dejq.PendingJob) error {
	deps := make([]uuid.UUID, 0, len(jobs))
	for _, job := range jobs {
		dbJob, err := c.jq.Enqueue(job.Type, job.Body, deps, "")
		if err != nil {
			return dejq.ErrEnqueueJob.Context(err)
		}
		c.logger.V(1).Info("enqueued job", "type", job.Type, "uuid", dbJob.String())
		deps = append(deps, dbJob)
	}
	return nil
}

func (c *client) Stop() {
	c.logger.V(1).Info("cancelling dequeuers")
	// cancel all dequeue calls
	if c.cancelFn != nil {
		c.cancelFn()
	}
}

func (c *client) DequeueLoop(ctx context.Context) {
	ctx, fn := context.WithCancel(ctx)
	c.cancelFn = fn
	types := make([]string, 0, len(c.handlers))
	for k := range c.handlers {
		types = append(types, k)
	}
	c.pollerWG.Add(c.workerPool)
	for w := 1; w <= c.workerPool; w++ {
		go c.worker(ctx, w, types)
	}
}

var channels = []string{""}

func (c *client) worker(ctx context.Context, id int, types []string) {
	for {
		jid, jtoken, jdeps, jtype, jargs, err := c.jq.Dequeue(ctx, types, channels)
		if err != nil {
			c.logger.Error(err, "error while dequeued trying again in 30 secs", "worker_id", id)
			time.Sleep(30 * time.Second)
			continue
		}
		c.logger.V(1).Info("dequeued job",
			"id", jid.String(),
			"token", jtoken.String(),
			"deps", fmt.Sprintf("%+v", jdeps),
			"args", fmt.Sprintf("%s", jargs),
			"type", jtype)
		job := newJob(jid, jtoken, jdeps, jtype, jargs)
		if err := c.run(ctx, job); err != nil {
			c.logger.Error(err, "error processing job", "uuid", jid.String(), "worker_id", id)
		}
	}
}

func (c *client) run(ctx context.Context, job *dbJob) error {
	c.logger.V(2).Info("processing job", "uuid", job.ID.String())
	if h, ok := c.handlers[job.Type()]; ok {
		go c.extend(ctx, job)
		if err := h(ctx, job); err != nil {
			return job.ErrorResponse(ctx, err)
		}
		job.Success(ctx)
	}
	c.logger.V(2).Info("finishing job", "uuid", job.ID.String())
	return c.delete(ctx, job)
}

type noResult struct{}

func (c *client) delete(_ context.Context, job *dbJob) error {
	err := c.jq.FinishJob(job.ID, noResult{})
	if err != nil {
		return dejq.ErrUnableToDelete.Context(err)
	}
	return nil
}

func (c *client) extend(ctx context.Context, job *dbJob) {
	timer := time.NewTimer(c.heartbeat)
	count := 0
	for {
		if count >= c.maxBeats {
			c.logger.Error(nil, "exceeded maximum amount of heartbeats", "uuid", job.ID.String())
			return
		}
		count++

		select {
		case <-ctx.Done():
			// Stop was called, but we want to let all the jobs to finish
			c.logger.Info("workers are closing, heartbeat still active", "uuid", job.ID.String())
		case <-job.errorChannel:
			c.logger.V(2).Info("heartbeat done", "uuid", job.ID.String())
			timer.Stop()
			return
		case <-timer.C:
			c.logger.V(2).Info("heartbeat", "uuid", job.ID.String())
			c.jq.RefreshHeartbeat(job.Token)
			timer.Reset(c.heartbeat)
		}
	}
}
