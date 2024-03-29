package dejq

import "context"

// PendingJob represents a new job. Received jobs use a different interface named Job.
type PendingJob struct {
	// Type represents payload type. It is recommended to use "v1" suffix/prefix to differentiate
	// incompatible payload versions.
	Type string `json:"type"`

	// Body must be a struct that can be marshalled to valid JSON.
	Body interface{} `json:"body"`
}

// Handler is called from worker pool when consuming a job. A job is not removed from the queue
// until the handler returns no error (nil).
//
// The context passed via DequeueLoop is available in all handler functions.
//
// When an error is returned, job might be scheduled again even multiple times until it is considered as a failure.
// This behavior is different depending on an implementation.
type Handler func(context.Context, Job) error

// Job represents a job task returned from a tasking system.
type Job interface {
	// Type returns the job type
	Type() string

	// Decode must be used to unmarshall body to a particular struct
	Decode(out interface{}) error

	// Id returns some kind of unique id (UUID typically)
	Id() string
}

// Jobs provides an interface for creating new jobs.
type Jobs interface {
	// Enqueue sends pending jobs to the queue. Multiple jobs passed in a slice are guaranteed
	// to be delivered in-order preserving dependencies between jobs. A dependent job can be
	// dequeued only if all dependencies were already dequeued and confirmed (marked as done).
	//
	// Dependant jobs do not share and data, use application database for sharing data across
	// jobs.
	//
	// Returns a unique strings, typically UUIDs, of enqueued jobs.
	//
	// Example: for jobs {a, b} the "b" only starts after "a" is finished.
	//
	// It is not possible to cancel existing job, if job "b" must be skipped for any reason, then
	// job "a" must set some flag in the application database to skip "b".
	Enqueue(ctx context.Context, jobs ...PendingJob) ([]string, error)

	// RegisterHandler registers an event listener for a particular type with an associated handler.
	RegisterHandler(name string, h Handler)

	// DequeueLoop polls for new messages and if it finds one it sends the message to a background handler.
	// When handler exits without error, the message is deleted from the queue. All handlers must be
	// registered via RegisterHandler before the DequeueLoop is called.
	DequeueLoop(ctx context.Context)

	// Stop let's background workers to finish all jobs and terminates them. It is blocking until all messages
	// are finished sending or consuming.
	Stop()

	// Stats returns statistics. Not all implementations supports stats, some may return zero values.
	Stats(ctx context.Context) (Stats, error)
}

// Stats provides monitoring statistics.
type Stats struct {
	EnqueuedJobs uint64
}
