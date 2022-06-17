package dejq

import "context"

// PendingJob represents a new job. Received jobs use a different interface named Job.
type PendingJob struct {
	// Type represents payload type. It is recommended to use _v1 suffix to differentiate
	// incompatible payload versions.
	Type string

	// Body must be a struct that can be marshalled to valid JSON.
	Body interface{}
}

// Enqueuer provides an interface for creating new jobs.
type Enqueuer interface {
	// Enqueue sends pending jobs to the queue. Multiple jobs passed in a slice are guaranteed
	// to be delivered in-order preserving dependencies between jobs. A dependent job can be
	// dequeued only if all dependencies were already dequeued and confirmed (marked as done).
	Enqueue(ctx context.Context, job ...PendingJob) error
}

// Job represents a job task returned from a tasking system.
type Job interface {
	// Type returns the job type
	Type() string

	// Decode must be used to unmarshall body to a particular struct
	Decode(out interface{}) error

	// Attribute returns an implementation-specific header
	attribute(key string) string
}

// Handler provides a standardized handler method, this is the required function composition for event handlers
type Handler func(context.Context, Job) error

// Dequeuer provides an interface for receiving jobs.
type Dequeuer interface {
	// DequeueLoop polls for new messages and if it finds one it sends the message to a background handler.
	// When handler exits without error, the message is deleted from the queue.
	DequeueLoop(ctx context.Context)

	// RegisterHandler registers an event listener for a particular type with an associated handler.
	RegisterHandler(name string, h Handler)

	// Stop let's background workers to finish all jobs and terminates them. It is blocking until all messages
	// are finished sending or consuming.
	Stop()
}
