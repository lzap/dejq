Very Simple Job Queue
=====================

THIS REPOSITORY IS IN MAINTAINANCE MODE. EXPECT NO UPDATES.

An abstract task queue API with three implementations:

* Postgres (via jobqueue library)
* AWS SQS (via FIFO queue)
* Redis (via list, no heartbeat/expiration)
* In-memory (synchronous queue for development or testing)

_Pronounced: "dejk"_

The goal of this project is to create a simple Go API to decouple a SQL-based job queue with the following features:

* Job scheduling.
* Dependencies.
* Heartbeat.

To keep things simple, job has a type (string, think of it as a queue name) and a payload (JSON struct, the body). There is no result and no data sharing. If jobs need to share any data, this must be done in a different way (e.g. database).

To receive a job, register a handler function to a type. When the function returns no error, implementations may try to redeliver it few times (for example SQS), but generally all dependant jobs will be executed too. This means that the application is responsible for tracking error state and optionally skipping dependant jobs if it needs to.

Multiple jobs can be enqueued in a single Enqueue call all implementations will ensure in-order processing. No other workers are guaranteed to receive jobs with unfinished dependencies.

Logging is done via [logr](https://github.com/go-logr/logr) abstract structured interface, there are adapters for most logging libraries available. This adapter only provides `error` and `info` logging functions with verbosity levels (0 to N). See details blow on how to set verbosity levels.

Make sure to call `Stop()` in the end of the `main` function in order to let all jobs to finish during graceful shutdown. The implementation immediately cancels all listening workers and waits indefinitely until all already started jobs and hearbeat goroutines are finished. Some implementations (SQS) receives jobs in batches, so make sure there is enough shutdown time for workers in order to process all messages in a batch which is 10 jobs by default.

HOW TO USE
----------

```go
package main

import (
	"context"
	stdlog "log"
	"os"

	"github.com/go-logr/stdr"
	"github.com/lzap/dejq"
	"github.com/lzap/dejq/mem"
)

type TestJob struct {
	SomeString string `json:"str"`
}

func main() {
	ctx := context.Background()
	log := stdr.NewWithOptions(stdlog.New(os.Stderr, "", stdlog.LstdFlags), stdr.Options{LogCaller: stdr.None})
	
	// in-memory client (has no options)
	jobs, _ := mem.NewClient(ctx, log)

	// register handlers
	jobs.RegisterHandler("test_job", func(ctx context.Context, job dejq.Job) error {
		var data TestJob
		job.Decode(&data)
		// process data
		return nil
	})

	// start dequeue loop as separate goroutine(s)
	jobs.DequeueLoop(ctx)

	// send a job
	j1 := dejq.PendingJob {
		Type: "test_job",
		Body: &TestJob{SomeString: "First job"},
	}
	jobs.Enqueue(ctx, j1)

	// send two dependent jobs
	j2 := dejq.PendingJob {
		Type: "test_job",
		Body: &TestJob{SomeString: "Dependant job"},
	}
	jobs.Enqueue(ctx, j1, j2)
	
	// graceful shutdown
	jobs.Stop()
}
```

See the API [interface](interface.go) and the example in [cmd](cmd) package for more details.

Postgres implementation
-----------------------

Based on the [jobqueue](https://github.com/osbuild/osbuild-composer/tree/main/pkg/jobqueue) Postgres implementation which takes advantage of some specific features of the database platform, namely `LISTEN/NOTIFY` and `SKIP LOCKED` to eliminate unnecessary polling and locking. Apparently, this implementation needs a connection to a database with few tables created, see migration files in the repository.

Arbitrary amount of goroutines can be configured to perform processing of jobs, the amount depends on the use case but typically should be around number of CPUs (cores) or more depending on I/O heavy workloads. For each job, another goroutine is spawned which updates heartbeat timestamp in configurable interval (until maximum amount of beats is reached). This approach works for short and even very long jobs 

Finished jobs are *left in the database forever*, explicit maintenance must be done regularly. See the package API for functions for database cleanup (delete finished jobs, find cancelled jobs, vacuum database). There are no command line tools for that, you need to write your own cleanup procedure (cronjob/task). When worker process dies for any reason and some jobs are left unprocessed, heartbeat stops updating. The cleanup procedure is responsible for either removing or requeue unresponsive jobs via `FinishJob` or `RequeueJob`.

The jobqueue implementation supports additional features (channels, results) which are unused by dejq library because SQS implementation does not support that. The goal of this library is to have a simple and common API.

Client configuration arguments:

* ctx - Go standard library context
* logger - `logr` facade for logging
* db - database/sql connection pool for the jobs database (migration must be done separately)
* workers - amount of workers to spawn (typically number of CPUs/cores or higher)
* heartbeat - duration of the heartbeat update (depends on the workload and your cleanup procedure)
* maxBeats - maximum amount of heartbeats until the job is considered stuck and cancelled

Every function handler is automatically assigned an extra goroutine responsible for updating heartbeat on the background until the handler returns. There is no limit on how long a job can be processed.

Logging verbosity levels:

* 0 - only errors (the default level)
* 1 - enqueue and dequeue operations with job id, type and body
* 2 - additional information from worker goroutines
* 3 - heartbeat information

*Error handling*: When a handler returns `nil`, job is marked as `failed` and no redelivery is performed. Dependant jobs will be executed. Manual redelivery of failed jobs is possible.

SQS implementation
------------------

This implementation is designed for AWS FIFO SQS queues, an attempt to use standard queue will result in an error. Exactly once delivery and first-in first-out capabilities of FIFO queues are needed for the dependencies feature (multiple jobs enqueued in a single operation).

A fixed amount of worker goroutines is spawned and they all perform long polling of messages in batches of 10 messages, then they dispatch the jobs to handler functions. This implementation ensures the order in batches preserving the dependencies.

Every function handler is automatically assigned an extra goroutine responsible for updating heartbeat (also known as visibility timeout) on the background until the handler returns. There is maximum time of 12 hours for a job which is limitation of the AWS SQS visibility timeout.

The SQS implementation is not production-ready, it needs more testing as it was written as a "fallback" implementation if we find the Postgres approach not suitable from the operational perspective.

*Error handling*: When a handler returns `nil`, job is redelivered multiple times (configurable in SQS) and if it all fails the job is discarded (or put to dead letter queue depends on configuration). Dependant jobs will be executed.

Redis implementation
--------------------

Very simple implementation using Redis linked list. Job dependencies are concatenated into list, so it is guaranteed that the same worker receives all of them. There is no heartbeat, dead-letter queue or resubmitting of failed jobs. This implementation is meant for temporary solution, to test the interface and upgrade to Postgres or SQS later on.

*Error handling*: When a handler returns `nil`, an error is sent to logger and the job is discarded. Dependant jobs will be executed.

In-Memory implementation
------------------------

It's a synchronous implementation where a single background goroutine is handing the tasks and enqueue operation blocks until the work is done. Also there is not error handling code, when a handler returns an error the library calls `panic` to draw immediate attention. At this point it should be pretty obvious, but to be sure: do not use this in production!

*Error handling*: When a handler returns `nil`, an error is sent to the logger and the job is discarded. Dependant jobs will be executed.

Running the example
-------------------

A small example is provided, to run memory implementation:

        go run cmd/example.main.go mem

Redis implementation:

        REDIS_HOST=localhost go run cmd/example.main.go redis

AWS SQS implementation:

        QUEUE_NAME=dejq.fifo go run cmd/example.main.go sqs

Postgres implementation:

        DB_HOST=localhost DB_NAME=dejq go run cmd/example.main.go pg

Planned features
----------------

* Make SQS channel configurable via New function
* Export statistics for Redis implementation (total number of jobs, messages, duration)
* Enqueue later (delayed job)

AUTHORS
-------

* Lukas Zapletal (@lzap)

LICENSE
-------

Copyright (c) 2022 Red Hat, licensed under MIT license

CREDITS
-------

* Inspired by gosqs: https://github.com/qhenkart/gosqs (MIT license)
* Postgres jobqueue is from: https://github.com/osbuild/osbuild-composer (Apache 2.0 license) 
