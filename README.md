Dead Simple Job Queue with grouping
===================================

Another task queue with three implementations:

* Postgres (via jobqueue library)
* AWS SQS (FIFO queue)
* In-memory (synchronous queue for development or testing)

The goal of this project is to create a simple Go API with the following features:

* Job scheduling.
* Dependencies.
* Heartbeat.

To keep things simple, job has a type (string) and a payload (JSON struct).

To receive a job, register a handler function to a type. When the function returns no error, only then the job is removed from the queue. When error is returned, implementations may try to re-deliver the job multiple times until they mark is as invalid (SQS puts the message to the dead-letter queue).

Multiple jobs can be enqueued as a slice and both Postgres and SQS implementations will ensure in-order processing.

Every handler has an extra goroutine responsible for updating heartbeat on the background until the handler is finished. This is fully transparent, and it allows long-running tasks up to 12 hours (SQS) or for unlimited time (Postgres).

HOW TO USE
----------

See the API [interface](interface.go) and the example in [cmd](cmd) package.

WARNING
-------

The SQS implementation is not production-ready, it needs more work and unit tests.

AUTHORS
-------

* Lukas Zapletal (@lzap)

LICENSE
-------

MIT

CREDITS
-------

* heavily inspired by gosqs: https://github.com/qhenkart/gosqs (MIT license)
* Postgres jobqueue is from: https://github.com/osbuild/image-builder (Apache 2.0 license) 
