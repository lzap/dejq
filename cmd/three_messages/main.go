package main

import (
	"context"
	"database/sql"
	"fmt"
	stdlog "log"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/go-logr/stdr"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/lzap/dejq"
	"github.com/lzap/dejq/mem"
	"github.com/lzap/dejq/postgres"
	"github.com/lzap/dejq/sqs"
)

type TestJob struct {
	SomeString string `json:"str"`
}

func main() {
	messages := 3
	wg := sync.WaitGroup{}
	wg.Add(messages)
	ctx := context.Background()
	stdr.SetVerbosity(3)
	log := stdr.NewWithOptions(stdlog.New(os.Stderr, "", stdlog.LstdFlags), stdr.Options{LogCaller: stdr.None})
	var jobs dejq.Jobs

	if os.Args[1] == "sqs" {
		// use AWS_PROFILE=saml env variable to use a different AWS config profile
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			panic(err)
		}

		jobs, err = sqs.NewClient(ctx, cfg, log, 3, 2, 5*time.Second, 3)
		if err != nil {
			panic(err)
		}
	} else if os.Args[1] == "pg" {
		// register and setup logging configuration
		connConfig, err := pgx.ParseConfig("postgres://nuc:5432/dejq_dev")
		if err != nil {
			panic(nil)
		}
		connStrRegistered := stdlib.RegisterConnConfig(connConfig)

		db, err := sql.Open("pgx", connStrRegistered)
		if err != nil {
			panic(nil)
		}
		jobs, err = postgres.NewClient(ctx, log, db, 2, 5*time.Second, 3)
		if err != nil {
			panic(err)
		}
	} else if os.Args[1] == "mem" {
		jobs, _ = mem.NewClient(ctx, log)
	}

	jobs.RegisterHandler("test_job", func(ctx context.Context, job dejq.Job) error {
		var data TestJob
		_ = job.Decode(&data)
		msg := fmt.Sprintf("received job: %s", data.SomeString)
		log.Info(msg, "type", job.Type())
		wg.Done()
		return nil
	})

	// start consuming messages
	jobs.DequeueLoop(ctx)

	pendingJobs := make([]dejq.PendingJob, 0, messages)
	for i := 1; i <= messages; i++ {
		j := dejq.PendingJob{
			Type: "test_job",
			Body: &TestJob{SomeString: fmt.Sprintf("A message number %d", i)},
		}
		pendingJobs = append(pendingJobs, j)
	}
	log.Info("Sending messages", "number", messages)
	err := jobs.Enqueue(ctx, pendingJobs...)
	if err != nil {
		panic(err)
	}

	// wait until all messages are consumed
	wg.Wait()

	// stop publishing goroutines and wait until all messages are sent or processed
	jobs.Stop()
}
