package main

import (
	"context"
	"fmt"
	stdlog "log"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/go-logr/stdr"
	"github.com/lzap/dejq"
	"github.com/lzap/dejq/mem"
	"github.com/lzap/dejq/sqs"
)

type TestJob struct {
	SomeString string `json:"a_string"`
}

func main() {
	messages := 3
	wg := sync.WaitGroup{}
	wg.Add(messages)
	ctx := context.Background()
	stdr.SetVerbosity(3)
	log := stdr.NewWithOptions(stdlog.New(os.Stderr, "", stdlog.LstdFlags), stdr.Options{LogCaller: stdr.None})
	var publisher dejq.Enqueuer
	var consumer dejq.Dequeuer

	if os.Args[1] == "sqs" {
		// use AWS_PROFILE=saml env variable to use a different AWS config profile
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			panic(err)
		}

		consumer, err = sqs.NewConsumer(ctx, cfg, log, 3, 2, 15*time.Second, 3)
		if err != nil {
			panic(err)
		}
		publisher, err = sqs.NewPublisher(ctx, cfg, log)
		if err != nil {
			panic(err)
		}
	} else if os.Args[1] == "mem" {
		consumer, _ = mem.NewClient(ctx, log)
		publisher = consumer.(dejq.Enqueuer)
	}

	consumer.RegisterHandler("test_job", func(ctx context.Context, job dejq.Job) error {
		var data TestJob
		_ = job.Decode(&data)
		msg := fmt.Sprintf("Received a job: %s", data.SomeString)
		log.Info(msg, "type", job.Type())
		wg.Done()
		return nil
	})

	// start consuming messages
	go consumer.DequeueLoop(ctx)

	jobs := make([]dejq.PendingJob, 0, messages)
	for i := 1; i <= messages; i++ {
		j := dejq.PendingJob{
			Type: "test_job",
			Body: &TestJob{SomeString: fmt.Sprintf("A message number %d", i)},
		}
		jobs = append(jobs, j)
	}
	log.Info("Sending messages", "number", messages)
	err := publisher.Enqueue(ctx, jobs...)
	if err != nil {
		panic(err)
	}

	// wait until all messages are consumed
	wg.Wait()

	// stop publishing goroutines and wait until all messages are sent
	publisher.Stop()
	// stop consuming goroutines and wait until all messages are processed
	consumer.Stop()
}
