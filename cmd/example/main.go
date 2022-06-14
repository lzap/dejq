package main

import (
	"context"
	stdlog "log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/go-logr/stdr"
	"github.com/lzap/dejq"
)

type TestJob struct {
	SomeString string `json:"a_string"`
}

func main() {
	ctx := context.Background()
	stdr.SetVerbosity(9)
	log := stdr.NewWithOptions(stdlog.New(os.Stderr, "", stdlog.LstdFlags), stdr.Options{LogCaller: stdr.None})

	// use AWS_PROFILE=saml env variable to use a different AWS config profile
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(err)
	}

	c, err := dejq.NewConsumer(ctx, cfg, "lzap-jobs-dev.fifo", log, 15, 3)
	if err != nil {
		panic(err)
	}
	c.RegisterHandler("test_job", func(ctx context.Context, job dejq.Job) error {
		log.Info("Received a job!", "type", job.Type(), "dedup_id", job.Attribute("dedup_id"))
		time.Sleep(1 * time.Minute)
		return nil
	})

	p, err := dejq.NewPublisher(ctx, cfg, "lzap-jobs-dev.fifo", log)
	if err != nil {
		panic(err)
	}

	err = p.Enqueue(ctx, "test_job", &TestJob{SomeString: "test"})
	if err != nil {
		panic(err)
	}

	go c.Dequeue(ctx)
	time.Sleep(40 * time.Second)

	// job sending can be implemented as goroutines, wait until all is sent
	p.Wait()
}
