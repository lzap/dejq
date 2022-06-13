package main

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/lzap/dejq"
	"github.com/lzap/dejq/log"
	"github.com/lzap/dejq/log/stdoutadapter"
)

type TestJob struct {
	SomeString string `json:"a_string"`
}

func main() {
	logger := stdoutadapter.NewLogger()

	// use AWS_PROFILE=saml env variable to use a different AWS config profile
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(err)
	}

	logger.Log(context.TODO(), log.LogLevelTrace, "starting consumer", nil)
	c, err := dejq.NewConsumer(context.TODO(), cfg, "lzap-jobs-dev.fifo", logger, 30, 3)
	if err != nil {
		panic(err)
	}
	c.RegisterHandler("test_job", func(ctx context.Context, job dejq.Job) error {
		println("Received a job! Type: ", job.Type())
		return nil
	})

	logger.Log(context.TODO(), log.LogLevelTrace, "starting publisher", nil)
	p, err := dejq.NewPublisher(context.TODO(), cfg, "lzap-jobs-dev.fifo", logger)
	if err != nil {
		panic(err)
	}

	logger.Log(context.TODO(), log.LogLevelTrace, "enqueuing job", nil)
	err = p.Enqueue(context.TODO(), "test_job", &TestJob{SomeString: "test"})
	if err != nil {
		panic(err)
	}

	c.Consume(context.TODO())

	// job sending can be implemented as goroutines, wait until all is sent
	p.Wait()
}
