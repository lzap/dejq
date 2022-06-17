package main

import (
	"context"
	"fmt"
	stdlog "log"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/go-logr/stdr"
	"github.com/lzap/dejq"
)

type TestJob struct {
	SomeString string `json:"a_string"`
}

func main() {
	messages := 2
	wg := sync.WaitGroup{}
	wg.Add(messages)
	ctx := context.Background()
	stdr.SetVerbosity(3)
	log := stdr.NewWithOptions(stdlog.New(os.Stderr, "", stdlog.LstdFlags), stdr.Options{LogCaller: stdr.None})

	// use AWS_PROFILE=saml env variable to use a different AWS config profile
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(err)
	}

	consumeClient, err := dejq.NewConsumer(ctx, cfg, "lzap-jobs-dev.fifo", log, 15, 3)
	if err != nil {
		panic(err)
	}
	consumeClient.RegisterHandler("test_job", func(ctx context.Context, job dejq.Job) error {
		var data TestJob
		job.Decode(&data)
		msg := fmt.Sprintf("Received a job: %s", data.SomeString)
		log.Info(msg, "type", job.Type(), "dedup_id", job.Attribute("dedup_id"))
		wg.Done()
		return nil
	})

	publishClient, err := dejq.NewPublisher(ctx, cfg, "lzap-jobs-dev.fifo", log)
	if err != nil {
		panic(err)
	}

	for i := 1; i <= messages; i++ {
		log.Info("Sending message", "number", i)
		err = publishClient.Enqueue(ctx, "test_job", &TestJob{SomeString: fmt.Sprintf("A message number %d", i)})
		if err != nil {
			panic(err)
		}
	}

	// start consuming messages
	go consumeClient.Dequeue(ctx)

	// wait until all messages are consumed
	wg.Wait()

	// stop and wait until all messages are sent (should not happen at this point)
	publishClient.Stop()
	consumeClient.Stop()

	//time.Sleep(10 * time.Second)
}
