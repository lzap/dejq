package sqs

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/go-logr/logr"
	"github.com/lzap/dejq"
)

const queueName = "lzap-jobs-dev.fifo"
const maxRetryCount = 5
const maxMessages = int32(10)

var errDataLimit = errors.New("InvalidParameterValue: One or more parameters are invalid. Reason: Message must be shorter than 262144 bytes")

type client struct {
	sqs      *sqs.Client
	queueURL string
	logger   logr.Logger
	senderWG sync.WaitGroup
	workerWG sync.WaitGroup
	pollerWG sync.WaitGroup
	stopFlag atomic.Value

	handlers             map[string]dejq.Handler
	visibilityTimeoutSec int
	maxExtensions        int
	workerPool           int
}

func NewPublisher(ctx context.Context, config aws.Config, logger logr.Logger) (*client, error) {
	pub := &client{
		sqs:    sqs.NewFromConfig(config),
		logger: logger,
	}

	if config.Logger == nil {
		config.Logger = NewAWSLogrAdapter(logger)
	}

	err := pub.getQueueUrl(ctx, queueName)
	if err != nil {
		return nil, dejq.ErrCreateClient.Context(err)
	}

	return pub, nil
}

func NewConsumer(ctx context.Context, config aws.Config, logger logr.Logger, visibilityTimeoutSec, maxExtend int) (*client, error) {
	client, err := NewPublisher(ctx, config, logger)
	if err != nil {
		return nil, dejq.ErrCreateClient.Context(err)
	}
	// TODO: VisibilityTimeout can be retrieved from queue dynamically (error thrown when < 10 sec)
	if visibilityTimeoutSec <= 10 {
		return nil, errors.New("heartbeat cannot be shorter than 10 seconds")
	}
	client.visibilityTimeoutSec = visibilityTimeoutSec
	client.maxExtensions = maxExtend
	client.workerPool = 3
	client.handlers = make(map[string]dejq.Handler)
	client.stopFlag.Store(false)
	return client, nil
}

// RegisterHandler registers an event listener and an associated handler. If the event matches, the handler will
// be run along with any included middleware
func (c *client) RegisterHandler(name string, h dejq.Handler) {
	c.handlers[name] = h
}

func (c *client) getQueueUrl(ctx context.Context, queueName string) error {
	input := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}
	result, err := c.sqs.GetQueueUrl(ctx, input)
	if err != nil {
		return err
	}
	c.queueURL = *result.QueueUrl
	return nil
}

func randomBase62(bytes int) string {
	b := make([]byte, bytes)
	_, err := rand.Read(b)
	if err != nil {
		panic("unable to read pseudorandom numbers")
	}
	n := new(big.Int)
	n.SetBytes(b)
	return n.Text(62)
}

func generateRandomString() string {
	return randomBase62(20)[0:22]
}

func (c *client) Enqueue(ctx context.Context, jobs ...dejq.PendingJob) error {
	var entries = make([]types.SendMessageBatchRequestEntry, 0, len(jobs))
	groupId := generateRandomString()
	for _, job := range jobs {
		bytes, err := json.Marshal(job.Body)
		if err != nil {
			return dejq.ErrPayloadMarshal.Context(err)
		}

		deduplicationId := generateRandomString()
		entries = append(entries, types.SendMessageBatchRequestEntry{
			Id:                     aws.String(deduplicationId),
			MessageBody:            aws.String(string(bytes)),
			MessageAttributes:      defaultSQSAttributes(job.Type, int64(len(jobs))),
			MessageGroupId:         aws.String(groupId),
			MessageDeduplicationId: aws.String(deduplicationId),
		})
	}
	sqsInput := &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: aws.String(c.queueURL),
	}

	c.senderWG.Add(1)
	// TODO implement confirmed batch sending via worker goroutine just like in CloudWatch
	go c.sendDirectMessage(ctx, sqsInput)
	return nil
}

// sendDirectMessage is used to handle sending and error failures in a separate go-routine.
//
// AWS-SDK will use their own retry mechanism for a failed request utilizing exponential backoff. If they fail
// then we will wait 10 seconds before trying again.
func (c *client) sendDirectMessage(ctx context.Context, input *sqs.SendMessageBatchInput, retryCount ...int) {
	var count int
	if len(retryCount) != 0 {
		count = retryCount[0]
	}

	if count > maxRetryCount-1 {
		c.logger.Error(nil, "too many failures, giving up")
		c.senderWG.Done()
		return
	}

	if m, err := c.sqs.SendMessageBatch(ctx, input); err != nil {
		if err.Error() == errDataLimit.Error() {
			c.logger.Error(err, "payload limit overflow, giving up")
			c.senderWG.Done()
			return
		}

		c.logger.Error(err, "error publishing, trying again in 10 seconds")
		time.Sleep(10 * time.Second)
		c.sendDirectMessage(ctx, input, count+1)
	} else {
		if c.logger.Enabled() {
			for _, msg := range m.Successful {
				c.logger.V(1).Info("message successfully sent", "message_id", msg.MessageId)
			}
			for _, msg := range m.Failed {
				err := fmt.Errorf("error %s: %s", *msg.Code, *msg.Message)
				c.logger.V(1).Error(err, "message send failed")
			}
		}
		c.senderWG.Done()
	}
}

func defaultSQSAttributes(jobType string, inGroup int64) map[string]types.MessageAttributeValue {
	result := make(map[string]types.MessageAttributeValue, 2)
	result["job_type"] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: &jobType}
	result["in_group"] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(strconv.FormatInt(inGroup, 10))}
	return result
}

// Stop will block until all background goroutines are done processing. Both message sending
// and processing is done asynchronously. Call this function before main() function exits to
// no messages are lost or unprocessed.
func (c *client) Stop() {
	c.logger.V(1).Info("waiting for background goroutines")
	c.stopFlag.Store(true)
	c.pollerWG.Wait()
	c.senderWG.Wait()
	c.workerWG.Wait()
}

// DequeueLoop polls for new messages and if it finds one, decodes it, sends it to the handler and deletes it
//
// A message is not considered dequeued until it has been sucessfully processed and deleted. There is a 30 Second
// delay between receiving a single message and receiving the same message. This delay can be adjusted in the AWS
// console and can also be extended during operation. If a message is successfully received 4 times but not deleted,
// it will be considered unprocessable and sent to the DLQ automatically
//
// Dequeue uses long-polling to check and retrieve messages, if it is unable to make a connection, the aws-SDK will use its
// advanced retrying mechanism (including exponential backoff), if all of the retries fail, then we will wait 10s before
// trying again.
//
// When a new message is received, it runs in a separate go-routine that will handle the full consuming of the message, error reporting
// and deleting
func (c *client) DequeueLoop(ctx context.Context) {
	jobs := make(chan *sqsJob)
	for w := 1; w <= c.workerPool; w++ {
		go c.worker(ctx, w, jobs)
	}

	attributeNames := []string{"All"}
	for {
		input := &sqs.ReceiveMessageInput{
			QueueUrl:              &c.queueURL,
			MaxNumberOfMessages:   maxMessages,
			MessageAttributeNames: attributeNames,
			WaitTimeSeconds:       10,
		}
		c.logger.V(3).Info("receive poll", "stopFlag", c.stopFlag.Load().(bool))
		output, err := c.sqs.ReceiveMessage(ctx, input)
		if err != nil {
			c.logger.Error(err, "error receiving messages, retrying in 10s")
			time.Sleep(10 * time.Second)
			continue
		}

		msgNum := len(output.Messages)
		c.workerWG.Add(msgNum)
		for i, m := range output.Messages {
			if _, ok := m.MessageAttributes["job_type"]; !ok {
				//a message will be sent to the DLQ automatically after 4 tries if it is received but not deleted
				c.logger.Error(nil, "error receiving messages, retrying in 10s")
				continue
			}

			c.logger.V(2).Info("enqueued for processing", "message_id", *m.MessageId, "total_messages", msgNum)
			// pass the original pointer and not the local copy
			jobs <- newJob(&output.Messages[i])
		}

		if c.stopFlag.Load().(bool) {
			c.logger.V(2).Info("exiting the dequeue loop")
			c.pollerWG.Done()
			return
		}
	}
}

// worker is an always-on concurrent worker that will take tasks when they are added into the messages buffer
func (c *client) worker(ctx context.Context, id int, messages <-chan *sqsJob) {
	for m := range messages {
		if err := c.run(ctx, m); err != nil {
			c.logger.Error(err, "error processing message", "message_id", *m.MessageId, "worker_id", id)
		}
	}
}

// run should be run within a worker. If there is no handler for that route, then the message will be deleted and
// fully consumed. If the handler exists, it will wait for the errorChannel channel to be processed. Once it receives feedback
// from the handler in the form of a channel, it will either log the error, or consume the message.
func (c *client) run(ctx context.Context, m *sqsJob) error {
	c.logger.V(2).Info("processing message", "message_id", *m.MessageId)
	if h, ok := c.handlers[m.Type()]; ok {
		go c.extend(ctx, m)
		if err := h(ctx, m); err != nil {
			c.workerWG.Done()
			return m.ErrorResponse(ctx, err)
		}
		m.Success(ctx)
	}
	c.logger.V(2).Info("consuming message", "message_id", *m.MessageId)
	return c.delete(ctx, m)
}

// delete will remove a message from the queue, this is necessary to fully and successfully consume a message.
func (c *client) delete(ctx context.Context, m *sqsJob) error {
	input := &sqs.DeleteMessageInput{
		QueueUrl:      &c.queueURL,
		ReceiptHandle: m.ReceiptHandle,
	}
	_, err := c.sqs.DeleteMessage(ctx, input)
	if err != nil {
		c.logger.Error(err, "error consuming message", "message_id", *m.MessageId)
		return dejq.ErrUnableToDelete.Context(err)
	}
	c.logger.V(2).Info("consumed message", "message_id", *m.MessageId)
	c.workerWG.Done()
	return nil
}

func (c *client) extend(ctx context.Context, m *sqsJob) {
	// add extra 10 seconds for HTTP REST processing
	tick := time.Duration(c.visibilityTimeoutSec-10) * time.Second
	timer := time.NewTimer(tick)
	count := 0
	for {
		if count >= c.maxExtensions {
			c.logger.Error(nil, "exceeded maximum amount of heartbeats", "message_id", *m.MessageId)
			return
		}
		count++

		select {
		case <-m.errorChannel:
			// worker is done
			c.logger.V(2).Info("heartbeat done", "message_id", *m.MessageId)
			timer.Stop()
			return
		case <-timer.C:
			c.logger.V(2).Info("extending message visibility", "message_id", *m.MessageId)
			input := &sqs.ChangeMessageVisibilityInput{
				QueueUrl:          &c.queueURL,
				ReceiptHandle:     m.ReceiptHandle,
				VisibilityTimeout: int32(c.visibilityTimeoutSec),
			}
			_, err := c.sqs.ChangeMessageVisibility(ctx, input)
			if err != nil {
				c.logger.Error(err, "unable to extend message visibility", "message_id", *m.MessageId)
				return
			}
			timer.Reset(tick)
		}
	}
}
