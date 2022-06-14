package dejq

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"math/big"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/lzap/dejq/log"
	"github.com/lzap/dejq/log/awsadapter"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const maxRetryCount = 5
const maxMessages = int64(10)

var errDataLimit = errors.New("InvalidParameterValue: One or more parameters are invalid. Reason: Message must be shorter than 262144 bytes")

type Publisher interface {
	Enqueue(ctx context.Context, job_type string, body interface{}) error
}

// Handler provides a standardized handler method, this is the required function composition for event handlers
type Handler func(context.Context, Job) error

// Adapter implements adapters in the context
type Adapter func(Handler) Handler

// Consumer provides an interface for receiving messages through AWS SQS and SNS
type Consumer interface {
	// Consume polls for new messages and if it finds one, decodes it, sends it to the handler and deletes it
	//
	// A message is not considered dequeued until it has been successfully processed and deleted. There is a 30 seconds
	// delay between receiving a single message and receiving the same message. This delay can be adjusted in the AWS
	// console and can also be extended during operation. If a message is successfully received 4 times but not deleted,
	// it will be considered unprocessable and sent to the DLQ automatically
	//
	// Consume uses long-polling to check and retrieve messages, if it is unable to make a connection, the aws-SDK will use its
	// advanced retrying mechanism (including exponential backoff), if all the retries fail, then we will wait 10s before
	// trying again.
	//
	// When a new message is received, it runs in a separate go-routine that will handle the full consuming of the message, error reporting
	// and deleting
	Consume()

	// RegisterHandler registers an event listener and an associated handler. If the event matches, the handler will
	// be run
	RegisterHandler(name string, h Handler)

	// Message serves as the direct messaging capability within the consumer. A worker can send direct messages to other workers
	Message(ctx context.Context, queue, event string, body interface{})

	// MessageSelf serves as the self messaging capability within the consumer, a worker can send messages to itself for continued
	// processing and resiliency
	MessageSelf(ctx context.Context, event string, body interface{})
}

type client struct {
	sqs      *sqs.Client
	queueURL string
	logger   log.Logger
	senderWG sync.WaitGroup

	handlers     map[string]Handler
	heartbeatSec int
	maxBeats     int
	workerPool   int
	maxMessages  int
}

func NewPublisher(ctx context.Context, config aws.Config, queueName string, logger log.Logger) (*client, error) {
	pub := &client{
		sqs:          sqs.NewFromConfig(config),
		logger:       logger,
		heartbeatSec: 30,
	}

	if config.Logger == nil {
		config.Logger = awsadapter.NewLogger(logger)
	}

	err := pub.getQueueUrl(ctx, queueName)
	if err != nil {
		return nil, err
	}

	return pub, nil
}

func NewConsumer(ctx context.Context, config aws.Config, queueName string, logger log.Logger, heartbeatSec, maxBeats int) (*client, error) {
	client, err := NewPublisher(ctx, config, queueName, logger)
	if err != nil {
		return nil, err
	}
	// TODO: VisibilityTimeout can be retrieved from queue dynamically (error thrown when < 10 sec)
	if heartbeatSec <= 10 {
		return nil, errors.New("heartbeat cannot be shorter than 10 seconds")
	}
	client.heartbeatSec = heartbeatSec
	client.maxBeats = maxBeats
	client.maxMessages = 10
	client.workerPool = 3
	client.handlers = make(map[string]Handler)
	return client, nil
}

// RegisterHandler registers an event listener and an associated handler. If the event matches, the handler will
// be run along with any included middleware
func (c *client) RegisterHandler(name string, h Handler) {
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

func generateDeduplicationId() string {
	return randomBase62(20)[0:22]
}

func (c *client) Enqueue(ctx context.Context, jobType string, body interface{}, extraAttributes ...string) error {
	bytes, err := json.Marshal(body)
	if err != nil {
		return err
	}

	deduplicationId := generateDeduplicationId()
	attributes := []string{"dedup_id", deduplicationId}
	if len(extraAttributes) > 0 {
		attributes = append(attributes, extraAttributes...)
	}
	sqsInput := &sqs.SendMessageInput{
		MessageBody:            aws.String(string(bytes)),
		MessageAttributes:      defaultSQSAttributes(jobType, attributes...),
		MessageGroupId:         aws.String(jobType),
		MessageDeduplicationId: aws.String(deduplicationId),
		QueueUrl:               aws.String(c.queueURL),
	}

	c.senderWG.Add(1)
	go c.sendDirectMessage(ctx, sqsInput)
	return nil
}

// sendDirectMessage is used to handle sending and error failures in a separate go-routine.
//
// AWS-SDK will use their own retry mechanism for a failed request utilizing exponential backoff. If they fail
// then we will wait 10 seconds before trying again.
func (c *client) sendDirectMessage(ctx context.Context, input *sqs.SendMessageInput, retryCount ...int) {
	var count int
	if len(retryCount) != 0 {
		count = retryCount[0]
	}

	if count > maxRetryCount-1 {
		c.logger.Log(ctx, log.LogLevelError, "too many failures, giving up", nil)
		c.senderWG.Done()
		return
	}

	if _, err := c.sqs.SendMessage(ctx, input); err != nil {
		if err.Error() == errDataLimit.Error() {
			c.logger.Log(ctx, log.LogLevelError, "payload limit overflow, giving up", nil)
			c.senderWG.Done()
			return
		}

		c.logger.Log(ctx, log.LogLevelWarn, "error publishing, trying again in 10 seconds: "+err.Error(), nil)
		time.Sleep(10 * time.Second)
		c.sendDirectMessage(ctx, input, count+1)
	} else {
		c.logger.Log(ctx, log.LogLevelTrace, "message sent", nil)
		c.senderWG.Done()
	}
}

func defaultSQSAttributes(jobType string, extra ...string) map[string]types.MessageAttributeValue {
	result := map[string]types.MessageAttributeValue{
		"job_type": {DataType: aws.String("String"), StringValue: &jobType},
	}
	for i, key := range extra {
		if i%2 == 0 {
			value := extra[i+1]
			result[key] = types.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: &value,
			}
		}
	}
	return result
}

func (c *client) Wait() {
	c.senderWG.Wait()
}

// Dequeue polls for new messages and if it finds one, decodes it, sends it to the handler and deletes it
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
func (c *client) Dequeue(ctx context.Context) {
	jobs := make(chan *sqsJob)
	for w := 1; w <= c.workerPool; w++ {
		go c.worker(ctx, w, jobs)
	}

	maxMessages := int32(c.maxMessages)
	attributeNames := []string{"All"}
	for {
		input := &sqs.ReceiveMessageInput{
			QueueUrl:              &c.queueURL,
			MaxNumberOfMessages:   maxMessages,
			MessageAttributeNames: attributeNames,
		}
		output, err := c.sqs.ReceiveMessage(ctx, input)
		if err != nil {
			c.logger.Log(ctx, log.LogLevelWarn, "error receiving messages, retrying in 10s: "+err.Error(), nil)
			time.Sleep(10 * time.Second)
			continue
		}

		for _, m := range output.Messages {
			if _, ok := m.MessageAttributes["job_type"]; !ok {
				//a message will be sent to the DLQ automatically after 4 tries if it is received but not deleted
				c.logger.Log(ctx, log.LogLevelWarn, "message has no job_type: "+*m.MessageId, nil)
				continue
			}

			c.logger.Log(ctx, log.LogLevelTrace, "enqueued: "+*m.MessageId, nil)
			jobs <- newJob(&m)
		}
	}
}

// worker is an always-on concurrent worker that will take tasks when they are added into the messages buffer
func (c *client) worker(ctx context.Context, id int, messages <-chan *sqsJob) {
	for m := range messages {
		c.logger.Log(ctx, log.LogLevelTrace, "dequeued: "+*m.MessageId+" by XXXX"+string(id), nil)
		if err := c.run(ctx, m); err != nil {
			c.logger.Log(ctx, log.LogLevelWarn, "error processing message: "+err.Error(), nil)
		}
	}
}

// run should be run within a worker. If there is no handler for that route, then the message will be deleted and
// fully consumed. If the handler exists, it will wait for the err channel to be processed. Once it receives feedback
// from the handler in the form of a channel, it will either log the error, or consume the message.
func (c *client) run(ctx context.Context, m *sqsJob) error {
	c.logger.Log(ctx, log.LogLevelTrace, "processing message: "+*m.MessageId, nil)
	if h, ok := c.handlers[m.Type()]; ok {
		go c.extend(ctx, m)
		if err := h(ctx, m); err != nil {
			return m.ErrorResponse(ctx, err)
		}
		m.Success(ctx)
	}
	return c.delete(ctx, m) //MESSAGE CONSUMED
}

// delete will remove a message from the queue, this is necessary to fully and successfully consume a message.
func (c *client) delete(ctx context.Context, m *sqsJob) error {
	c.logger.Log(ctx, log.LogLevelTrace, "deleting message: "+*m.MessageId, nil)
	input := &sqs.DeleteMessageInput{
		QueueUrl:      &c.queueURL,
		ReceiptHandle: m.ReceiptHandle,
	}
	_, err := c.sqs.DeleteMessage(ctx, input)
	if err != nil {
		c.logger.Log(ctx, log.LogLevelWarn, "error deleting message: "+err.Error(), nil)
		return ErrUnableToDelete.Context(err)
	}
	return nil
}

func (c *client) extend(ctx context.Context, m *sqsJob) {
	// add extra 10 seconds for HTTP REST processing
	tick := time.Duration(c.heartbeatSec-10) * time.Second
	timer := time.NewTimer(tick)
	count := 0
	for {
		if count >= c.maxBeats {
			c.logger.Log(ctx, log.LogLevelWarn, "exceeded maximum amount of heartbeats", nil)
			return
		}
		count++

		select {
		case <-m.err:
			// worker is done
			c.logger.Log(ctx, log.LogLevelTrace, "worker is done: "+*m.MessageId, nil)
			timer.Stop()
			return
		case <-timer.C:
			c.logger.Log(ctx, log.LogLevelTrace, "extending: "+*m.MessageId, nil)
			input := &sqs.ChangeMessageVisibilityInput{
				QueueUrl:          &c.queueURL,
				ReceiptHandle:     m.ReceiptHandle,
				VisibilityTimeout: int32(c.heartbeatSec),
			}
			_, err := c.sqs.ChangeMessageVisibility(ctx, input)
			if err != nil {
				c.logger.Log(ctx, log.LogLevelError, "unable to extend message visibility: "+err.Error(), nil)
				return
			}
			timer.Reset(tick)
		}
	}
}
