package dejq

import (
	"fmt"
)

// DError defines the error handler for the gosqs package. DError satisfies the error interface and can be
// used safely with other error handlers
type DError struct {
	Err string `json:"errorChannel"`
	// contextErr passes the actual error as part of the error message
	contextErr error
}

// Error is used for implementing the error interface, and for creating
// a proper error string
func (e *DError) Error() string {
	if e.contextErr != nil {
		return fmt.Sprintf("%s: %s", e.Err, e.contextErr.Error())
	}

	return e.Err
}

// Context is used for creating a new instance of the error with the contextual error attached
func (e *DError) Context(err error) *DError {
	ctxErr := new(DError)
	*ctxErr = *e
	ctxErr.contextErr = err

	return ctxErr
}

// newSQSErr creates a new SQS Error
func newSQSErr(msg string) *DError {
	e := new(DError)
	e.Err = msg
	return e
}

// ErrUndefinedPublisher invalid credentials
var ErrUndefinedPublisher = newSQSErr("sqs client is undefined")

// ErrInvalidCreds invalid credentials
var ErrInvalidCreds = newSQSErr("invalid aws credentials")

// ErrUnableToDelete unable to delete item
var ErrUnableToDelete = newSQSErr("unable to delete item in queue")

// ErrUnableToExtend unable to extend message processing time
var ErrUnableToExtend = newSQSErr("unable to extend message processing time")

// ErrQueueURL undefined queueURL
var ErrQueueURL = newSQSErr("undefined queueURL")

// ErrMarshal unable to marshal request
var ErrMarshal = newSQSErr("unable to marshal request")

// ErrInvalidVal the custom attribute value must match the type of the custom attribute Datatype
var ErrInvalidVal = newSQSErr("value type does not match specified datatype")

// ErrNoRoute message received without a route
var ErrNoRoute = newSQSErr("message received without a route")

// ErrGetMessage fires when a request to retrieve messages from sqs fails
var ErrGetMessage = newSQSErr("unable to retrieve message")

// ErrMessageProcessing occurs when a message has exceeded the consumption time limit set by aws SQS
var ErrMessageProcessing = newSQSErr("processing time exceeding limit")

// ErrBodyOverflow AWS SQS can only hold payloads of 262144 bytes. Messages must either be routed to s3 or truncated
var ErrBodyOverflow = newSQSErr("message surpasses sqs limit of 262144, please truncate body")

// ErrPublish If there is an error publishing a message. gosqs will wait 10 seconds and try again up to the configured retry count
var ErrPublish = newSQSErr("message publish failure. Retrying...")
