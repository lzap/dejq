package dejq

import (
	"fmt"
)

// DError defines the error handler for this package.
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

// Unwrap the context error.
func (e *DError) Unwrap() error {
	return e.contextErr
}

// newDejqErr creates a new SQS Error
func newDejqErr(msg string) *DError {
	e := new(DError)
	e.Err = msg
	return e
}

var ErrCreateClient = newDejqErr("unable to create client")

var ErrPayloadMarshal = newDejqErr("unable to marshal payload")

var ErrUnableToDelete = newDejqErr("unable to delete job from queue")
