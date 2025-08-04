// Package errors provides error types for amq
package errors

import (
	"errors"
	"fmt"
	"time"
)

// Common AMQ errors
var (
	// Queue errors
	ErrQueueNotFound    = errors.New("queue not found")
	ErrQueueExists      = errors.New("queue already exists")
	ErrQueueFull        = errors.New("queue at capacity")
	ErrQueueEmpty       = errors.New("queue is empty")
	
	// Client errors
	ErrClientNotFound   = errors.New("client not registered")
	ErrClientExists     = errors.New("client already exists")
	ErrClientNotActive  = errors.New("client not active")
	
	// Message errors
	ErrMessageNotFound  = errors.New("message not found")
	ErrMessageExpired   = errors.New("message TTL exceeded")
	ErrMessageTooLarge  = errors.New("message exceeds size limit")
	ErrInvalidMessage   = errors.New("invalid message format")
	
	// Subscription errors
	ErrNotSubscribed    = errors.New("client not subscribed to topic")
	ErrAlreadySubscribed = errors.New("client already subscribed")
	
	// System errors
	ErrBackpressure     = errors.New("system under load")
	ErrCircuitOpen      = errors.New("circuit breaker open")
	ErrShuttingDown     = errors.New("system shutting down")
	ErrTimeout          = errors.New("operation timed out")
	
	// Storage errors
	ErrStorageFailure   = errors.New("storage operation failed")
	ErrTransactionFailed = errors.New("transaction failed")
)

// APIError represents an API error
type APIError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	StatusCode int `json:"status_code"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("API error %d: %s", e.Code, e.Message)
}

// NewAPIError creates a new API error
func NewAPIError(statusCode int, message string) *APIError {
	return &APIError{
		StatusCode: statusCode,
		Message:    message,
		Code:       statusCode,
	}
}

// AMQError provides structured error information
type AMQError struct {
	Code      ErrorCode     `json:"code"`
	Message   string        `json:"message"`
	Component string        `json:"component"`
	Timestamp time.Time     `json:"timestamp"`
	Cause     error         `json:"-"`
	Details   interface{}   `json:"details,omitempty"`
}

// ErrorCode represents specific error codes
type ErrorCode int

const (
	// Queue errors (1000-1999)
	CodeQueueNotFound    ErrorCode = 1001
	CodeQueueExists      ErrorCode = 1002
	CodeQueueFull        ErrorCode = 1003
	CodeQueueEmpty       ErrorCode = 1004
	
	// Client errors (2000-2999)
	CodeClientNotFound   ErrorCode = 2001
	CodeClientExists     ErrorCode = 2002
	CodeClientNotActive  ErrorCode = 2003
	CodeAuthFailed       ErrorCode = 2004
	
	// Message errors (3000-3999)
	CodeMessageNotFound  ErrorCode = 3001
	CodeMessageExpired   ErrorCode = 3002
	CodeMessageTooLarge  ErrorCode = 3003
	CodeInvalidMessage   ErrorCode = 3004
	
	// System errors (4000-4999)
	CodeBackpressure     ErrorCode = 4001
	CodeCircuitOpen      ErrorCode = 4002
	CodeShuttingDown     ErrorCode = 4003
	CodeTimeout          ErrorCode = 4004
	CodeRateLimited      ErrorCode = 4005
	
	// Storage errors (5000-5999)
	CodeStorageFailure   ErrorCode = 5001
	CodeTransactionFailed ErrorCode = 5002
)

// Error implements the error interface
func (e *AMQError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s (caused by: %v)", e.Component, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Component, e.Message)
}

// Unwrap returns the underlying error
func (e *AMQError) Unwrap() error {
	return e.Cause
}

// NewAMQError creates a new AMQ error
func NewAMQError(code ErrorCode, component, message string) *AMQError {
	return &AMQError{
		Code:      code,
		Message:   message,
		Component: component,
		Timestamp: time.Now(),
	}
}

// WithCause adds a cause to the error
func (e *AMQError) WithCause(cause error) *AMQError {
	e.Cause = cause
	return e
}

// WithDetails adds details to the error
func (e *AMQError) WithDetails(details interface{}) *AMQError {
	e.Details = details
	return e
}

// IsRetryable returns true if the error is retryable
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	
	// Check standard errors
	switch err {
	case ErrBackpressure, ErrTimeout, ErrStorageFailure, ErrTransactionFailed:
		return true
	case ErrQueueNotFound, ErrClientNotFound, ErrMessageExpired:
		return false
	}
	
	// Check AMQ errors
	var amqErr *AMQError
	if errors.As(err, &amqErr) {
		switch amqErr.Code {
		case CodeBackpressure, CodeTimeout, CodeStorageFailure, CodeTransactionFailed:
			return true
		}
	}
	
	return false
}

// IsFatal returns true if the error is fatal and should not be retried
func IsFatal(err error) bool {
	if err == nil {
		return false
	}
	
	// Check standard errors
	switch err {
	case ErrMessageExpired, ErrMessageTooLarge, ErrInvalidMessage:
		return true
	}
	
	// Check AMQ errors
	var amqErr *AMQError
	if errors.As(err, &amqErr) {
		switch amqErr.Code {
		case CodeMessageExpired, CodeMessageTooLarge, CodeInvalidMessage, CodeAuthFailed:
			return true
		}
	}
	
	return false
}

// ErrorResponse is used for API error responses
type ErrorResponse struct {
	Error     string      `json:"error"`
	Code      ErrorCode   `json:"code"`
	Timestamp time.Time   `json:"timestamp"`
	RequestID string      `json:"request_id,omitempty"`
	Details   interface{} `json:"details,omitempty"`
}

// NewErrorResponse creates a new error response
func NewErrorResponse(err error, requestID string) *ErrorResponse {
	resp := &ErrorResponse{
		Error:     err.Error(),
		Timestamp: time.Now(),
		RequestID: requestID,
	}
	
	var amqErr *AMQError
	if errors.As(err, &amqErr) {
		resp.Code = amqErr.Code
		resp.Details = amqErr.Details
	}
	
	return resp
}