package client

import (
	"context"
	
	"github.com/rizome-dev/amq/pkg/types"
)

// Producer interface for sending messages
type Producer interface {
	// SubmitTask submits a task to a topic
	SubmitTask(ctx context.Context, topic string, payload []byte, opts ...MessageOption) (*types.Message, error)
	
	// SendDirect sends a direct message to another client
	SendDirect(ctx context.Context, to string, payload []byte, opts ...MessageOption) (*types.Message, error)
}

// Consumer interface for receiving messages
type Consumer interface {
	// Subscribe subscribes to one or more topics
	Subscribe(ctx context.Context, topics ...string) error
	
	// Unsubscribe unsubscribes from one or more topics
	Unsubscribe(ctx context.Context, topics ...string) error
	
	// Receive receives messages (blocking with timeout)
	Receive(ctx context.Context, limit int) ([]*types.Message, error)
	
	// Ack acknowledges successful message processing
	Ack(ctx context.Context, messageID string) error
	
	// Nack acknowledges failed message processing
	Nack(ctx context.Context, messageID string, reason string) error
}

// Client interface combines Producer and Consumer
type Client interface {
	Producer
	Consumer
	
	// ID returns the client identifier
	ID() string
	
	// Close closes the client connection
	Close() error
}

// MessageHandler is a callback for handling messages
type MessageHandler func(ctx context.Context, msg *types.Message) error

// ConsumerOptions configure a consumer
type ConsumerOptions struct {
	// MaxConcurrency limits concurrent message processing
	MaxConcurrency int
	
	// PollInterval sets how often to poll for messages
	PollInterval int
	
	// BatchSize sets how many messages to fetch at once
	BatchSize int
	
	// AutoAck automatically acknowledges messages after handler returns nil
	AutoAck bool
}

// AsyncConsumer interface for asynchronous message consumption
type AsyncConsumer interface {
	Consumer
	
	// Start starts asynchronous message consumption with a handler
	Start(handler MessageHandler, opts ConsumerOptions) error
	
	// Stop stops asynchronous message consumption
	Stop() error
}

// MessageOption is a function that modifies message properties
type MessageOption func(*types.Message)

// ClientOption is a function that configures a client
type ClientOption func(*clientConfig)

// clientConfig holds client configuration
type clientConfig struct {
	metadata map[string]string
}

// WithMetadata adds metadata to the client
func WithMetadata(key, value string) ClientOption {
	return func(c *clientConfig) {
		if c.metadata == nil {
			c.metadata = make(map[string]string)
		}
		c.metadata[key] = value
	}
}

// ConnectionInfo provides information about a client connection
type ConnectionInfo struct {
	ClientID     string
	Subscriptions []string
	LastActivity string
	Metadata     map[string]string
}