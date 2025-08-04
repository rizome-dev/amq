// Package amq provides an Agentic Message Queue SDK for agent communication
package amq

import (
	"context"
	"fmt"
	"time"
	
	"github.com/rizome-dev/amq/pkg/client"
	"github.com/rizome-dev/amq/pkg/queue"
	"github.com/rizome-dev/amq/pkg/store"
	"github.com/rizome-dev/amq/pkg/types"
)

// AMQ is the main client for the Agentic Message Queue
type AMQ struct {
	manager *queue.Manager
	store   store.Store
	config  Config
}

// Config holds AMQ configuration
type Config struct {
	// StorePath is the path to the BadgerDB data directory
	StorePath string
	
	// WorkerPoolSize is the number of worker goroutines per queue
	WorkerPoolSize int
	
	// HeartbeatInterval is how often agents should send heartbeats
	HeartbeatInterval time.Duration
	
	// MessageTimeout is the default timeout for message processing
	MessageTimeout time.Duration
}

// DefaultConfig returns the default configuration
func DefaultConfig() Config {
	return Config{
		StorePath:         "./amq-data",
		WorkerPoolSize:    10,
		HeartbeatInterval: 30 * time.Second,
		MessageTimeout:    5 * time.Minute,
	}
}

// New creates a new AMQ instance
func New(config Config) (*AMQ, error) {
	// Create store
	store := store.NewBadgerStore()
	if err := store.Open(config.StorePath); err != nil {
		return nil, fmt.Errorf("failed to open store: %w", err)
	}
	
	// Create queue manager
	queueConfig := queue.Config{
		WorkerPoolSize:      config.WorkerPoolSize,
		MessageTimeout:      config.MessageTimeout,
		ExpiryCheckInterval: 1 * time.Minute,
		RetryInterval:       30 * time.Second,
	}
	manager := queue.NewManager(store, queueConfig)
	
	// Start manager
	if err := manager.Start(); err != nil {
		store.Close()
		return nil, fmt.Errorf("failed to start manager: %w", err)
	}
	
	return &AMQ{
		manager: manager,
		store:   store,
		config:  config,
	}, nil
}

// Close closes the AMQ instance
func (a *AMQ) Close() error {
	if err := a.manager.Stop(); err != nil {
		return err
	}
	return a.store.Close()
}

// NewClient creates a new client connection
func (a *AMQ) NewClient(id string, opts ...client.ClientOption) (client.Client, error) {
	return client.NewClient(id, a.manager, opts...)
}

// NewAsyncConsumer creates a new asynchronous consumer
func (a *AMQ) NewAsyncConsumer(id string, opts ...client.ClientOption) (client.AsyncConsumer, error) {
	return client.NewAsyncConsumer(id, a.manager, opts...)
}

// CreateQueue creates a new queue
func (a *AMQ) CreateQueue(ctx context.Context, name string, qtype types.QueueType) error {
	queue := types.NewQueue(name, qtype)
	return a.manager.CreateQueue(ctx, queue)
}

// DeleteQueue deletes a queue
func (a *AMQ) DeleteQueue(ctx context.Context, name string) error {
	return a.manager.DeleteQueue(ctx, name)
}

// GetQueueStats returns statistics for a queue
func (a *AMQ) GetQueueStats(ctx context.Context, queueName string) (*types.QueueStats, error) {
	return a.manager.GetQueueStats(ctx, queueName)
}

// GetClientInfo returns information about a client
func (a *AMQ) GetClientInfo(ctx context.Context, clientID string) (*types.Client, error) {
	return a.manager.GetClientInfo(ctx, clientID)
}

// ListQueues returns all queues in the system
func (a *AMQ) ListQueues(ctx context.Context) ([]*types.Queue, error) {
	return a.store.ListQueues(ctx)
}

// ListClients returns clients based on filter  
func (a *AMQ) ListClients(ctx context.Context, filter ClientFilter) ([]*types.Client, error) {
	storeFilter := store.ClientFilter{
		MetadataKey:   filter.MetadataKey,
		MetadataValue: filter.MetadataValue,
		Limit:         filter.Limit,
	}
	
	return a.store.ListClients(ctx, storeFilter)
}

// AdminSubmitTask allows direct task submission (for admin/testing)
func (a *AMQ) AdminSubmitTask(ctx context.Context, from, topic string, payload []byte) (*types.Message, error) {
	return a.manager.SubmitTask(ctx, from, topic, payload)
}

// AdminSendDirect allows direct message sending (for admin/testing)
func (a *AMQ) AdminSendDirect(ctx context.Context, from, to string, payload []byte) (*types.Message, error) {
	return a.manager.SendDirectMessage(ctx, from, to, payload)
}

// ClientFilter defines filters for listing clients
type ClientFilter struct {
	MetadataKey   string
	MetadataValue string
	Limit         int
}

// Version returns the AMQ version
func Version() string {
	return "0.1.0"
}

// Manager returns the underlying queue manager for advanced usage
func (a *AMQ) Manager() *queue.Manager {
	return a.manager
}
