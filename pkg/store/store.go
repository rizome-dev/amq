package store

import (
	"context"
	"time"
	
	"github.com/rizome-dev/amq/pkg/types"
)

// Store defines the interface for message queue storage
type Store interface {
	// Initialize the store
	Open(path string) error
	Close() error
	
	// Message operations
	SaveMessage(ctx context.Context, msg *types.Message) error
	GetMessage(ctx context.Context, id string) (*types.Message, error)
	UpdateMessage(ctx context.Context, msg *types.Message) error
	DeleteMessage(ctx context.Context, id string) error
	ListMessages(ctx context.Context, filter MessageFilter) ([]*types.Message, error)
	
	// Queue operations
	EnqueueMessage(ctx context.Context, queueName string, msgID string) error
	DequeueMessage(ctx context.Context, queueName string) (string, error)
	PeekQueue(ctx context.Context, queueName string, limit int) ([]string, error)
	GetQueueSize(ctx context.Context, queueName string) (int64, error)
	
	// Client operations
	SaveClient(ctx context.Context, client *types.Client) error
	GetClient(ctx context.Context, id string) (*types.Client, error)
	UpdateClient(ctx context.Context, client *types.Client) error
	DeleteClient(ctx context.Context, id string) error
	ListClients(ctx context.Context, filter ClientFilter) ([]*types.Client, error)
	
	// Subscription operations
	AddSubscription(ctx context.Context, clientID, topic string) error
	RemoveSubscription(ctx context.Context, clientID, topic string) error
	GetSubscriptions(ctx context.Context, clientID string) ([]string, error)
	GetSubscribers(ctx context.Context, topic string) ([]string, error)
	
	// Queue metadata operations
	SaveQueue(ctx context.Context, queue *types.Queue) error
	GetQueue(ctx context.Context, name string) (*types.Queue, error)
	UpdateQueue(ctx context.Context, queue *types.Queue) error
	DeleteQueue(ctx context.Context, name string) error
	ListQueues(ctx context.Context) ([]*types.Queue, error)
	
	// Index operations
	AddToIndex(ctx context.Context, indexName, key, value string) error
	RemoveFromIndex(ctx context.Context, indexName, key, value string) error
	GetFromIndex(ctx context.Context, indexName, key string) ([]string, error)
	
	// Transaction support
	RunInTransaction(ctx context.Context, fn func(tx Transaction) error) error
}

// Transaction defines transaction operations
type Transaction interface {
	// Message operations
	SaveMessage(msg *types.Message) error
	UpdateMessage(msg *types.Message) error
	DeleteMessage(id string) error
	
	// Queue operations  
	EnqueueMessage(queueName string, msgID string) error
	DequeueMessage(queueName string) (string, error)
	
	// Client operations
	SaveClient(client *types.Client) error
	UpdateClient(client *types.Client) error
	
	// Index operations
	AddToIndex(indexName, key, value string) error
	RemoveFromIndex(indexName, key, value string) error
}

// MessageFilter defines filters for listing messages
type MessageFilter struct {
	QueueName  string
	Status     *types.MessageStatus
	ClientID   string
	Topic      string
	Since      *time.Time
	Until      *time.Time
	Limit      int
}

// ClientFilter defines filters for listing clients
type ClientFilter struct {
	MetadataKey   string
	MetadataValue string
	Limit         int
}