package types

import (
	"time"
)

// QueueType represents the type of queue
type QueueType int

const (
	// QueueTypeTask - queue for task messages
	QueueTypeTask QueueType = iota
	// QueueTypeDirect - queue for direct messages
	QueueTypeDirect
	// QueueTypeDead - dead letter queue
	QueueTypeDead
)

// QueueSettings contains queue-specific settings
type QueueSettings struct {
	MaxSize         int           `json:"max_size"`          // Maximum queue size (0 = unlimited)
	MaxMessageSize  int           `json:"max_message_size"`  // Maximum message size in bytes
	DefaultTTL      time.Duration `json:"default_ttl"`       // Default message TTL
	RetentionPeriod time.Duration `json:"retention_period"`  // How long to keep completed messages
	EnablePriority  bool          `json:"enable_priority"`   // Enable priority processing
	BatchSize       int           `json:"batch_size"`        // Batch size for processing
}

// Queue represents a message queue
type Queue struct {
	Name        string        `json:"name"`         // Queue/Topic name
	Type        QueueType     `json:"type"`         // Queue type
	Created     time.Time     `json:"created"`      // Queue creation time
	Settings    QueueSettings `json:"settings"`     // Queue-specific settings
	
	// Statistics
	MessageCount    int64 `json:"message_count"`     // Current message count
	TotalEnqueued   int64 `json:"total_enqueued"`    // Total messages enqueued
	TotalDequeued   int64 `json:"total_dequeued"`    // Total messages dequeued
	TotalFailed     int64 `json:"total_failed"`      // Total failed messages
	
	// Runtime state (not persisted)
	Subscribers []string `json:"subscribers,omitempty"` // Client IDs subscribed (for task queues)
}

// DefaultQueueSettings returns default queue settings
func DefaultQueueSettings() QueueSettings {
	return QueueSettings{
		MaxSize:         0,                    // Unlimited
		MaxMessageSize:  1024 * 1024,          // 1MB
		DefaultTTL:      24 * time.Hour,       // 24 hours
		RetentionPeriod: 7 * 24 * time.Hour,   // 7 days
		EnablePriority:  true,
		BatchSize:       10,
	}
}

// NewQueue creates a new queue
func NewQueue(name string, queueType QueueType) *Queue {
	return &Queue{
		Name:     name,
		Type:     queueType,
		Created:  time.Now(),
		Settings: DefaultQueueSettings(),
	}
}

// NewTaskQueue creates a new task queue with a topic
func NewTaskQueue(topic string) *Queue {
	return NewQueue(topic, QueueTypeTask)
}

// NewDirectQueue creates a new direct message queue for a client
func NewDirectQueue(clientID string) *Queue {
	return NewQueue(clientID, QueueTypeDirect)
}

// NewDeadLetterQueue creates a new dead letter queue
func NewDeadLetterQueue() *Queue {
	q := NewQueue("dead-letter", QueueTypeDead)
	// Dead letter queue has longer retention
	q.Settings.RetentionPeriod = 30 * 24 * time.Hour // 30 days
	return q
}

// AddSubscriber adds a client to the subscriber list
func (q *Queue) AddSubscriber(clientID string) {
	// Check if already subscribed
	for _, id := range q.Subscribers {
		if id == clientID {
			return
		}
	}
	q.Subscribers = append(q.Subscribers, clientID)
}

// RemoveSubscriber removes a client from the subscriber list
func (q *Queue) RemoveSubscriber(clientID string) {
	subscribers := make([]string, 0, len(q.Subscribers))
	for _, id := range q.Subscribers {
		if id != clientID {
			subscribers = append(subscribers, id)
		}
	}
	q.Subscribers = subscribers
}

// HasSubscriber checks if a client is subscribed
func (q *Queue) HasSubscriber(clientID string) bool {
	for _, id := range q.Subscribers {
		if id == clientID {
			return true
		}
	}
	return false
}

// IncrementEnqueued increments the enqueue counter
func (q *Queue) IncrementEnqueued() {
	q.MessageCount++
	q.TotalEnqueued++
}

// IncrementDequeued increments the dequeue counter
func (q *Queue) IncrementDequeued() {
	if q.MessageCount > 0 {
		q.MessageCount--
	}
	q.TotalDequeued++
}

// IncrementFailed increments the failed counter
func (q *Queue) IncrementFailed() {
	q.TotalFailed++
}

// IsFull checks if the queue is full
func (q *Queue) IsFull() bool {
	return q.Settings.MaxSize > 0 && q.MessageCount >= int64(q.Settings.MaxSize)
}

// String returns string representation of QueueType
func (t QueueType) String() string {
	switch t {
	case QueueTypeTask:
		return "task"
	case QueueTypeDirect:
		return "direct"
	case QueueTypeDead:
		return "dead"
	default:
		return "unknown"
	}
}

// QueueStats represents queue statistics
type QueueStats struct {
	Name          string        `json:"name"`
	Type          QueueType     `json:"type"`
	MessageCount  int64         `json:"message_count"`
	Subscribers   int           `json:"subscribers"`
	EnqueueRate   float64       `json:"enqueue_rate"`   // Messages per second
	DequeueRate   float64       `json:"dequeue_rate"`   // Messages per second
	ErrorRate     float64       `json:"error_rate"`     // Errors per second
	AverageWait   time.Duration `json:"average_wait"`   // Average time in queue
}