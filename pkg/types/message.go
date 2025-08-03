package types

import (
	"time"
)

// MessageType represents the type of message
type MessageType int

const (
	// MessageTypeTask represents a task message routed via topics
	MessageTypeTask MessageType = iota
	// MessageTypeDirect represents a direct P2P message
	MessageTypeDirect
)

// MessageStatus represents the status of a message
type MessageStatus int

const (
	// MessageStatusPending - message is waiting to be processed
	MessageStatusPending MessageStatus = iota
	// MessageStatusProcessing - message is currently being processed
	MessageStatusProcessing
	// MessageStatusCompleted - message has been successfully processed
	MessageStatusCompleted
	// MessageStatusFailed - message processing failed
	MessageStatusFailed
	// MessageStatusExpired - message TTL expired
	MessageStatusExpired
	// MessageStatusDead - message moved to dead letter queue
	MessageStatusDead
)

// Message represents a message in the queue
type Message struct {
	ID          string        `json:"id"`           // Unique message identifier
	Type        MessageType   `json:"type"`         // Task or Direct
	From        string        `json:"from"`         // Sender client ID
	To          string        `json:"to,omitempty"`           // Target client ID (for Direct)
	Topic       string        `json:"topic,omitempty"`        // Topic name (for Task)
	Payload     []byte        `json:"payload"`      // Message content
	Priority    int           `json:"priority"`     // Message priority (0-9, higher is more important)
	Status      MessageStatus `json:"status"`       // Current message status
	CreatedAt   time.Time     `json:"created_at"`   // Message creation timestamp
	ProcessedAt *time.Time    `json:"processed_at,omitempty"` // When message was processed
	CompletedAt *time.Time    `json:"completed_at,omitempty"` // When message was completed
	RetryCount  int           `json:"retry_count"`  // Number of retry attempts
	MaxRetries  int           `json:"max_retries"`  // Maximum retry attempts
	TTL         time.Duration `json:"ttl,omitempty"`          // Time to live
	ExpiresAt   *time.Time    `json:"expires_at,omitempty"`   // Expiration timestamp
	Error       string        `json:"error,omitempty"`        // Error message if failed
	Metadata    map[string]string `json:"metadata,omitempty"` // Additional metadata
}

// NewTaskMessage creates a new task message
func NewTaskMessage(from, topic string, payload []byte) *Message {
	now := time.Now()
	return &Message{
		ID:         GenerateMessageID(),
		Type:       MessageTypeTask,
		From:       from,
		Topic:      topic,
		Payload:    payload,
		Priority:   5, // Default medium priority
		Status:     MessageStatusPending,
		CreatedAt:  now,
		RetryCount: 0,
		MaxRetries: 3, // Default max retries
		Metadata:   make(map[string]string),
	}
}

// NewDirectMessage creates a new direct message
func NewDirectMessage(from, to string, payload []byte) *Message {
	now := time.Now()
	return &Message{
		ID:         GenerateMessageID(),
		Type:       MessageTypeDirect,
		From:       from,
		To:         to,
		Payload:    payload,
		Priority:   5, // Default medium priority
		Status:     MessageStatusPending,
		CreatedAt:  now,
		RetryCount: 0,
		MaxRetries: 3, // Default max retries
		Metadata:   make(map[string]string),
	}
}

// IsExpired checks if the message has expired
func (m *Message) IsExpired() bool {
	if m.ExpiresAt == nil {
		return false
	}
	return time.Now().After(*m.ExpiresAt)
}

// CanRetry checks if the message can be retried
func (m *Message) CanRetry() bool {
	return m.RetryCount < m.MaxRetries && m.Status == MessageStatusFailed
}

// IncrementRetry increments the retry count
func (m *Message) IncrementRetry() {
	m.RetryCount++
}

// SetProcessing marks the message as processing
func (m *Message) SetProcessing() {
	m.Status = MessageStatusProcessing
	now := time.Now()
	m.ProcessedAt = &now
}

// SetCompleted marks the message as completed
func (m *Message) SetCompleted() {
	m.Status = MessageStatusCompleted
	now := time.Now()
	m.CompletedAt = &now
}

// SetFailed marks the message as failed with an error
func (m *Message) SetFailed(err string) {
	m.Status = MessageStatusFailed
	m.Error = err
}

// String returns string representation of MessageType
func (t MessageType) String() string {
	switch t {
	case MessageTypeTask:
		return "task"
	case MessageTypeDirect:
		return "direct"
	default:
		return "unknown"
	}
}

// String returns string representation of MessageStatus
func (s MessageStatus) String() string {
	switch s {
	case MessageStatusPending:
		return "pending"
	case MessageStatusProcessing:
		return "processing"
	case MessageStatusCompleted:
		return "completed"
	case MessageStatusFailed:
		return "failed"
	case MessageStatusExpired:
		return "expired"
	case MessageStatusDead:
		return "dead"
	default:
		return "unknown"
	}
}