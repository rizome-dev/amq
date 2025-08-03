package types

import (
	"time"
)

// Client represents a generic client in the AMQ system
type Client struct {
	ID         string            `json:"id"`          // Unique client identifier
	Metadata   map[string]string `json:"metadata"`    // Optional client metadata
	CreatedAt  time.Time         `json:"created_at"`  // When client was created
	LastSeen   time.Time         `json:"last_seen"`   // Last activity timestamp
}

// NewClient creates a new client
func NewClient(id string) *Client {
	now := time.Now()
	return &Client{
		ID:        id,
		Metadata:  make(map[string]string),
		CreatedAt: now,
		LastSeen:  now,
	}
}

// UpdateActivity updates the last seen timestamp
func (c *Client) UpdateActivity() {
	c.LastSeen = time.Now()
}

// IsActive checks if client has been active within the given duration
func (c *Client) IsActive(timeout time.Duration) bool {
	return time.Since(c.LastSeen) < timeout
}

// SetMetadata sets a metadata key-value pair
func (c *Client) SetMetadata(key, value string) {
	if c.Metadata == nil {
		c.Metadata = make(map[string]string)
	}
	c.Metadata[key] = value
}

// GetMetadata gets a metadata value by key
func (c *Client) GetMetadata(key string) (string, bool) {
	if c.Metadata == nil {
		return "", false
	}
	value, exists := c.Metadata[key]
	return value, exists
}

// Subscription represents a client's subscription to a topic
type Subscription struct {
	ClientID     string    `json:"client_id"`
	Topic        string    `json:"topic"`
	SubscribedAt time.Time `json:"subscribed_at"`
}

// NewSubscription creates a new subscription
func NewSubscription(clientID, topic string) *Subscription {
	return &Subscription{
		ClientID:     clientID,
		Topic:        topic,
		SubscribedAt: time.Now(),
	}
}