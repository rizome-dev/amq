package types

import (
	"testing"
	"time"
)

func TestNewTaskMessage(t *testing.T) {
	fromAgent := "agent-1"
	topic := "process-data"
	payload := []byte("test payload")
	
	msg := NewTaskMessage(fromAgent, topic, payload)
	
	if msg.Type != MessageTypeTask {
		t.Errorf("Expected message type %v, got %v", MessageTypeTask, msg.Type)
	}
	
	if msg.FromAgent != fromAgent {
		t.Errorf("Expected from agent %s, got %s", fromAgent, msg.FromAgent)
	}
	
	if msg.Topic != topic {
		t.Errorf("Expected topic %s, got %s", topic, msg.Topic)
	}
	
	if string(msg.Payload) != string(payload) {
		t.Errorf("Expected payload %s, got %s", string(payload), string(msg.Payload))
	}
	
	if msg.Status != MessageStatusPending {
		t.Errorf("Expected status %v, got %v", MessageStatusPending, msg.Status)
	}
	
	if msg.Priority != 5 {
		t.Errorf("Expected default priority 5, got %d", msg.Priority)
	}
	
	if msg.MaxRetries != 3 {
		t.Errorf("Expected default max retries 3, got %d", msg.MaxRetries)
	}
}

func TestNewDirectMessage(t *testing.T) {
	fromAgent := "agent-1"
	toAgent := "agent-2"
	payload := []byte("direct message")
	
	msg := NewDirectMessage(fromAgent, toAgent, payload)
	
	if msg.Type != MessageTypeDirect {
		t.Errorf("Expected message type %v, got %v", MessageTypeDirect, msg.Type)
	}
	
	if msg.FromAgent != fromAgent {
		t.Errorf("Expected from agent %s, got %s", fromAgent, msg.FromAgent)
	}
	
	if msg.ToAgent != toAgent {
		t.Errorf("Expected to agent %s, got %s", toAgent, msg.ToAgent)
	}
	
	if msg.Topic != "" {
		t.Errorf("Expected empty topic for direct message, got %s", msg.Topic)
	}
}

func TestMessageIsExpired(t *testing.T) {
	msg := NewTaskMessage("agent-1", "topic", []byte("data"))
	
	// No expiration set
	if msg.IsExpired() {
		t.Error("Message without expiration should not be expired")
	}
	
	// Set expiration in the future
	future := time.Now().Add(1 * time.Hour)
	msg.ExpiresAt = &future
	if msg.IsExpired() {
		t.Error("Message with future expiration should not be expired")
	}
	
	// Set expiration in the past
	past := time.Now().Add(-1 * time.Hour)
	msg.ExpiresAt = &past
	if !msg.IsExpired() {
		t.Error("Message with past expiration should be expired")
	}
}

func TestMessageCanRetry(t *testing.T) {
	msg := NewTaskMessage("agent-1", "topic", []byte("data"))
	
	// New message should not be retryable
	if msg.CanRetry() {
		t.Error("New message should not be retryable")
	}
	
	// Failed message should be retryable
	msg.SetFailed("error")
	if !msg.CanRetry() {
		t.Error("Failed message should be retryable")
	}
	
	// Max retries reached
	msg.RetryCount = 3
	if msg.CanRetry() {
		t.Error("Message at max retries should not be retryable")
	}
	
	// Non-failed message should not be retryable
	msg.RetryCount = 1
	msg.Status = MessageStatusCompleted
	if msg.CanRetry() {
		t.Error("Completed message should not be retryable")
	}
}

func TestMessageStatusChanges(t *testing.T) {
	msg := NewTaskMessage("agent-1", "topic", []byte("data"))
	
	// Test SetProcessing
	msg.SetProcessing()
	if msg.Status != MessageStatusProcessing {
		t.Errorf("Expected status %v, got %v", MessageStatusProcessing, msg.Status)
	}
	if msg.ProcessedAt == nil {
		t.Error("ProcessedAt should be set")
	}
	
	// Test SetCompleted
	msg.SetCompleted()
	if msg.Status != MessageStatusCompleted {
		t.Errorf("Expected status %v, got %v", MessageStatusCompleted, msg.Status)
	}
	if msg.CompletedAt == nil {
		t.Error("CompletedAt should be set")
	}
	
	// Test SetFailed
	errorMsg := "processing failed"
	msg.SetFailed(errorMsg)
	if msg.Status != MessageStatusFailed {
		t.Errorf("Expected status %v, got %v", MessageStatusFailed, msg.Status)
	}
	if msg.Error != errorMsg {
		t.Errorf("Expected error %s, got %s", errorMsg, msg.Error)
	}
}

func TestMessageTypeString(t *testing.T) {
	tests := []struct {
		msgType  MessageType
		expected string
	}{
		{MessageTypeTask, "task"},
		{MessageTypeDirect, "direct"},
		{MessageType(999), "unknown"},
	}
	
	for _, test := range tests {
		result := test.msgType.String()
		if result != test.expected {
			t.Errorf("Expected %s for type %v, got %s", test.expected, test.msgType, result)
		}
	}
}

func TestMessageStatusString(t *testing.T) {
	tests := []struct {
		status   MessageStatus
		expected string
	}{
		{MessageStatusPending, "pending"},
		{MessageStatusProcessing, "processing"},
		{MessageStatusCompleted, "completed"},
		{MessageStatusFailed, "failed"},
		{MessageStatusExpired, "expired"},
		{MessageStatusDead, "dead"},
		{MessageStatus(999), "unknown"},
	}
	
	for _, test := range tests {
		result := test.status.String()
		if result != test.expected {
			t.Errorf("Expected %s for status %v, got %s", test.expected, test.status, result)
		}
	}
}

func TestIncrementRetry(t *testing.T) {
	msg := NewTaskMessage("agent-1", "topic", []byte("data"))
	
	if msg.RetryCount != 0 {
		t.Errorf("Expected initial retry count 0, got %d", msg.RetryCount)
	}
	
	msg.IncrementRetry()
	if msg.RetryCount != 1 {
		t.Errorf("Expected retry count 1, got %d", msg.RetryCount)
	}
	
	msg.IncrementRetry()
	if msg.RetryCount != 2 {
		t.Errorf("Expected retry count 2, got %d", msg.RetryCount)
	}
}