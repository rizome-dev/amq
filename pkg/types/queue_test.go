package types

import (
	"testing"
	"time"
)

func TestDefaultQueueSettings(t *testing.T) {
	settings := DefaultQueueSettings()
	
	if settings.MaxSize != 0 {
		t.Errorf("Expected unlimited max size (0), got %d", settings.MaxSize)
	}
	
	if settings.MaxMessageSize != 1024*1024 {
		t.Errorf("Expected max message size 1MB, got %d", settings.MaxMessageSize)
	}
	
	if settings.DefaultTTL != 24*time.Hour {
		t.Errorf("Expected default TTL 24h, got %v", settings.DefaultTTL)
	}
	
	if settings.RetentionPeriod != 7*24*time.Hour {
		t.Errorf("Expected retention period 7d, got %v", settings.RetentionPeriod)
	}
	
	if !settings.EnablePriority {
		t.Error("Expected priority to be enabled by default")
	}
	
	if settings.BatchSize != 10 {
		t.Errorf("Expected batch size 10, got %d", settings.BatchSize)
	}
}

func TestNewQueue(t *testing.T) {
	name := "test-queue"
	queueType := QueueTypeTask
	
	queue := NewQueue(name, queueType)
	
	if queue.Name != name {
		t.Errorf("Expected name %s, got %s", name, queue.Name)
	}
	
	if queue.Type != queueType {
		t.Errorf("Expected type %v, got %v", queueType, queue.Type)
	}
	
	if queue.MessageCount != 0 {
		t.Errorf("Expected message count 0, got %d", queue.MessageCount)
	}
	
	// Check that settings are initialized
	if queue.Settings.MaxMessageSize == 0 {
		t.Error("Queue settings not initialized")
	}
}

func TestNewTaskQueue(t *testing.T) {
	topic := "process-data"
	queue := NewTaskQueue(topic)
	
	if queue.Name != topic {
		t.Errorf("Expected queue name to be topic %s, got %s", topic, queue.Name)
	}
	
	if queue.Type != QueueTypeTask {
		t.Errorf("Expected queue type %v, got %v", QueueTypeTask, queue.Type)
	}
}

func TestNewDirectQueue(t *testing.T) {
	agentID := "agent-123"
	queue := NewDirectQueue(agentID)
	
	if queue.Name != agentID {
		t.Errorf("Expected queue name to be agent ID %s, got %s", agentID, queue.Name)
	}
	
	if queue.Type != QueueTypeDirect {
		t.Errorf("Expected queue type %v, got %v", QueueTypeDirect, queue.Type)
	}
}

func TestNewDeadLetterQueue(t *testing.T) {
	queue := NewDeadLetterQueue()
	
	if queue.Name != "dead-letter" {
		t.Errorf("Expected queue name 'dead-letter', got %s", queue.Name)
	}
	
	if queue.Type != QueueTypeDead {
		t.Errorf("Expected queue type %v, got %v", QueueTypeDead, queue.Type)
	}
	
	if queue.Settings.RetentionPeriod != 30*24*time.Hour {
		t.Errorf("Expected dead letter retention 30d, got %v", queue.Settings.RetentionPeriod)
	}
}

func TestQueueSubscribers(t *testing.T) {
	queue := NewTaskQueue("test-topic")
	
	// Test AddSubscriber
	queue.AddSubscriber("agent-1")
	if len(queue.Subscribers) != 1 {
		t.Errorf("Expected 1 subscriber, got %d", len(queue.Subscribers))
	}
	
	// Test duplicate subscriber
	queue.AddSubscriber("agent-1")
	if len(queue.Subscribers) != 1 {
		t.Error("Duplicate subscriber should not be added")
	}
	
	// Test HasSubscriber
	if !queue.HasSubscriber("agent-1") {
		t.Error("Should have subscriber 'agent-1'")
	}
	
	if queue.HasSubscriber("agent-2") {
		t.Error("Should not have subscriber 'agent-2'")
	}
	
	// Test RemoveSubscriber
	queue.AddSubscriber("agent-2")
	queue.RemoveSubscriber("agent-1")
	
	if queue.HasSubscriber("agent-1") {
		t.Error("Should not have subscriber 'agent-1' after removal")
	}
	
	if !queue.HasSubscriber("agent-2") {
		t.Error("Should still have subscriber 'agent-2'")
	}
}

func TestQueueCounters(t *testing.T) {
	queue := NewTaskQueue("test")
	
	// Test IncrementEnqueued
	queue.IncrementEnqueued()
	
	if queue.MessageCount != 1 {
		t.Errorf("Expected message count 1, got %d", queue.MessageCount)
	}
	
	if queue.TotalEnqueued != 1 {
		t.Errorf("Expected total enqueued 1, got %d", queue.TotalEnqueued)
	}
	
	// Test IncrementDequeued
	queue.IncrementDequeued()
	
	if queue.MessageCount != 0 {
		t.Errorf("Expected message count 0, got %d", queue.MessageCount)
	}
	
	if queue.TotalDequeued != 1 {
		t.Errorf("Expected total dequeued 1, got %d", queue.TotalDequeued)
	}
	
	// Test dequeue when empty
	queue.IncrementDequeued()
	
	if queue.MessageCount != 0 {
		t.Error("Message count should not go negative")
	}
	
	// Test IncrementFailed
	queue.IncrementFailed()
	
	if queue.TotalFailed != 1 {
		t.Errorf("Expected total failed 1, got %d", queue.TotalFailed)
	}
}

func TestQueueIsFull(t *testing.T) {
	queue := NewTaskQueue("test")
	
	// Unlimited queue should never be full
	queue.MessageCount = 1000000
	if queue.IsFull() {
		t.Error("Unlimited queue should never be full")
	}
	
	// Set max size
	queue.Settings.MaxSize = 10
	queue.MessageCount = 9
	
	if queue.IsFull() {
		t.Error("Queue should not be full at 9/10")
	}
	
	queue.MessageCount = 10
	if !queue.IsFull() {
		t.Error("Queue should be full at 10/10")
	}
	
	queue.MessageCount = 11
	if !queue.IsFull() {
		t.Error("Queue should be full at 11/10")
	}
}

func TestQueueTypeString(t *testing.T) {
	tests := []struct {
		queueType QueueType
		expected  string
	}{
		{QueueTypeTask, "task"},
		{QueueTypeDirect, "direct"},
		{QueueTypeDead, "dead"},
		{QueueType(999), "unknown"},
	}
	
	for _, test := range tests {
		result := test.queueType.String()
		if result != test.expected {
			t.Errorf("Expected %s for type %v, got %s", test.expected, test.queueType, result)
		}
	}
}