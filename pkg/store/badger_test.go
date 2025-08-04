package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	
	"github.com/rizome-dev/amq/pkg/types"
)

func setupTestStore(t *testing.T) (*BadgerStore, func()) {
	tmpDir, err := os.MkdirTemp("", "amq-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	
	store := NewBadgerStore()
	if err := store.Open(filepath.Join(tmpDir, "test.db")); err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to open store: %v", err)
	}
	
	cleanup := func() {
		store.Close()
		os.RemoveAll(tmpDir)
	}
	
	return store, cleanup
}

func TestBadgerStoreOpenClose(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	
	// Store should be open
	if store.db == nil {
		t.Error("Store database should be initialized")
	}
}

func TestMessageOperations(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	
	ctx := context.Background()
	
	// Create test message
	msg := types.NewTaskMessage("agent-1", "test-topic", []byte("test payload"))
	
	// Test SaveMessage
	if err := store.SaveMessage(ctx, msg); err != nil {
		t.Fatalf("Failed to save message: %v", err)
	}
	
	// Test GetMessage
	retrieved, err := store.GetMessage(ctx, msg.ID)
	if err != nil {
		t.Fatalf("Failed to get message: %v", err)
	}
	
	if retrieved.ID != msg.ID {
		t.Errorf("Expected message ID %s, got %s", msg.ID, retrieved.ID)
	}
	
	if string(retrieved.Payload) != string(msg.Payload) {
		t.Errorf("Expected payload %s, got %s", string(msg.Payload), string(retrieved.Payload))
	}
	
	// Test UpdateMessage
	retrieved.SetProcessing()
	if err := store.UpdateMessage(ctx, retrieved); err != nil {
		t.Fatalf("Failed to update message: %v", err)
	}
	
	updated, err := store.GetMessage(ctx, msg.ID)
	if err != nil {
		t.Fatalf("Failed to get updated message: %v", err)
	}
	
	if updated.Status != types.MessageStatusProcessing {
		t.Errorf("Expected status %v, got %v", types.MessageStatusProcessing, updated.Status)
	}
	
	// Test DeleteMessage
	if err := store.DeleteMessage(ctx, msg.ID); err != nil {
		t.Fatalf("Failed to delete message: %v", err)
	}
	
	_, err = store.GetMessage(ctx, msg.ID)
	if err == nil {
		t.Error("Expected error getting deleted message")
	}
}

func TestQueueOperations(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	
	ctx := context.Background()
	queueName := "test-queue"
	
	// Test EnqueueMessage
	msgIDs := []string{"msg-1", "msg-2", "msg-3"}
	for _, msgID := range msgIDs {
		if err := store.EnqueueMessage(ctx, queueName, msgID); err != nil {
			t.Fatalf("Failed to enqueue message %s: %v", msgID, err)
		}
	}
	
	// Test GetQueueSize
	size, err := store.GetQueueSize(ctx, queueName)
	if err != nil {
		t.Fatalf("Failed to get queue size: %v", err)
	}
	
	if size != int64(len(msgIDs)) {
		t.Errorf("Expected queue size %d, got %d", len(msgIDs), size)
	}
	
	// Test PeekQueue
	peeked, err := store.PeekQueue(ctx, queueName, 2)
	if err != nil {
		t.Fatalf("Failed to peek queue: %v", err)
	}
	
	if len(peeked) != 2 {
		t.Errorf("Expected 2 peeked messages, got %d", len(peeked))
	}
	
	if peeked[0] != msgIDs[0] || peeked[1] != msgIDs[1] {
		t.Error("Peeked messages don't match expected order")
	}
	
	// Test DequeueMessage
	dequeued, err := store.DequeueMessage(ctx, queueName)
	if err != nil {
		t.Fatalf("Failed to dequeue message: %v", err)
	}
	
	if dequeued != msgIDs[0] {
		t.Errorf("Expected dequeued message %s, got %s", msgIDs[0], dequeued)
	}
	
	// Check size after dequeue
	size, _ = store.GetQueueSize(ctx, queueName)
	if size != 2 {
		t.Errorf("Expected queue size 2 after dequeue, got %d", size)
	}
	
	// Test dequeue until empty
	for i := 0; i < 2; i++ {
		_, err = store.DequeueMessage(ctx, queueName)
		if err != nil {
			t.Fatalf("Failed to dequeue message: %v", err)
		}
	}
	
	// Test dequeue from empty queue
	_, err = store.DequeueMessage(ctx, queueName)
	if err == nil {
		t.Error("Expected error dequeuing from empty queue")
	}
}

/*
func TestAgentOperations(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	
	ctx := context.Background()
	
	// Create test agent
	agent := types.NewAgent("test-agent", "Test Agent")
	agent.AddCapability("processing")
	agent.Subscribe("topic-1")
	
	// Test SaveAgent
	if err := store.SaveAgent(ctx, agent); err != nil {
		t.Fatalf("Failed to save agent: %v", err)
	}
	
	// Test GetAgent
	retrieved, err := store.GetAgent(ctx, agent.ID)
	if err != nil {
		t.Fatalf("Failed to get agent: %v", err)
	}
	
	if retrieved.Name != agent.Name {
		t.Errorf("Expected agent name %s, got %s", agent.Name, retrieved.Name)
	}
	
	// Test UpdateAgent
	retrieved.SetStatus(types.AgentStatusActive)
	if err := store.UpdateAgent(ctx, retrieved); err != nil {
		t.Fatalf("Failed to update agent: %v", err)
	}
	
	updated, err := store.GetAgent(ctx, agent.ID)
	if err != nil {
		t.Fatalf("Failed to get updated agent: %v", err)
	}
	
	if updated.Status != types.AgentStatusActive {
		t.Errorf("Expected status %v, got %v", types.AgentStatusActive, updated.Status)
	}
	
	// Test DeleteAgent
	if err := store.DeleteAgent(ctx, agent.ID); err != nil {
		t.Fatalf("Failed to delete agent: %v", err)
	}
	
	_, err = store.GetAgent(ctx, agent.ID)
	if err == nil {
		t.Error("Expected error getting deleted agent")
	}
}
*/

func TestListMessages(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	
	ctx := context.Background()
	
	// Create test messages
	msg1 := types.NewTaskMessage("agent-1", "topic-1", []byte("msg1"))
	msg2 := types.NewTaskMessage("agent-2", "topic-2", []byte("msg2"))
	msg3 := types.NewDirectMessage("agent-1", "agent-2", []byte("msg3"))
	
	// Set different statuses
	msg2.SetProcessing()
	msg3.SetCompleted()
	
	// Save messages
	for _, msg := range []*types.Message{msg1, msg2, msg3} {
		if err := store.SaveMessage(ctx, msg); err != nil {
			t.Fatalf("Failed to save message: %v", err)
		}
	}
	
	// Test filter by status
	pending := types.MessageStatusPending
	messages, err := store.ListMessages(ctx, MessageFilter{
		Status: &pending,
	})
	if err != nil {
		t.Fatalf("Failed to list messages: %v", err)
	}
	
	if len(messages) != 1 {
		t.Errorf("Expected 1 pending message, got %d", len(messages))
	}
	
	// Test filter by topic
	messages, err = store.ListMessages(ctx, MessageFilter{
		Topic: "topic-1",
	})
	if err != nil {
		t.Fatalf("Failed to list messages: %v", err)
	}
	
	if len(messages) != 1 {
		t.Errorf("Expected 1 message for topic-1, got %d", len(messages))
	}
	
	// Test filter by client
	messages, err = store.ListMessages(ctx, MessageFilter{
		ClientID: "agent-1",
	})
	if err != nil {
		t.Fatalf("Failed to list messages: %v", err)
	}
	
	if len(messages) != 2 {
		t.Errorf("Expected 2 messages for agent-1, got %d", len(messages))
	}
	
	// Test with limit
	messages, err = store.ListMessages(ctx, MessageFilter{
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("Failed to list messages: %v", err)
	}
	
	if len(messages) != 2 {
		t.Errorf("Expected 2 messages with limit, got %d", len(messages))
	}
}

/*
func TestListAgents(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	
	ctx := context.Background()
	
	// Create test agents
	agent1 := types.NewAgent("agent-1", "Agent 1")
	agent1.AddCapability("processing")
	agent1.Subscribe("topic-1")
	
	agent2 := types.NewAgent("agent-2", "Agent 2")
	agent2.AddCapability("routing")
	agent2.Subscribe("topic-1")
	agent2.Subscribe("topic-2")
	agent2.SetStatus(types.AgentStatusActive)
	
	// Save agents
	for _, agent := range []*types.Agent{agent1, agent2} {
		if err := store.SaveAgent(ctx, agent); err != nil {
			t.Fatalf("Failed to save agent: %v", err)
		}
	}
	
	// Test list all
	agents, err := store.ListAgents(ctx, AgentFilter{})
	if err != nil {
		t.Fatalf("Failed to list agents: %v", err)
	}
	
	if len(agents) != 2 {
		t.Errorf("Expected 2 agents, got %d", len(agents))
	}
	
	// Test filter by status
	active := types.AgentStatusActive
	agents, err = store.ListAgents(ctx, AgentFilter{
		Status: &active,
	})
	if err != nil {
		t.Fatalf("Failed to list agents: %v", err)
	}
	
	if len(agents) != 1 {
		t.Errorf("Expected 1 active agent, got %d", len(agents))
	}
	
	// Test filter by capability
	agents, err = store.ListAgents(ctx, AgentFilter{
		Capability: "processing",
	})
	if err != nil {
		t.Fatalf("Failed to list agents: %v", err)
	}
	
	if len(agents) != 1 {
		t.Errorf("Expected 1 agent with processing capability, got %d", len(agents))
	}
	
	// Test filter by topic
	agents, err = store.ListAgents(ctx, AgentFilter{
		Topic: "topic-2",
	})
	if err != nil {
		t.Fatalf("Failed to list agents: %v", err)
	}
	
	if len(agents) != 1 {
		t.Errorf("Expected 1 agent subscribed to topic-2, got %d", len(agents))
	}
}
*/

func TestQueueMetadataOperations(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	
	ctx := context.Background()
	
	// Create test queue
	queue := types.NewTaskQueue("test-topic")
	
	// Test SaveQueue
	if err := store.SaveQueue(ctx, queue); err != nil {
		t.Fatalf("Failed to save queue: %v", err)
	}
	
	// Test GetQueue
	retrieved, err := store.GetQueue(ctx, queue.Name)
	if err != nil {
		t.Fatalf("Failed to get queue: %v", err)
	}
	
	if retrieved.Type != queue.Type {
		t.Errorf("Expected queue type %v, got %v", queue.Type, retrieved.Type)
	}
	
	// Test UpdateQueue
	retrieved.IncrementEnqueued()
	if err := store.UpdateQueue(ctx, retrieved); err != nil {
		t.Fatalf("Failed to update queue: %v", err)
	}
	
	updated, err := store.GetQueue(ctx, queue.Name)
	if err != nil {
		t.Fatalf("Failed to get updated queue: %v", err)
	}
	
	if updated.TotalEnqueued != 1 {
		t.Errorf("Expected total enqueued 1, got %d", updated.TotalEnqueued)
	}
	
	// Test ListQueues
	queues, err := store.ListQueues(ctx)
	if err != nil {
		t.Fatalf("Failed to list queues: %v", err)
	}
	
	if len(queues) != 1 {
		t.Errorf("Expected 1 queue, got %d", len(queues))
	}
	
	// Test DeleteQueue
	if err := store.DeleteQueue(ctx, queue.Name); err != nil {
		t.Fatalf("Failed to delete queue: %v", err)
	}
	
	_, err = store.GetQueue(ctx, queue.Name)
	if err == nil {
		t.Error("Expected error getting deleted queue")
	}
}

func TestIndexOperations(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	
	ctx := context.Background()
	
	indexName := "test-index"
	key := "test-key"
	
	// Test AddToIndex
	values := []string{"val1", "val2", "val3"}
	for _, val := range values {
		if err := store.AddToIndex(ctx, indexName, key, val); err != nil {
			t.Fatalf("Failed to add to index: %v", err)
		}
	}
	
	// Test duplicate value
	if err := store.AddToIndex(ctx, indexName, key, "val1"); err != nil {
		t.Fatalf("Failed to add duplicate to index: %v", err)
	}
	
	// Test GetFromIndex
	retrieved, err := store.GetFromIndex(ctx, indexName, key)
	if err != nil {
		t.Fatalf("Failed to get from index: %v", err)
	}
	
	if len(retrieved) != 3 {
		t.Errorf("Expected 3 values, got %d", len(retrieved))
	}
	
	// Test RemoveFromIndex
	if err := store.RemoveFromIndex(ctx, indexName, key, "val2"); err != nil {
		t.Fatalf("Failed to remove from index: %v", err)
	}
	
	retrieved, _ = store.GetFromIndex(ctx, indexName, key)
	if len(retrieved) != 2 {
		t.Errorf("Expected 2 values after removal, got %d", len(retrieved))
	}
	
	// Test remove all values
	for _, val := range retrieved {
		if err := store.RemoveFromIndex(ctx, indexName, key, val); err != nil {
			t.Fatalf("Failed to remove from index: %v", err)
		}
	}
	
	// Index should be empty
	retrieved, _ = store.GetFromIndex(ctx, indexName, key)
	if len(retrieved) != 0 {
		t.Errorf("Expected empty index, got %d values", len(retrieved))
	}
}

func TestTransaction(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	
	ctx := context.Background()
	
	// Test successful transaction
	msg := types.NewTaskMessage("agent-1", "topic", []byte("test"))
	
	err := store.RunInTransaction(ctx, func(tx Transaction) error {
		if err := tx.SaveMessage(msg); err != nil {
			return err
		}
		
		if err := tx.EnqueueMessage("test-queue", msg.ID); err != nil {
			return err
		}
		
		return nil
	})
	
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}
	
	// Verify message was saved
	retrieved, err := store.GetMessage(ctx, msg.ID)
	if err != nil {
		t.Fatalf("Failed to get message: %v", err)
	}
	
	if retrieved.ID != msg.ID {
		t.Error("Message not saved in transaction")
	}
	
	// Verify message was enqueued
	size, _ := store.GetQueueSize(ctx, "test-queue")
	if size != 1 {
		t.Error("Message not enqueued in transaction")
	}
	
	// Test failed transaction
	msg2 := types.NewTaskMessage("agent-2", "topic", []byte("test2"))
	
	err = store.RunInTransaction(ctx, func(tx Transaction) error {
		if err := tx.SaveMessage(msg2); err != nil {
			return err
		}
		
		// Simulate error
		return context.Canceled
	})
	
	if err == nil {
		t.Error("Expected transaction to fail")
	}
	
	// Verify message was not saved
	_, err = store.GetMessage(ctx, msg2.ID)
	if err == nil {
		t.Error("Message should not be saved in failed transaction")
	}
}

func TestConcurrentAccess(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()
	
	ctx := context.Background()
	
	// Test concurrent writes with retry logic
	done := make(chan error, 10)
	
	for i := 0; i < 10; i++ {
		go func(n int) {
			msg := types.NewTaskMessage("agent", "topic", []byte(string(rune('A' + n))))
			
			// Retry on transaction conflicts
			var err error
			for retry := 0; retry < 5; retry++ {
				err = store.SaveMessage(ctx, msg)
				if err == nil || !isTransactionConflict(err) {
					break
				}
			}
			done <- err
		}(i)
	}
	
	// Wait for all goroutines
	successCount := 0
	for i := 0; i < 10; i++ {
		if err := <-done; err != nil {
			t.Logf("Concurrent write error: %v", err)
		} else {
			successCount++
		}
	}
	
	// Verify messages were saved
	messages, err := store.ListMessages(ctx, MessageFilter{})
	if err != nil {
		t.Fatalf("Failed to list messages: %v", err)
	}
	
	if len(messages) != successCount {
		t.Errorf("Expected %d messages, got %d", successCount, len(messages))
	}
	
	// At least some should succeed
	if successCount < 5 {
		t.Errorf("Too few successful writes: %d/10", successCount)
	}
}

func isTransactionConflict(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == "Transaction Conflict. Please retry"
}