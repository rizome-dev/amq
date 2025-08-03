package queue

import (
	"context"
	"fmt"
	"sync"
	"time"
	
	"github.com/rizome-dev/amq/pkg/store"
	"github.com/rizome-dev/amq/pkg/types"
)

// Manager handles queue operations and message routing
type Manager struct {
	store          store.Store
	clients        map[string]*types.Client
	clientsMutex   sync.RWMutex
	queues         map[string]*types.Queue
	queuesMutex    sync.RWMutex
	subscriptions  map[string]map[string]bool // clientID -> topics
	subMutex       sync.RWMutex
	processors     map[string]*processor
	processorMutex sync.RWMutex
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	config         Config
}

// Config holds queue manager configuration
type Config struct {
	WorkerPoolSize    int
	MessageTimeout    time.Duration
	ExpiryCheckInterval time.Duration
	RetryInterval     time.Duration
}

// DefaultConfig returns default configuration
func DefaultConfig() Config {
	return Config{
		WorkerPoolSize:      10,
		MessageTimeout:      5 * time.Minute,
		ExpiryCheckInterval: 1 * time.Minute,
		RetryInterval:       30 * time.Second,
	}
}

// NewManager creates a new queue manager
func NewManager(store store.Store, config Config) *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &Manager{
		store:         store,
		clients:       make(map[string]*types.Client),
		queues:        make(map[string]*types.Queue),
		subscriptions: make(map[string]map[string]bool),
		processors:    make(map[string]*processor),
		ctx:           ctx,
		cancel:        cancel,
		config:        config,
	}
}

// Start starts the queue manager
func (m *Manager) Start() error {
	// Load queues from store
	if err := m.loadQueues(); err != nil {
		return fmt.Errorf("failed to load queues: %w", err)
	}
	
	// Start background workers
	m.wg.Add(2)
	go m.messageExpirer()
	go m.retryProcessor()
	
	return nil
}

// Stop stops the queue manager
func (m *Manager) Stop() error {
	m.cancel()
	m.wg.Wait()
	
	// Stop all processors
	m.processorMutex.Lock()
	for _, p := range m.processors {
		p.stop()
	}
	m.processorMutex.Unlock()
	
	return nil
}

// RegisterClient registers a new client
func (m *Manager) RegisterClient(ctx context.Context, client *types.Client) error {
	// Save to store
	if err := m.store.SaveClient(ctx, client); err != nil {
		return fmt.Errorf("failed to save client: %w", err)
	}
	
	// Add to memory
	m.clientsMutex.Lock()
	m.clients[client.ID] = client
	m.clientsMutex.Unlock()
	
	// Initialize subscription map
	m.subMutex.Lock()
	m.subscriptions[client.ID] = make(map[string]bool)
	m.subMutex.Unlock()
	
	// Create inbox queue for direct messages
	inboxQueue := types.NewDirectQueue(client.ID)
	if err := m.CreateQueue(ctx, inboxQueue); err != nil {
		return fmt.Errorf("failed to create inbox queue: %w", err)
	}
	
	return nil
}

// UnregisterClient unregisters a client
func (m *Manager) UnregisterClient(ctx context.Context, clientID string) error {
	m.clientsMutex.Lock()
	_, exists := m.clients[clientID]
	if !exists {
		m.clientsMutex.Unlock()
		return fmt.Errorf("client not found: %s", clientID)
	}
	delete(m.clients, clientID)
	m.clientsMutex.Unlock()
	
	// Get client subscriptions
	m.subMutex.Lock()
	topics := m.subscriptions[clientID]
	delete(m.subscriptions, clientID)
	m.subMutex.Unlock()
	
	// Unsubscribe from all topics
	for topic := range topics {
		if err := m.removeSubscription(ctx, clientID, topic); err != nil {
			// Log but don't fail
			continue
		}
	}
	
	// Delete from store
	if err := m.store.DeleteClient(ctx, clientID); err != nil {
		return fmt.Errorf("failed to delete client: %w", err)
	}
	
	// Delete inbox queue
	if err := m.DeleteQueue(ctx, clientID); err != nil {
		// Log but don't fail
	}
	
	return nil
}

// UpdateClientActivity updates client last seen timestamp
func (m *Manager) UpdateClientActivity(ctx context.Context, clientID string) error {
	m.clientsMutex.Lock()
	client, exists := m.clients[clientID]
	if !exists {
		m.clientsMutex.Unlock()
		return fmt.Errorf("client not found: %s", clientID)
	}
	
	client.UpdateActivity()
	m.clientsMutex.Unlock()
	
	// Update in store
	return m.store.UpdateClient(ctx, client)
}

// CreateQueue creates a new queue
func (m *Manager) CreateQueue(ctx context.Context, queue *types.Queue) error {
	// Save to store
	if err := m.store.SaveQueue(ctx, queue); err != nil {
		return fmt.Errorf("failed to save queue: %w", err)
	}
	
	// Add to memory
	m.queuesMutex.Lock()
	m.queues[queue.Name] = queue
	m.queuesMutex.Unlock()
	
	return nil
}

// DeleteQueue deletes a queue
func (m *Manager) DeleteQueue(ctx context.Context, queueName string) error {
	m.queuesMutex.Lock()
	delete(m.queues, queueName)
	m.queuesMutex.Unlock()
	
	return m.store.DeleteQueue(ctx, queueName)
}

// SubmitTask submits a task message to a topic
func (m *Manager) SubmitTask(ctx context.Context, from, topic string, payload []byte, opts ...MessageOption) (*types.Message, error) {
	// Update client activity
	_ = m.UpdateClientActivity(ctx, from)
	
	// Create task message
	msg := types.NewTaskMessage(from, topic, payload)
	
	// Apply options
	for _, opt := range opts {
		opt(msg)
	}
	
	// Save message
	if err := m.store.SaveMessage(ctx, msg); err != nil {
		return nil, fmt.Errorf("failed to save message: %w", err)
	}
	
	// Enqueue to topic queue
	if err := m.store.EnqueueMessage(ctx, topic, msg.ID); err != nil {
		return nil, fmt.Errorf("failed to enqueue message: %w", err)
	}
	
	// Update queue stats
	m.queuesMutex.RLock()
	queue, exists := m.queues[topic]
	m.queuesMutex.RUnlock()
	
	if exists {
		queue.IncrementEnqueued()
		_ = m.store.UpdateQueue(ctx, queue)
	}
	
	// Notify processors
	m.notifyProcessors(topic)
	
	return msg, nil
}

// SendDirectMessage sends a direct message to a client
func (m *Manager) SendDirectMessage(ctx context.Context, from, to string, payload []byte, opts ...MessageOption) (*types.Message, error) {
	// Update client activity
	_ = m.UpdateClientActivity(ctx, from)
	
	// Verify target client exists
	m.clientsMutex.RLock()
	_, exists := m.clients[to]
	m.clientsMutex.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("target client not found: %s", to)
	}
	
	// Create direct message
	msg := types.NewDirectMessage(from, to, payload)
	
	// Apply options
	for _, opt := range opts {
		opt(msg)
	}
	
	// Save message
	if err := m.store.SaveMessage(ctx, msg); err != nil {
		return nil, fmt.Errorf("failed to save message: %w", err)
	}
	
	// Enqueue to client's inbox
	if err := m.store.EnqueueMessage(ctx, to, msg.ID); err != nil {
		return nil, fmt.Errorf("failed to enqueue message: %w", err)
	}
	
	// Update queue stats
	m.queuesMutex.RLock()
	queue, exists := m.queues[to]
	m.queuesMutex.RUnlock()
	
	if exists {
		queue.IncrementEnqueued()
		_ = m.store.UpdateQueue(ctx, queue)
	}
	
	// Notify processor
	m.notifyProcessors(to)
	
	return msg, nil
}

// ReceiveMessages receives messages for a client
func (m *Manager) ReceiveMessages(ctx context.Context, clientID string, limit int) ([]*types.Message, error) {
	// Update client activity
	_ = m.UpdateClientActivity(ctx, clientID)
	
	// Verify client exists
	m.clientsMutex.RLock()
	_, exists := m.clients[clientID]
	m.clientsMutex.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("client not found: %s", clientID)
	}
	
	messages := make([]*types.Message, 0, limit)
	
	// Check direct messages first
	for i := 0; i < limit; i++ {
		msgID, err := m.store.DequeueMessage(ctx, clientID)
		if err != nil {
			break // No more direct messages
		}
		
		msg, err := m.store.GetMessage(ctx, msgID)
		if err != nil {
			continue
		}
		
		// Mark as processing
		msg.SetProcessing()
		if err := m.store.UpdateMessage(ctx, msg); err != nil {
			// Re-enqueue on error
			_ = m.store.EnqueueMessage(ctx, clientID, msgID)
			continue
		}
		
		messages = append(messages, msg)
	}
	
	// Check subscribed topics if needed
	remaining := limit - len(messages)
	if remaining > 0 {
		m.subMutex.RLock()
		topics := m.subscriptions[clientID]
		m.subMutex.RUnlock()
		
		for topic := range topics {
			for i := 0; i < remaining; i++ {
				msgID, err := m.store.DequeueMessage(ctx, topic)
				if err != nil {
					break // No more messages in this topic
				}
				
				msg, err := m.store.GetMessage(ctx, msgID)
				if err != nil {
					continue
				}
				
				// Mark as processing
				msg.SetProcessing()
				if err := m.store.UpdateMessage(ctx, msg); err != nil {
					// Re-enqueue on error
					_ = m.store.EnqueueMessage(ctx, topic, msgID)
					continue
				}
				
				messages = append(messages, msg)
			}
			
			remaining = limit - len(messages)
			if remaining <= 0 {
				break
			}
		}
	}
	
	return messages, nil
}

// AckMessage acknowledges successful message processing
func (m *Manager) AckMessage(ctx context.Context, clientID, messageID string) error {
	// Update client activity
	_ = m.UpdateClientActivity(ctx, clientID)
	
	msg, err := m.store.GetMessage(ctx, messageID)
	if err != nil {
		return fmt.Errorf("failed to get message: %w", err)
	}
	
	// Verify message is in processing state
	if msg.Status != types.MessageStatusProcessing {
		return fmt.Errorf("message not in processing state")
	}
	
	// Mark as completed
	msg.SetCompleted()
	if err := m.store.UpdateMessage(ctx, msg); err != nil {
		return fmt.Errorf("failed to update message: %w", err)
	}
	
	return nil
}

// NackMessage acknowledges failed message processing
func (m *Manager) NackMessage(ctx context.Context, clientID, messageID string, reason string) error {
	// Update client activity
	_ = m.UpdateClientActivity(ctx, clientID)
	
	msg, err := m.store.GetMessage(ctx, messageID)
	if err != nil {
		return fmt.Errorf("failed to get message: %w", err)
	}
	
	// Verify message is in processing state
	if msg.Status != types.MessageStatusProcessing {
		return fmt.Errorf("message not in processing state")
	}
	
	// Mark as failed
	msg.SetFailed(reason)
	msg.IncrementRetry()
	
	// Check if can retry
	if msg.CanRetry() {
		// Re-enqueue for retry
		msg.Status = types.MessageStatusPending
		if err := m.store.UpdateMessage(ctx, msg); err != nil {
			return fmt.Errorf("failed to update message: %w", err)
		}
		
		// Re-enqueue to appropriate queue
		queueName := msg.Topic
		if msg.Type == types.MessageTypeDirect {
			queueName = msg.To
		}
		
		if err := m.store.EnqueueMessage(ctx, queueName, msg.ID); err != nil {
			return fmt.Errorf("failed to re-enqueue message: %w", err)
		}
	} else {
		// Move to dead letter queue
		msg.Status = types.MessageStatusDead
		if err := m.store.UpdateMessage(ctx, msg); err != nil {
			return fmt.Errorf("failed to update message: %w", err)
		}
		
		if err := m.store.EnqueueMessage(ctx, "dead-letter", msg.ID); err != nil {
			return fmt.Errorf("failed to enqueue to dead letter: %w", err)
		}
	}
	
	return nil
}

// SubscribeToTopic subscribes a client to a topic
func (m *Manager) SubscribeToTopic(ctx context.Context, clientID, topic string) error {
	// Verify client exists
	m.clientsMutex.RLock()
	_, exists := m.clients[clientID]
	m.clientsMutex.RUnlock()
	
	if !exists {
		return fmt.Errorf("client not found: %s", clientID)
	}
	
	// Update subscriptions
	m.subMutex.Lock()
	if m.subscriptions[clientID] == nil {
		m.subscriptions[clientID] = make(map[string]bool)
	}
	m.subscriptions[clientID][topic] = true
	m.subMutex.Unlock()
	
	// Save subscription to store
	if err := m.store.AddSubscription(ctx, clientID, topic); err != nil {
		return fmt.Errorf("failed to save subscription: %w", err)
	}
	
	// Create queue if doesn't exist
	m.queuesMutex.RLock()
	_, queueExists := m.queues[topic]
	m.queuesMutex.RUnlock()
	
	if !queueExists {
		queue := types.NewTaskQueue(topic)
		if err := m.CreateQueue(ctx, queue); err != nil {
			return fmt.Errorf("failed to create queue: %w", err)
		}
	}
	
	// Update queue subscribers
	m.queuesMutex.Lock()
	queue := m.queues[topic]
	queue.AddSubscriber(clientID)
	m.queuesMutex.Unlock()
	
	return m.store.UpdateQueue(ctx, queue)
}

// UnsubscribeFromTopic unsubscribes a client from a topic
func (m *Manager) UnsubscribeFromTopic(ctx context.Context, clientID, topic string) error {
	// Update subscriptions
	m.subMutex.Lock()
	if m.subscriptions[clientID] != nil {
		delete(m.subscriptions[clientID], topic)
	}
	m.subMutex.Unlock()
	
	// Remove subscription from store
	if err := m.store.RemoveSubscription(ctx, clientID, topic); err != nil {
		return fmt.Errorf("failed to remove subscription: %w", err)
	}
	
	return m.removeSubscription(ctx, clientID, topic)
}

// removeSubscription removes a subscription and updates queue
func (m *Manager) removeSubscription(ctx context.Context, clientID, topic string) error {
	// Update queue subscribers
	m.queuesMutex.Lock()
	queue, exists := m.queues[topic]
	if exists {
		queue.RemoveSubscriber(clientID)
	}
	m.queuesMutex.Unlock()
	
	if exists {
		return m.store.UpdateQueue(ctx, queue)
	}
	
	return nil
}

// GetQueueStats returns statistics for a queue
func (m *Manager) GetQueueStats(ctx context.Context, queueName string) (*types.QueueStats, error) {
	m.queuesMutex.RLock()
	queue, exists := m.queues[queueName]
	m.queuesMutex.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("queue not found: %s", queueName)
	}
	
	size, err := m.store.GetQueueSize(ctx, queueName)
	if err != nil {
		return nil, err
	}
	
	stats := &types.QueueStats{
		Name:         queue.Name,
		Type:         queue.Type,
		MessageCount: size,
		Subscribers:  len(queue.Subscribers),
		// TODO: Calculate rates based on time windows
		EnqueueRate: 0,
		DequeueRate: 0,
		ErrorRate:   0,
		AverageWait: 0,
	}
	
	return stats, nil
}

// GetClientInfo returns information about a client
func (m *Manager) GetClientInfo(ctx context.Context, clientID string) (*types.Client, error) {
	m.clientsMutex.RLock()
	client, exists := m.clients[clientID]
	m.clientsMutex.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("client not found: %s", clientID)
	}
	
	// Return a copy to avoid mutations
	clientCopy := *client
	return &clientCopy, nil
}

// loadQueues loads queues from store
func (m *Manager) loadQueues() error {
	queues, err := m.store.ListQueues(m.ctx)
	if err != nil {
		return err
	}
	
	m.queuesMutex.Lock()
	for _, queue := range queues {
		m.queues[queue.Name] = queue
	}
	m.queuesMutex.Unlock()
	
	// Create dead letter queue if doesn't exist
	if _, exists := m.queues["dead-letter"]; !exists {
		dlq := types.NewDeadLetterQueue()
		if err := m.CreateQueue(m.ctx, dlq); err != nil {
			return fmt.Errorf("failed to create dead letter queue: %w", err)
		}
	}
	
	return nil
}

// messageExpirer expires old messages
func (m *Manager) messageExpirer() {
	defer m.wg.Done()
	
	ticker := time.NewTicker(m.config.ExpiryCheckInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.expireMessages()
		}
	}
}

// expireMessages checks and expires messages
func (m *Manager) expireMessages() {
	// Get pending messages
	pending := types.MessageStatusPending
	messages, err := m.store.ListMessages(m.ctx, store.MessageFilter{
		Status: &pending,
	})
	if err != nil {
		return
	}
	
	for _, msg := range messages {
		if msg.IsExpired() {
			msg.Status = types.MessageStatusExpired
			_ = m.store.UpdateMessage(m.ctx, msg)
		}
	}
}

// retryProcessor processes retry messages
func (m *Manager) retryProcessor() {
	defer m.wg.Done()
	
	ticker := time.NewTicker(m.config.RetryInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.processRetries()
		}
	}
}

// processRetries processes messages that need retry
func (m *Manager) processRetries() {
	// Implementation for retry processing
	// This would check for failed messages that can be retried
	// and re-enqueue them based on retry policy
}

// notifyProcessors notifies processors about new messages
func (m *Manager) notifyProcessors(queueName string) {
	m.processorMutex.RLock()
	processor, exists := m.processors[queueName]
	m.processorMutex.RUnlock()
	
	if exists {
		processor.notify()
	}
}

// MessageOption is a function that modifies a message
type MessageOption func(*types.Message)

// WithPriority sets the message priority
func WithPriority(priority int) MessageOption {
	return func(m *types.Message) {
		m.Priority = priority
	}
}

// WithTTL sets the message TTL
func WithTTL(ttl time.Duration) MessageOption {
	return func(m *types.Message) {
		m.TTL = ttl
		expiresAt := m.CreatedAt.Add(ttl)
		m.ExpiresAt = &expiresAt
	}
}

// WithMaxRetries sets the maximum retry count
func WithMaxRetries(maxRetries int) MessageOption {
	return func(m *types.Message) {
		m.MaxRetries = maxRetries
	}
}

// WithMetadata adds metadata to the message
func WithMetadata(key, value string) MessageOption {
	return func(m *types.Message) {
		if m.Metadata == nil {
			m.Metadata = make(map[string]string)
		}
		m.Metadata[key] = value
	}
}