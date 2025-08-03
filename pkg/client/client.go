package client

import (
	"context"
	"errors"
	"sync"
	"time"
	
	"github.com/rizome-dev/amq/pkg/queue"
	"github.com/rizome-dev/amq/pkg/types"
)

// defaultClient is the default implementation of the Client interface
type defaultClient struct {
	id         string
	manager    *queue.Manager
	subscriptions map[string]bool
	mu         sync.RWMutex
	closed     bool
	metadata   map[string]string
}

// NewClient creates a new client
func NewClient(id string, manager *queue.Manager, opts ...ClientOption) (Client, error) {
	if id == "" {
		return nil, errors.New("client ID cannot be empty")
	}
	
	config := &clientConfig{}
	for _, opt := range opts {
		opt(config)
	}
	
	c := &defaultClient{
		id:            id,
		manager:       manager,
		subscriptions: make(map[string]bool),
		metadata:      config.metadata,
	}
	
	// Register client with the system
	ctx := context.Background()
	client := types.NewClient(id)
	for k, v := range c.metadata {
		client.SetMetadata(k, v)
	}
	
	if err := manager.RegisterClient(ctx, client); err != nil {
		return nil, err
	}
	
	return c, nil
}

// ID returns the client identifier
func (c *defaultClient) ID() string {
	return c.id
}

// SubmitTask submits a task to a topic
func (c *defaultClient) SubmitTask(ctx context.Context, topic string, payload []byte, opts ...MessageOption) (*types.Message, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, errors.New("client is closed")
	}
	c.mu.RUnlock()
	
	// Convert MessageOption to queue.MessageOption
	queueOpts := make([]queue.MessageOption, len(opts))
	for i, opt := range opts {
		queueOpts[i] = queue.MessageOption(opt)
	}
	
	return c.manager.SubmitTask(ctx, c.id, topic, payload, queueOpts...)
}

// SendDirect sends a direct message to another client
func (c *defaultClient) SendDirect(ctx context.Context, to string, payload []byte, opts ...MessageOption) (*types.Message, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, errors.New("client is closed")
	}
	c.mu.RUnlock()
	
	// Convert MessageOption to queue.MessageOption
	queueOpts := make([]queue.MessageOption, len(opts))
	for i, opt := range opts {
		queueOpts[i] = queue.MessageOption(opt)
	}
	
	return c.manager.SendDirectMessage(ctx, c.id, to, payload, queueOpts...)
}

// Subscribe subscribes to one or more topics
func (c *defaultClient) Subscribe(ctx context.Context, topics ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.closed {
		return errors.New("client is closed")
	}
	
	for _, topic := range topics {
		if err := c.manager.SubscribeToTopic(ctx, c.id, topic); err != nil {
			return err
		}
		c.subscriptions[topic] = true
	}
	
	return nil
}

// Unsubscribe unsubscribes from one or more topics
func (c *defaultClient) Unsubscribe(ctx context.Context, topics ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.closed {
		return errors.New("client is closed")
	}
	
	for _, topic := range topics {
		if err := c.manager.UnsubscribeFromTopic(ctx, c.id, topic); err != nil {
			return err
		}
		delete(c.subscriptions, topic)
	}
	
	return nil
}

// Receive receives messages (blocking with timeout)
func (c *defaultClient) Receive(ctx context.Context, limit int) ([]*types.Message, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, errors.New("client is closed")
	}
	c.mu.RUnlock()
	
	return c.manager.ReceiveMessages(ctx, c.id, limit)
}

// Ack acknowledges successful message processing
func (c *defaultClient) Ack(ctx context.Context, messageID string) error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return errors.New("client is closed")
	}
	c.mu.RUnlock()
	
	return c.manager.AckMessage(ctx, c.id, messageID)
}

// Nack acknowledges failed message processing
func (c *defaultClient) Nack(ctx context.Context, messageID string, reason string) error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return errors.New("client is closed")
	}
	c.mu.RUnlock()
	
	return c.manager.NackMessage(ctx, c.id, messageID, reason)
}

// Close closes the client connection
func (c *defaultClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.closed {
		return nil
	}
	
	c.closed = true
	
	// Unregister client
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	return c.manager.UnregisterClient(ctx, c.id)
}

// asyncConsumer implements AsyncConsumer
type asyncConsumer struct {
	*defaultClient
	handler  MessageHandler
	opts     ConsumerOptions
	running  bool
	stopCh   chan struct{}
	done     chan struct{}
	wg       sync.WaitGroup
}

// NewAsyncConsumer creates a new asynchronous consumer
func NewAsyncConsumer(id string, manager *queue.Manager, opts ...ClientOption) (AsyncConsumer, error) {
	client, err := NewClient(id, manager, opts...)
	if err != nil {
		return nil, err
	}
	
	return &asyncConsumer{
		defaultClient: client.(*defaultClient),
		stopCh:        make(chan struct{}),
		done:          make(chan struct{}),
	}, nil
}

// Start starts asynchronous message consumption
func (ac *asyncConsumer) Start(handler MessageHandler, opts ConsumerOptions) error {
	ac.mu.Lock()
	if ac.running {
		ac.mu.Unlock()
		return errors.New("consumer already running")
	}
	
	ac.handler = handler
	ac.opts = opts
	ac.running = true
	ac.mu.Unlock()
	
	// Set defaults
	if ac.opts.BatchSize == 0 {
		ac.opts.BatchSize = 10
	}
	if ac.opts.MaxConcurrency == 0 {
		ac.opts.MaxConcurrency = 1
	}
	if ac.opts.PollInterval == 0 {
		ac.opts.PollInterval = 100 // 100ms
	}
	
	// Start worker goroutines
	for i := 0; i < ac.opts.MaxConcurrency; i++ {
		ac.wg.Add(1)
		go ac.worker()
	}
	
	return nil
}

// Stop stops asynchronous message consumption
func (ac *asyncConsumer) Stop() error {
	ac.mu.Lock()
	if !ac.running {
		ac.mu.Unlock()
		return nil
	}
	ac.running = false
	ac.mu.Unlock()
	
	// Signal workers to stop
	close(ac.stopCh)
	
	// Wait for workers to finish
	ac.wg.Wait()
	close(ac.done)
	
	return nil
}

// worker is the main worker loop
func (ac *asyncConsumer) worker() {
	defer ac.wg.Done()
	
	pollTimer := time.NewTimer(time.Duration(ac.opts.PollInterval) * time.Millisecond)
	defer pollTimer.Stop()
	
	for {
		select {
		case <-ac.stopCh:
			return
		case <-pollTimer.C:
			ac.processMessages()
			pollTimer.Reset(time.Duration(ac.opts.PollInterval) * time.Millisecond)
		}
	}
}

// processMessages fetches and processes messages
func (ac *asyncConsumer) processMessages() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	messages, err := ac.Receive(ctx, ac.opts.BatchSize)
	if err != nil || len(messages) == 0 {
		return
	}
	
	for _, msg := range messages {
		select {
		case <-ac.stopCh:
			return
		default:
		}
		
		ac.processMessage(msg)
	}
}

// processMessage processes a single message
func (ac *asyncConsumer) processMessage(msg *types.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	
	err := ac.handler(ctx, msg)
	
	if err != nil {
		_ = ac.Nack(ctx, msg.ID, err.Error())
	} else if ac.opts.AutoAck {
		_ = ac.Ack(ctx, msg.ID)
	}
}

// Message option implementations

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

// WithMessageMetadata adds metadata to the message
func WithMessageMetadata(key, value string) MessageOption {
	return func(m *types.Message) {
		if m.Metadata == nil {
			m.Metadata = make(map[string]string)
		}
		m.Metadata[key] = value
	}
}