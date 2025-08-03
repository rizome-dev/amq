package queue

import (
	"context"
	"sync"
	"time"
	
	"github.com/rizome-dev/amq/pkg/types"
)

// processor handles message processing for a queue
type processor struct {
	queueName string
	manager   *Manager
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	notifyCh  chan struct{}
}

// newProcessor creates a new processor
func newProcessor(queueName string, manager *Manager) *processor {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &processor{
		queueName: queueName,
		manager:   manager,
		ctx:       ctx,
		cancel:    cancel,
		notifyCh:  make(chan struct{}, 1),
	}
}

// start starts the processor
func (p *processor) start(workers int) {
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

// stop stops the processor
func (p *processor) stop() {
	p.cancel()
	p.wg.Wait()
}

// notify notifies the processor of new messages
func (p *processor) notify() {
	select {
	case p.notifyCh <- struct{}{}:
	default:
		// Channel is full, notification already pending
	}
}

// worker is the main processing loop
func (p *processor) worker() {
	defer p.wg.Done()
	
	// Initial check for messages
	p.processMessages()
	
	// Polling interval when no messages
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-p.ctx.Done():
			return
			
		case <-p.notifyCh:
			// New messages available
			p.processMessages()
			
		case <-ticker.C:
			// Periodic check
			p.processMessages()
		}
	}
}

// processMessages processes available messages
func (p *processor) processMessages() {
	for {
		// Check if context is cancelled
		select {
		case <-p.ctx.Done():
			return
		default:
		}
		
		// Dequeue a message
		msgID, err := p.manager.store.DequeueMessage(p.ctx, p.queueName)
		if err != nil {
			// No more messages or error
			return
		}
		
		// Get message details
		msg, err := p.manager.store.GetMessage(p.ctx, msgID)
		if err != nil {
			// Log error and continue
			continue
		}
		
		// Check if message has expired
		if msg.IsExpired() {
			msg.Status = types.MessageStatusExpired
			_ = p.manager.store.UpdateMessage(p.ctx, msg)
			continue
		}
		
		// Find available agent to process
		// This is a simplified version - in production you'd want
		// more sophisticated agent selection based on capabilities,
		// load balancing, etc.
		
		// For task messages, find a subscribed agent
		if msg.Type == types.MessageTypeTask {
			p.manager.queuesMutex.RLock()
			queue, exists := p.manager.queues[p.queueName]
			p.manager.queuesMutex.RUnlock()
			
			if !exists || len(queue.Subscribers) == 0 {
				// No subscribers, re-enqueue
				_ = p.manager.store.EnqueueMessage(p.ctx, p.queueName, msgID)
				return
			}
			
			// Simple round-robin selection
			// In production, you'd want to check agent status, load, etc.
			agentID := queue.Subscribers[0]
			
			// Mark message as assigned to agent
			msg.Status = types.MessageStatusPending
			msg.Metadata["assigned_to"] = agentID
			_ = p.manager.store.UpdateMessage(p.ctx, msg)
			
			// Enqueue to agent's inbox
			_ = p.manager.store.EnqueueMessage(p.ctx, agentID, msgID)
		}
		
		// For direct messages, they're already in the right queue
		// so nothing more to do here
	}
}