package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	
	"github.com/dgraph-io/badger/v4"
	"github.com/rizome-dev/amq/pkg/types"
)

// BadgerStore implements Store interface using BadgerDB
type BadgerStore struct {
	db *badger.DB
}

// NewBadgerStore creates a new BadgerDB store
func NewBadgerStore() *BadgerStore {
	return &BadgerStore{}
}

// Open initializes the BadgerDB connection
func (s *BadgerStore) Open(path string) error {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil // Disable badger logging for now
	
	db, err := badger.Open(opts)
	if err != nil {
		return fmt.Errorf("failed to open badger db: %w", err)
	}
	
	s.db = db
	
	// Start garbage collection
	go s.runGC()
	
	return nil
}

// Close closes the BadgerDB connection
func (s *BadgerStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// runGC runs garbage collection periodically
func (s *BadgerStore) runGC() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	
	for range ticker.C {
		if s.db == nil {
			return
		}
		
		err := s.db.RunValueLogGC(0.5)
		if err != nil && err != badger.ErrNoRewrite {
			// Log error but don't stop GC
			continue
		}
	}
}

// Key generation functions
func messageKey(id string) []byte {
	return []byte(fmt.Sprintf("msg:%s", id))
}

func queueKey(name string) []byte {
	return []byte(fmt.Sprintf("queue:%s:meta", name))
}

func queueMessagesKey(name string) []byte {
	return []byte(fmt.Sprintf("queue:%s:msgs", name))
}

func clientKey(id string) []byte {
	return []byte(fmt.Sprintf("client:%s", id))
}

func clientInboxKey(id string) []byte {
	return []byte(fmt.Sprintf("client:%s:inbox", id))
}

func subscriptionKey(clientID, topic string) []byte {
	return []byte(fmt.Sprintf("sub:%s:%s", clientID, topic))
}

func topicSubscribersKey(topic string) []byte {
	return []byte(fmt.Sprintf("topic:%s:subs", topic))
}

func indexKey(indexName, key string) []byte {
	return []byte(fmt.Sprintf("idx:%s:%s", indexName, key))
}

// SaveMessage saves a message to the store
func (s *BadgerStore) SaveMessage(ctx context.Context, msg *types.Message) error {
	return s.db.Update(func(txn *badger.Txn) error {
		data, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("failed to marshal message: %w", err)
		}
		
		if err := txn.Set(messageKey(msg.ID), data); err != nil {
			return fmt.Errorf("failed to save message: %w", err)
		}
		
		// Add to indexes
		tx := &badgerTransaction{txn: txn}
		
		// Index by status
		if err := tx.AddToIndex(fmt.Sprintf("status:%s", msg.Status), "msgs", msg.ID); err != nil {
			return err
		}
		
		// Index by topic (for task messages)
		if msg.Type == types.MessageTypeTask && msg.Topic != "" {
			if err := tx.AddToIndex(fmt.Sprintf("topic:%s", msg.Topic), "msgs", msg.ID); err != nil {
				return err
			}
		}
		
		// Index by client (for direct messages)
		if msg.Type == types.MessageTypeDirect && msg.To != "" {
			if err := tx.AddToIndex(fmt.Sprintf("client:%s", msg.To), "tasks", msg.ID); err != nil {
				return err
			}
		}
		
		return nil
	})
}

// GetMessage retrieves a message by ID
func (s *BadgerStore) GetMessage(ctx context.Context, id string) (*types.Message, error) {
	var msg types.Message
	
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(messageKey(id))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("message not found: %s", id)
			}
			return err
		}
		
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &msg)
		})
	})
	
	if err != nil {
		return nil, err
	}
	
	return &msg, nil
}

// UpdateMessage updates an existing message
func (s *BadgerStore) UpdateMessage(ctx context.Context, msg *types.Message) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// Get old message for index cleanup
		oldMsg, err := s.GetMessage(ctx, msg.ID)
		if err != nil {
			return err
		}
		
		tx := &badgerTransaction{txn: txn}
		
		// Remove from old status index
		if oldMsg.Status != msg.Status {
			if err := tx.RemoveFromIndex(fmt.Sprintf("status:%s", oldMsg.Status), "msgs", msg.ID); err != nil {
				return err
			}
			if err := tx.AddToIndex(fmt.Sprintf("status:%s", msg.Status), "msgs", msg.ID); err != nil {
				return err
			}
		}
		
		// Save updated message
		data, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("failed to marshal message: %w", err)
		}
		
		return txn.Set(messageKey(msg.ID), data)
	})
}

// DeleteMessage deletes a message
func (s *BadgerStore) DeleteMessage(ctx context.Context, id string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// Get message for index cleanup
		msg, err := s.GetMessage(ctx, id)
		if err != nil {
			return err
		}
		
		tx := &badgerTransaction{txn: txn}
		
		// Remove from indexes
		if err := tx.RemoveFromIndex(fmt.Sprintf("status:%s", msg.Status), "msgs", msg.ID); err != nil {
			return err
		}
		
		if msg.Type == types.MessageTypeTask && msg.Topic != "" {
			if err := tx.RemoveFromIndex(fmt.Sprintf("topic:%s", msg.Topic), "msgs", msg.ID); err != nil {
				return err
			}
		}
		
		if msg.Type == types.MessageTypeDirect && msg.To != "" {
			if err := tx.RemoveFromIndex(fmt.Sprintf("client:%s", msg.To), "tasks", msg.ID); err != nil {
				return err
			}
		}
		
		// Delete message
		return txn.Delete(messageKey(id))
	})
}

// ListMessages lists messages based on filter
func (s *BadgerStore) ListMessages(ctx context.Context, filter MessageFilter) ([]*types.Message, error) {
	messages := make([]*types.Message, 0)
	
	err := s.db.View(func(txn *badger.Txn) error {
		// If filtering by status, use index
		if filter.Status != nil {
			msgIDs, err := s.GetFromIndex(ctx, fmt.Sprintf("status:%s", *filter.Status), "msgs")
			if err != nil {
				return err
			}
			
			for _, id := range msgIDs {
				msg, err := s.GetMessage(ctx, id)
				if err != nil {
					continue
				}
				
				if s.matchesFilter(msg, filter) {
					messages = append(messages, msg)
					if filter.Limit > 0 && len(messages) >= filter.Limit {
						break
					}
				}
			}
			
			return nil
		}
		
		// Otherwise scan all messages
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		
		prefix := []byte("msg:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			
			var msg types.Message
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &msg)
			})
			if err != nil {
				continue
			}
			
			if s.matchesFilter(&msg, filter) {
				messages = append(messages, &msg)
				if filter.Limit > 0 && len(messages) >= filter.Limit {
					break
				}
			}
		}
		
		return nil
	})
	
	return messages, err
}

// matchesFilter checks if a message matches the filter criteria
func (s *BadgerStore) matchesFilter(msg *types.Message, filter MessageFilter) bool {
	if filter.Status != nil && msg.Status != *filter.Status {
		return false
	}
	
	if filter.Topic != "" && msg.Topic != filter.Topic {
		return false
	}
	
	if filter.ClientID != "" && msg.From != filter.ClientID && msg.To != filter.ClientID {
		return false
	}
	
	if filter.Since != nil && msg.CreatedAt.Before(*filter.Since) {
		return false
	}
	
	if filter.Until != nil && msg.CreatedAt.After(*filter.Until) {
		return false
	}
	
	return true
}

// EnqueueMessage adds a message ID to a queue
func (s *BadgerStore) EnqueueMessage(ctx context.Context, queueName string, msgID string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		key := queueMessagesKey(queueName)
		
		// Get current queue
		var msgIDs []string
		item, err := txn.Get(key)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		
		if err == nil {
			err = item.Value(func(val []byte) error {
				return json.Unmarshal(val, &msgIDs)
			})
			if err != nil {
				return err
			}
		}
		
		// Append message ID
		msgIDs = append(msgIDs, msgID)
		
		// Save updated queue
		data, err := json.Marshal(msgIDs)
		if err != nil {
			return err
		}
		
		return txn.Set(key, data)
	})
}

// DequeueMessage removes and returns the first message ID from a queue
func (s *BadgerStore) DequeueMessage(ctx context.Context, queueName string) (string, error) {
	var msgID string
	
	err := s.db.Update(func(txn *badger.Txn) error {
		key := queueMessagesKey(queueName)
		
		// Get current queue
		var msgIDs []string
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("queue empty")
			}
			return err
		}
		
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &msgIDs)
		})
		if err != nil {
			return err
		}
		
		if len(msgIDs) == 0 {
			return fmt.Errorf("queue empty")
		}
		
		// Get first message ID
		msgID = msgIDs[0]
		msgIDs = msgIDs[1:]
		
		// Save updated queue
		if len(msgIDs) == 0 {
			return txn.Delete(key)
		}
		
		data, err := json.Marshal(msgIDs)
		if err != nil {
			return err
		}
		
		return txn.Set(key, data)
	})
	
	return msgID, err
}

// PeekQueue returns message IDs from queue without removing them
func (s *BadgerStore) PeekQueue(ctx context.Context, queueName string, limit int) ([]string, error) {
	var msgIDs []string
	
	err := s.db.View(func(txn *badger.Txn) error {
		key := queueMessagesKey(queueName)
		
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil // Empty queue
			}
			return err
		}
		
		var allIDs []string
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &allIDs)
		})
		if err != nil {
			return err
		}
		
		// Return up to limit IDs
		if limit > 0 && len(allIDs) > limit {
			msgIDs = allIDs[:limit]
		} else {
			msgIDs = allIDs
		}
		
		return nil
	})
	
	return msgIDs, err
}

// GetQueueSize returns the number of messages in a queue
func (s *BadgerStore) GetQueueSize(ctx context.Context, queueName string) (int64, error) {
	var size int64
	
	err := s.db.View(func(txn *badger.Txn) error {
		key := queueMessagesKey(queueName)
		
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil // Empty queue
			}
			return err
		}
		
		var msgIDs []string
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &msgIDs)
		})
		if err != nil {
			return err
		}
		
		size = int64(len(msgIDs))
		return nil
	})
	
	return size, err
}

// SaveClient saves a client to the store
func (s *BadgerStore) SaveClient(ctx context.Context, client *types.Client) error {
	return s.db.Update(func(txn *badger.Txn) error {
		data, err := json.Marshal(client)
		if err != nil {
			return fmt.Errorf("failed to marshal client: %w", err)
		}
		
		return txn.Set(clientKey(client.ID), data)
	})
}

// GetClient retrieves a client by ID
func (s *BadgerStore) GetClient(ctx context.Context, id string) (*types.Client, error) {
	var client types.Client
	
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(clientKey(id))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("client not found: %s", id)
			}
			return err
		}
		
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &client)
		})
	})
	
	if err != nil {
		return nil, err
	}
	
	return &client, nil
}

// UpdateClient updates an existing client
func (s *BadgerStore) UpdateClient(ctx context.Context, client *types.Client) error {
	return s.SaveClient(ctx, client)
}

// DeleteClient deletes a client
func (s *BadgerStore) DeleteClient(ctx context.Context, id string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// Delete client
		if err := txn.Delete(clientKey(id)); err != nil {
			return err
		}
		
		// Delete client's inbox
		return txn.Delete(clientInboxKey(id))
	})
}

// ListClients lists clients based on filter
func (s *BadgerStore) ListClients(ctx context.Context, filter ClientFilter) ([]*types.Client, error) {
	clients := make([]*types.Client, 0)
	
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		
		prefix := []byte("client:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			// Skip inbox keys
			key := string(it.Item().Key())
			if strings.Contains(key, ":inbox") {
				continue
			}
			
			item := it.Item()
			var client types.Client
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &client)
			})
			if err != nil {
				continue
			}
			
			// Apply filters
			if filter.MetadataKey != "" && filter.MetadataValue != "" {
				val, exists := client.GetMetadata(filter.MetadataKey)
				if !exists || val != filter.MetadataValue {
					continue
				}
			}
			
			clients = append(clients, &client)
			
			if filter.Limit > 0 && len(clients) >= filter.Limit {
				break
			}
		}
		
		return nil
	})
	
	return clients, err
}

// AddSubscription adds a subscription for a client to a topic
func (s *BadgerStore) AddSubscription(ctx context.Context, clientID, topic string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// Add subscription record
		sub := types.NewSubscription(clientID, topic)
		data, err := json.Marshal(sub)
		if err != nil {
			return err
		}
		
		return txn.Set(subscriptionKey(clientID, topic), data)
	})
}

// RemoveSubscription removes a subscription
func (s *BadgerStore) RemoveSubscription(ctx context.Context, clientID, topic string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(subscriptionKey(clientID, topic))
	})
}

// GetSubscriptions gets all topics a client is subscribed to
func (s *BadgerStore) GetSubscriptions(ctx context.Context, clientID string) ([]string, error) {
	topics := make([]string, 0)
	
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		
		prefix := []byte(fmt.Sprintf("sub:%s:", clientID))
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := string(it.Item().Key())
			// Extract topic from key: sub:clientID:topic
			parts := strings.Split(key, ":")
			if len(parts) >= 3 {
				topics = append(topics, parts[2])
			}
		}
		
		return nil
	})
	
	return topics, err
}

// GetSubscribers gets all clients subscribed to a topic
func (s *BadgerStore) GetSubscribers(ctx context.Context, topic string) ([]string, error) {
	clients := make([]string, 0)
	
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		
		prefix := []byte("sub:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := string(it.Item().Key())
			// Check if this subscription is for our topic
			if strings.HasSuffix(key, ":"+topic) {
				// Extract clientID from key: sub:clientID:topic
				parts := strings.Split(key, ":")
				if len(parts) >= 3 {
					clients = append(clients, parts[1])
				}
			}
		}
		
		return nil
	})
	
	return clients, err
}

// SaveQueue saves queue metadata
func (s *BadgerStore) SaveQueue(ctx context.Context, queue *types.Queue) error {
	return s.db.Update(func(txn *badger.Txn) error {
		data, err := json.Marshal(queue)
		if err != nil {
			return fmt.Errorf("failed to marshal queue: %w", err)
		}
		
		return txn.Set(queueKey(queue.Name), data)
	})
}

// GetQueue retrieves queue metadata
func (s *BadgerStore) GetQueue(ctx context.Context, name string) (*types.Queue, error) {
	var queue types.Queue
	
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(queueKey(name))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("queue not found: %s", name)
			}
			return err
		}
		
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &queue)
		})
	})
	
	if err != nil {
		return nil, err
	}
	
	return &queue, nil
}

// UpdateQueue updates queue metadata
func (s *BadgerStore) UpdateQueue(ctx context.Context, queue *types.Queue) error {
	return s.SaveQueue(ctx, queue)
}

// DeleteQueue deletes a queue and all its messages
func (s *BadgerStore) DeleteQueue(ctx context.Context, name string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// Delete queue metadata
		if err := txn.Delete(queueKey(name)); err != nil {
			return err
		}
		
		// Delete queue messages
		return txn.Delete(queueMessagesKey(name))
	})
}

// ListQueues lists all queues
func (s *BadgerStore) ListQueues(ctx context.Context) ([]*types.Queue, error) {
	queues := make([]*types.Queue, 0)
	
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		
		prefix := []byte("queue:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			// Only process metadata keys
			key := string(it.Item().Key())
			if !strings.HasSuffix(key, ":meta") {
				continue
			}
			
			item := it.Item()
			var queue types.Queue
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &queue)
			})
			if err != nil {
				continue
			}
			
			queues = append(queues, &queue)
		}
		
		return nil
	})
	
	return queues, err
}

// AddToIndex adds a value to an index
func (s *BadgerStore) AddToIndex(ctx context.Context, indexName, key, value string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		indexKey := indexKey(indexName, key)
		
		// Get current values
		var values []string
		item, err := txn.Get(indexKey)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		
		if err == nil {
			err = item.Value(func(val []byte) error {
				return json.Unmarshal(val, &values)
			})
			if err != nil {
				return err
			}
		}
		
		// Check if value already exists
		for _, v := range values {
			if v == value {
				return nil // Already indexed
			}
		}
		
		// Add value
		values = append(values, value)
		
		// Save updated index
		data, err := json.Marshal(values)
		if err != nil {
			return err
		}
		
		return txn.Set(indexKey, data)
	})
}

// RemoveFromIndex removes a value from an index
func (s *BadgerStore) RemoveFromIndex(ctx context.Context, indexName, key, value string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		indexKey := indexKey(indexName, key)
		
		// Get current values
		var values []string
		item, err := txn.Get(indexKey)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil // Index doesn't exist
			}
			return err
		}
		
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &values)
		})
		if err != nil {
			return err
		}
		
		// Remove value
		newValues := make([]string, 0, len(values))
		for _, v := range values {
			if v != value {
				newValues = append(newValues, v)
			}
		}
		
		// If no values left, delete the index
		if len(newValues) == 0 {
			return txn.Delete(indexKey)
		}
		
		// Save updated index
		data, err := json.Marshal(newValues)
		if err != nil {
			return err
		}
		
		return txn.Set(indexKey, data)
	})
}

// GetFromIndex retrieves values from an index
func (s *BadgerStore) GetFromIndex(ctx context.Context, indexName, key string) ([]string, error) {
	var values []string
	
	err := s.db.View(func(txn *badger.Txn) error {
		indexKey := indexKey(indexName, key)
		
		item, err := txn.Get(indexKey)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil // Empty index
			}
			return err
		}
		
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &values)
		})
	})
	
	return values, err
}

// RunInTransaction runs a function within a transaction
func (s *BadgerStore) RunInTransaction(ctx context.Context, fn func(tx Transaction) error) error {
	return s.db.Update(func(txn *badger.Txn) error {
		tx := &badgerTransaction{txn: txn, store: s, ctx: ctx}
		return fn(tx)
	})
}

// badgerTransaction implements the Transaction interface
type badgerTransaction struct {
	txn   *badger.Txn
	store *BadgerStore
	ctx   context.Context
}

func (t *badgerTransaction) SaveMessage(msg *types.Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}
	
	return t.txn.Set(messageKey(msg.ID), data)
}

func (t *badgerTransaction) UpdateMessage(msg *types.Message) error {
	return t.SaveMessage(msg)
}

func (t *badgerTransaction) DeleteMessage(id string) error {
	return t.txn.Delete(messageKey(id))
}

func (t *badgerTransaction) EnqueueMessage(queueName string, msgID string) error {
	key := queueMessagesKey(queueName)
	
	// Get current queue
	var msgIDs []string
	item, err := t.txn.Get(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return err
	}
	
	if err == nil {
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &msgIDs)
		})
		if err != nil {
			return err
		}
	}
	
	// Append message ID
	msgIDs = append(msgIDs, msgID)
	
	// Save updated queue
	data, err := json.Marshal(msgIDs)
	if err != nil {
		return err
	}
	
	return t.txn.Set(key, data)
}

func (t *badgerTransaction) DequeueMessage(queueName string) (string, error) {
	key := queueMessagesKey(queueName)
	
	// Get current queue
	var msgIDs []string
	item, err := t.txn.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return "", fmt.Errorf("queue empty")
		}
		return "", err
	}
	
	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &msgIDs)
	})
	if err != nil {
		return "", err
	}
	
	if len(msgIDs) == 0 {
		return "", fmt.Errorf("queue empty")
	}
	
	// Get first message ID
	msgID := msgIDs[0]
	msgIDs = msgIDs[1:]
	
	// Save updated queue
	if len(msgIDs) == 0 {
		if err := t.txn.Delete(key); err != nil {
			return "", err
		}
	} else {
		data, err := json.Marshal(msgIDs)
		if err != nil {
			return "", err
		}
		
		if err := t.txn.Set(key, data); err != nil {
			return "", err
		}
	}
	
	return msgID, nil
}

func (t *badgerTransaction) SaveClient(client *types.Client) error {
	data, err := json.Marshal(client)
	if err != nil {
		return fmt.Errorf("failed to marshal client: %w", err)
	}
	
	return t.txn.Set(clientKey(client.ID), data)
}

func (t *badgerTransaction) UpdateClient(client *types.Client) error {
	return t.SaveClient(client)
}

func (t *badgerTransaction) AddToIndex(indexName, key, value string) error {
	indexKey := indexKey(indexName, key)
	
	// Get current values
	var values []string
	item, err := t.txn.Get(indexKey)
	if err != nil && err != badger.ErrKeyNotFound {
		return err
	}
	
	if err == nil {
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &values)
		})
		if err != nil {
			return err
		}
	}
	
	// Check if value already exists
	for _, v := range values {
		if v == value {
			return nil // Already indexed
		}
	}
	
	// Add value
	values = append(values, value)
	
	// Save updated index
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}
	
	return t.txn.Set(indexKey, data)
}

func (t *badgerTransaction) RemoveFromIndex(indexName, key, value string) error {
	indexKey := indexKey(indexName, key)
	
	// Get current values
	var values []string
	item, err := t.txn.Get(indexKey)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil // Index doesn't exist
		}
		return err
	}
	
	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &values)
	})
	if err != nil {
		return err
	}
	
	// Remove value
	newValues := make([]string, 0, len(values))
	for _, v := range values {
		if v != value {
			newValues = append(newValues, v)
		}
	}
	
	// If no values left, delete the index
	if len(newValues) == 0 {
		return t.txn.Delete(indexKey)
	}
	
	// Save updated index
	data, err := json.Marshal(newValues)
	if err != nil {
		return err
	}
	
	return t.txn.Set(indexKey, data)
}