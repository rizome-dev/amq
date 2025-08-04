package store

import (
	"context"
	"encoding/binary"
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

// MigrateLegacyQueues migrates existing queues from O(n) to O(1) format
// This should be called after opening the database to ensure backward compatibility
func (s *BadgerStore) MigrateLegacyQueues(ctx context.Context) error {
	return s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		
		// Find all legacy queue message keys
		prefix := []byte("queue:")
		
		var legacyQueues []string
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			keyStr := string(key)
			
			// Check if this is a legacy queue messages key
			if strings.HasSuffix(keyStr, ":msgs") && !strings.Contains(keyStr, ":items:") {
				// Extract queue name: queue:name:msgs -> name
				parts := strings.Split(keyStr, ":")
				if len(parts) >= 3 {
					queueName := strings.Join(parts[1:len(parts)-1], ":")
					legacyQueues = append(legacyQueues, queueName)
				}
			}
		}
		
		// Migrate each legacy queue
		for _, queueName := range legacyQueues {
			if err := s.migrateLegacyQueue(txn, queueName); err != nil {
				return fmt.Errorf("failed to migrate queue %s: %w", queueName, err)
			}
		}
		
		return nil
	})
}

// migrateLegacyQueue migrates a single queue from legacy format to new format
func (s *BadgerStore) migrateLegacyQueue(txn *badger.Txn, queueName string) error {
	legacyKey := queueMessagesKey(queueName)
	
	// Check if this queue has already been migrated
	_, err := txn.Get(queueHeadKey(queueName))
	if err == nil {
		// Already migrated, just delete the legacy key
		return txn.Delete(legacyKey)
	}
	
	// Get legacy queue data
	item, err := txn.Get(legacyKey)
	if err == badger.ErrKeyNotFound {
		return nil // Nothing to migrate
	} else if err != nil {
		return fmt.Errorf("failed to get legacy queue data: %w", err)
	}
	
	var msgIDs []string
	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &msgIDs)
	})
	if err != nil {
		return fmt.Errorf("failed to unmarshal legacy queue data: %w", err)
	}
	
	// Skip empty queues
	if len(msgIDs) == 0 {
		return txn.Delete(legacyKey)
	}
	
	// Create new queue structure
	headKey := queueHeadKey(queueName)
	tailKey := queueTailKey(queueName)
	
	// Set head and tail positions
	if err := txn.Set(headKey, uint64ToBytes(0)); err != nil {
		return fmt.Errorf("failed to set head position: %w", err)
	}
	if err := txn.Set(tailKey, uint64ToBytes(uint64(len(msgIDs)))); err != nil {
		return fmt.Errorf("failed to set tail position: %w", err)
	}
	
	// Store each message ID as individual items
	for i, msgID := range msgIDs {
		itemKey := queueItemKey(queueName, uint64(i))
		if err := txn.Set(itemKey, []byte(msgID)); err != nil {
			return fmt.Errorf("failed to store queue item: %w", err)
		}
	}
	
	// Delete legacy key
	return txn.Delete(legacyKey)
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

// New O(1) queue key functions
func queueHeadKey(name string) []byte {
	return []byte(fmt.Sprintf("queue:%s:head", name))
}

func queueTailKey(name string) []byte {
	return []byte(fmt.Sprintf("queue:%s:tail", name))
}

func queueItemKey(name string, seq uint64) []byte {
	return []byte(fmt.Sprintf("queue:%s:items:%016x", name, seq))
}

// Helper functions for queue sequence numbers
func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func bytesToUint64(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
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

// EnqueueMessage adds a message ID to a queue with O(1) complexity
func (s *BadgerStore) EnqueueMessage(ctx context.Context, queueName string, msgID string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// Get current tail position
		tailKey := queueTailKey(queueName)
		var tail uint64 = 0
		
		item, err := txn.Get(tailKey)
		if err == nil {
			err = item.Value(func(val []byte) error {
				tail = bytesToUint64(val)
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to read tail position: %w", err)
			}
		} else if err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to get tail position: %w", err)
		}
		
		// Store message at tail position
		itemKey := queueItemKey(queueName, tail)
		if err := txn.Set(itemKey, []byte(msgID)); err != nil {
			return fmt.Errorf("failed to store queue item: %w", err)
		}
		
		// Update tail position
		newTail := tail + 1
		if err := txn.Set(tailKey, uint64ToBytes(newTail)); err != nil {
			return fmt.Errorf("failed to update tail position: %w", err)
		}
		
		// Initialize head if this is the first item
		headKey := queueHeadKey(queueName)
		_, err = txn.Get(headKey)
		if err == badger.ErrKeyNotFound {
			if err := txn.Set(headKey, uint64ToBytes(0)); err != nil {
				return fmt.Errorf("failed to initialize head position: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("failed to check head position: %w", err)
		}
		
		return nil
	})
}

// DequeueMessage removes and returns the first message ID from a queue with O(1) complexity
func (s *BadgerStore) DequeueMessage(ctx context.Context, queueName string) (string, error) {
	var msgID string
	
	err := s.db.Update(func(txn *badger.Txn) error {
		// Get current head and tail positions
		headKey := queueHeadKey(queueName)
		tailKey := queueTailKey(queueName)
		
		var head, tail uint64
		
		// Get head position
		headItem, err := txn.Get(headKey)
		if err == badger.ErrKeyNotFound {
			return fmt.Errorf("queue empty")
		} else if err != nil {
			return fmt.Errorf("failed to get head position: %w", err)
		}
		
		err = headItem.Value(func(val []byte) error {
			head = bytesToUint64(val)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to read head position: %w", err)
		}
		
		// Get tail position
		tailItem, err := txn.Get(tailKey)
		if err == badger.ErrKeyNotFound {
			return fmt.Errorf("queue empty")
		} else if err != nil {
			return fmt.Errorf("failed to get tail position: %w", err)
		}
		
		err = tailItem.Value(func(val []byte) error {
			tail = bytesToUint64(val)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to read tail position: %w", err)
		}
		
		// Check if queue is empty
		if head >= tail {
			return fmt.Errorf("queue empty")
		}
		
		// Get message at head position
		itemKey := queueItemKey(queueName, head)
		item, err := txn.Get(itemKey)
		if err != nil {
			return fmt.Errorf("failed to get queue item: %w", err)
		}
		
		err = item.Value(func(val []byte) error {
			msgID = string(val)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to read queue item: %w", err)
		}
		
		// Remove the item and update head position
		if err := txn.Delete(itemKey); err != nil {
			return fmt.Errorf("failed to delete queue item: %w", err)
		}
		
		newHead := head + 1
		if err := txn.Set(headKey, uint64ToBytes(newHead)); err != nil {
			return fmt.Errorf("failed to update head position: %w", err)
		}
		
		// Clean up head/tail pointers if queue becomes empty
		if newHead >= tail {
			if err := txn.Delete(headKey); err != nil {
				return fmt.Errorf("failed to cleanup head position: %w", err)
			}
			if err := txn.Delete(tailKey); err != nil {
				return fmt.Errorf("failed to cleanup tail position: %w", err)
			}
		}
		
		return nil
	})
	
	return msgID, err
}

// PeekQueue returns message IDs from queue without removing them with O(1) for size check + O(limit) for reading
func (s *BadgerStore) PeekQueue(ctx context.Context, queueName string, limit int) ([]string, error) {
	var msgIDs []string
	
	err := s.db.View(func(txn *badger.Txn) error {
		// Get current head and tail positions
		headKey := queueHeadKey(queueName)
		tailKey := queueTailKey(queueName)
		
		var head, tail uint64
		
		// Get head position
		headItem, err := txn.Get(headKey)
		if err == badger.ErrKeyNotFound {
			return nil // Empty queue
		} else if err != nil {
			return fmt.Errorf("failed to get head position: %w", err)
		}
		
		err = headItem.Value(func(val []byte) error {
			head = bytesToUint64(val)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to read head position: %w", err)
		}
		
		// Get tail position
		tailItem, err := txn.Get(tailKey)
		if err == badger.ErrKeyNotFound {
			return nil // Empty queue
		} else if err != nil {
			return fmt.Errorf("failed to get tail position: %w", err)
		}
		
		err = tailItem.Value(func(val []byte) error {
			tail = bytesToUint64(val)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to read tail position: %w", err)
		}
		
		// Check if queue is empty
		if head >= tail {
			return nil // Empty queue
		}
		
		// Calculate how many messages to read
		queueSize := int(tail - head)
		readCount := queueSize
		if limit > 0 && limit < queueSize {
			readCount = limit
		}
		
		// Read messages from head position
		msgIDs = make([]string, readCount)
		for i := 0; i < readCount; i++ {
			itemKey := queueItemKey(queueName, head+uint64(i))
			item, err := txn.Get(itemKey)
			if err != nil {
				return fmt.Errorf("failed to get queue item at position %d: %w", head+uint64(i), err)
			}
			
			err = item.Value(func(val []byte) error {
				msgIDs[i] = string(val)
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to read queue item at position %d: %w", head+uint64(i), err)
			}
		}
		
		return nil
	})
	
	return msgIDs, err
}

// GetQueueSize returns the number of messages in a queue with O(1) complexity
func (s *BadgerStore) GetQueueSize(ctx context.Context, queueName string) (int64, error) {
	var size int64
	
	err := s.db.View(func(txn *badger.Txn) error {
		// Get current head and tail positions
		headKey := queueHeadKey(queueName)
		tailKey := queueTailKey(queueName)
		
		var head, tail uint64
		
		// Get head position
		headItem, err := txn.Get(headKey)
		if err == badger.ErrKeyNotFound {
			return nil // Empty queue
		} else if err != nil {
			return fmt.Errorf("failed to get head position: %w", err)
		}
		
		err = headItem.Value(func(val []byte) error {
			head = bytesToUint64(val)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to read head position: %w", err)
		}
		
		// Get tail position
		tailItem, err := txn.Get(tailKey)
		if err == badger.ErrKeyNotFound {
			return nil // Empty queue
		} else if err != nil {
			return fmt.Errorf("failed to get tail position: %w", err)
		}
		
		err = tailItem.Value(func(val []byte) error {
			tail = bytesToUint64(val)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to read tail position: %w", err)
		}
		
		// Calculate queue size
		if tail > head {
			size = int64(tail - head)
		}
		
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
		if err := txn.Delete(queueKey(name)); err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to delete queue metadata: %w", err)
		}
		
		// Delete legacy queue messages (for backward compatibility)
		if err := txn.Delete(queueMessagesKey(name)); err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to delete legacy queue messages: %w", err)
		}
		
		// Delete new queue structure: head, tail, and all items
		headKey := queueHeadKey(name)
		tailKey := queueTailKey(name)
		
		var head, tail uint64
		
		// Get head and tail positions to know what to delete
		headItem, err := txn.Get(headKey)
		if err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to get head position: %w", err)
		}
		if err == nil {
			err = headItem.Value(func(val []byte) error {
				head = bytesToUint64(val)
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to read head position: %w", err)
			}
		}
		
		tailItem, err := txn.Get(tailKey)
		if err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to get tail position: %w", err)
		}
		if err == nil {
			err = tailItem.Value(func(val []byte) error {
				tail = bytesToUint64(val)
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to read tail position: %w", err)
			}
			
			// Delete all queue items
			for seq := head; seq < tail; seq++ {
				itemKey := queueItemKey(name, seq)
				if err := txn.Delete(itemKey); err != nil && err != badger.ErrKeyNotFound {
					return fmt.Errorf("failed to delete queue item %d: %w", seq, err)
				}
			}
		}
		
		// Delete head and tail keys
		if err := txn.Delete(headKey); err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to delete head key: %w", err)
		}
		if err := txn.Delete(tailKey); err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to delete tail key: %w", err)
		}
		
		return nil
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
	// Get current tail position
	tailKey := queueTailKey(queueName)
	var tail uint64 = 0
	
	item, err := t.txn.Get(tailKey)
	if err == nil {
		err = item.Value(func(val []byte) error {
			tail = bytesToUint64(val)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to read tail position: %w", err)
		}
	} else if err != badger.ErrKeyNotFound {
		return fmt.Errorf("failed to get tail position: %w", err)
	}
	
	// Store message at tail position
	itemKey := queueItemKey(queueName, tail)
	if err := t.txn.Set(itemKey, []byte(msgID)); err != nil {
		return fmt.Errorf("failed to store queue item: %w", err)
	}
	
	// Update tail position
	newTail := tail + 1
	if err := t.txn.Set(tailKey, uint64ToBytes(newTail)); err != nil {
		return fmt.Errorf("failed to update tail position: %w", err)
	}
	
	// Initialize head if this is the first item
	headKey := queueHeadKey(queueName)
	_, err = t.txn.Get(headKey)
	if err == badger.ErrKeyNotFound {
		if err := t.txn.Set(headKey, uint64ToBytes(0)); err != nil {
			return fmt.Errorf("failed to initialize head position: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to check head position: %w", err)
	}
	
	return nil
}

func (t *badgerTransaction) DequeueMessage(queueName string) (string, error) {
	// Get current head and tail positions
	headKey := queueHeadKey(queueName)
	tailKey := queueTailKey(queueName)
	
	var head, tail uint64
	
	// Get head position
	headItem, err := t.txn.Get(headKey)
	if err == badger.ErrKeyNotFound {
		return "", fmt.Errorf("queue empty")
	} else if err != nil {
		return "", fmt.Errorf("failed to get head position: %w", err)
	}
	
	err = headItem.Value(func(val []byte) error {
		head = bytesToUint64(val)
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("failed to read head position: %w", err)
	}
	
	// Get tail position
	tailItem, err := t.txn.Get(tailKey)
	if err == badger.ErrKeyNotFound {
		return "", fmt.Errorf("queue empty")
	} else if err != nil {
		return "", fmt.Errorf("failed to get tail position: %w", err)
	}
	
	err = tailItem.Value(func(val []byte) error {
		tail = bytesToUint64(val)
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("failed to read tail position: %w", err)
	}
	
	// Check if queue is empty
	if head >= tail {
		return "", fmt.Errorf("queue empty")
	}
	
	// Get message at head position
	itemKey := queueItemKey(queueName, head)
	item, err := t.txn.Get(itemKey)
	if err != nil {
		return "", fmt.Errorf("failed to get queue item: %w", err)
	}
	
	var msgID string
	err = item.Value(func(val []byte) error {
		msgID = string(val)
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("failed to read queue item: %w", err)
	}
	
	// Remove the item and update head position
	if err := t.txn.Delete(itemKey); err != nil {
		return "", fmt.Errorf("failed to delete queue item: %w", err)
	}
	
	newHead := head + 1
	if err := t.txn.Set(headKey, uint64ToBytes(newHead)); err != nil {
		return "", fmt.Errorf("failed to update head position: %w", err)
	}
	
	// Clean up head/tail pointers if queue becomes empty
	if newHead >= tail {
		if err := t.txn.Delete(headKey); err != nil {
			return "", fmt.Errorf("failed to cleanup head position: %w", err)
		}
		if err := t.txn.Delete(tailKey); err != nil {
			return "", fmt.Errorf("failed to cleanup tail position: %w", err)
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