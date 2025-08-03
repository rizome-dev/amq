package types

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// GenerateMessageID generates a unique message ID
func GenerateMessageID() string {
	// Generate timestamp prefix for natural sorting
	timestamp := time.Now().UnixNano()
	
	// Generate random suffix
	randomBytes := make([]byte, 8)
	_, err := rand.Read(randomBytes)
	if err != nil {
		// Fallback to timestamp only
		return fmt.Sprintf("msg_%d", timestamp)
	}
	
	return fmt.Sprintf("msg_%d_%s", timestamp, hex.EncodeToString(randomBytes))
}

// GenerateAgentID generates a unique agent ID
func GenerateAgentID() string {
	randomBytes := make([]byte, 16)
	_, err := rand.Read(randomBytes)
	if err != nil {
		// Fallback to timestamp
		return fmt.Sprintf("agent_%d", time.Now().UnixNano())
	}
	
	return fmt.Sprintf("agent_%s", hex.EncodeToString(randomBytes))
}

// GenerateQueueID generates a unique queue ID
func GenerateQueueID(name string) string {
	return fmt.Sprintf("queue_%s", name)
}