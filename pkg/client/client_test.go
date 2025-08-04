package client

import (
	"os"
	"testing"
	"github.com/rizome-dev/amq/pkg/queue"
	"github.com/rizome-dev/amq/pkg/store"
)

func TestNewClient(t *testing.T) {
	// Create temporary directory for test database
	tmpDir, err := os.MkdirTemp("", "client_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a mock store and manager for testing
	mockStore := store.NewBadgerStore()
	if err := mockStore.Open(tmpDir); err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer mockStore.Close()

	manager := queue.NewManager(mockStore, queue.DefaultConfig())
	if err := manager.Start(); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	client, err := NewClient("test-client", manager)
	if err != nil {
		t.Errorf("NewClient() returned error: %v", err)
	}
	if client == nil {
		t.Error("NewClient() returned nil")
	}
}
