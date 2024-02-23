package registry

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/feast-dev/feast/go/protos/feast/core"
)

// TestNewHttpRegistryStore tests the NewHttpRegistryStore function.
func TestNewHttpRegistryStore(t *testing.T) {
	// Mock server
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mockServer.Close()

	// Configure the test
	config := &registry.RegistryConfig{
		Path:     mockServer.URL,
		ClientId: "test-client",
	}
	project := "test-project"

	// Test NewHttpRegistryStore with a valid configuration
	_, err := registry.NewHttpRegistryStore(config, project)
	if err != nil {
		t.Errorf("Expected no error, but got: %v", err)
	}

	// Test NewHttpRegistryStore with an invalid configuration (simulating connection error)
	config.Path = "invalid-url"
	_, err = registry.NewHttpRegistryStore(config, project)
	if err == nil {
		t.Error("Expected an error, but got nil")
	}
}

// TestHttpRegistryStore_LoadEntities tests the LoadEntities method.
func TestHttpRegistryStore_LoadEntities(t *testing.T) {
	// Mock server
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mockServer.Close()

	// Create HttpRegistryStore with mock server configuration
	hrs := &registry.HttpRegistryStore{
		project:  "test-project",
		endpoint: mockServer.URL,
		clientId: "test-client",
		client:   http.Client{},
	}

	// Mock protobuf data
	mockData := []byte("mock protobuf data")

	// Mock response
	mockResponse := &core.EntityList{}
	err := core.Unmarshal(mockData, mockResponse)
	if err != nil {
		t.Fatalf("Failed to unmarshal mock response: %v", err)
	}

	// Mock loadProtobufMessages
	hrs.LoadProtobufMessages = func(url string, messageProcessor func([]byte) error) error {
		return messageProcessor(mockData)
	}

	// Test LoadEntities
	registry := &core.Registry{}
	err = hrs.LoadEntities(registry)
	if err != nil {
		t.Errorf("Expected no error, but got: %v", err)
	}

	// Check if entities are loaded
	if len(registry.Entities) != len(mockResponse.Entities) {
		t.Errorf("Expected %d entities, but got %d", len(mockResponse.Entities), len(registry.Entities))
	}
}
