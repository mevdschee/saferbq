package saferbq

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

func TestNewClientSuccess(t *testing.T) {
	ctx := context.Background()
	client, err := NewClient(ctx, "test-project", option.WithoutAuthentication())

	if err != nil {
		t.Fatalf("NewClient() unexpected error: %v", err)
	}
	if client == nil {
		t.Fatal("NewClient() expected client, got nil")
	}

	// Verify that the client is properly initialized
	if err := client.Close(); err != nil {
		t.Errorf("Client.Close() error: %v", err)
	}
}

func TestNewClientError(t *testing.T) {
	ctx := context.Background()
	// Use an invalid option to trigger an error from bigquery.NewClient
	invalidOpt := option.WithCredentialsFile("/nonexistent/path/to/credentials.json")

	client, err := NewClient(ctx, "test-project", invalidOpt)
	if err == nil {
		t.Error("NewClient() with invalid credentials file should return error")
		if client != nil {
			client.Close()
		}
	}
	if client != nil {
		t.Errorf("NewClient() should return nil client on error, got %v", client)
	}
}

func TestClientQuery(t *testing.T) {
	ctx := context.Background()
	client, err := NewClient(ctx, "test-project", option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer client.Close()

	// Test that Query() creates a Query that can be further configured
	queryStr := "SELECT * FROM $table WHERE id = @id"
	q := client.Query(queryStr)

	// Set parameters
	q.Parameters = []bigquery.QueryParameter{
		{Name: "$table", Value: "users"},
		{Name: "@id", Value: 123},
	}

	if len(q.Parameters) != 2 {
		t.Errorf("Query.Parameters length = %d, want 2", len(q.Parameters))
	}

	// Verify originalSQL is preserved
	if q.originalSQL != queryStr {
		t.Errorf("Query.originalSQL = %q, want %q", q.originalSQL, queryStr)
	}
}

func TestEmbeddedBigQueryClient(t *testing.T) {
	ctx := context.Background()
	client, err := NewClient(ctx, "test-project", option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer client.Close()

	// Test that we can access bigquery.Client methods through embedding
	// This verifies the embedding is working correctly
	dataset := client.Dataset("test-dataset")
	if dataset == nil {
		t.Errorf("Client.Dataset() returned nil")
		return
	}

	// Verify Dataset method returns proper dataset reference
	if dataset.DatasetID != "test-dataset" {
		t.Errorf("Dataset.DatasetID = %q, want %q", dataset.DatasetID, "test-dataset")
	}

	// Verify Project is set correctly
	if dataset.ProjectID != "test-project" {
		t.Errorf("Dataset.ProjectID = %q, want %q", dataset.ProjectID, "test-project")
	}
}
