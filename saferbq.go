package saferbq

import (
	"context"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

// Client wraps a BigQuery client with enhanced parameter handling.
// It provides the same functionality as bigquery.Client but with support
// for $identifier parameters in queries.
type Client struct {
	bigquery.Client
}

// NewClient creates a new BigQuery client with saferbq enhancements.
// It accepts the same options as bigquery.NewClient and returns a client
// that supports $identifier parameters in queries.
//
// Example:
//
//	client, err := saferbq.NewClient(ctx, "my-project")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
func NewClient(ctx context.Context, projectID string, opts ...option.ClientOption) (*Client, error) {
	bqClient, err := bigquery.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{*bqClient}, nil
}

// Query creates a new Query with dollar-sign parameter support.
// The query string can contain $identifier parameters that will be
// safely quoted, as well as native BigQuery @parameters and ? positional
// parameters.
//
// Example:
//
//	q := client.Query("SELECT * FROM $table WHERE status = @status")
//	q.Parameters = []bigquery.QueryParameter{
//	    {Name: "$table", Value: "users"},
//	    {Name: "@status", Value: "active"},
//	}
func (c *Client) Query(q string) *Query {
	bq := c.Client.Query(q)
	return &Query{
		Query:       *bq,
		originalSQL: q,
	}
}
