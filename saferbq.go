// Package saferbq provides a wrapper for the BigQuery SDK that prevents SQL injection
// in DDL operations by enabling dollar-sign $ syntax for table and dataset names.
//
// The package introduces $identifier syntax that automatically wraps identifiers in
// backticks, validates them for invalid characters, and works alongside native
// BigQuery @parameters and ? positional parameters.
//
// For more information, see: https://github.com/mevdschee/saferbq

package saferbq

import (
	"context"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

// Client wraps a BigQuery client with enhanced parameter handling.
type Client struct {
	bigquery.Client
}

// override NewClient to return a saferbq Client
func NewClient(ctx context.Context, projectID string, opts ...option.ClientOption) (*Client, error) {
	bqClient, err := bigquery.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{*bqClient}, nil
}

// Query creates a new Query with dollar-sign parameter support.
func (c *Client) Query(q string) *Query {
	bq := c.Client.Query(q)
	return &Query{
		Query:       *bq,
		originalSQL: q,
	}
}
