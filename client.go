// Package saferbq provides a wrapper around the BigQuery Go SDK that enables
// dollar-sign parameter syntax and safe identifier quoting for DDL operations.
//
// The native BigQuery SDK uses @ for named parameters (e.g., @param) and ? for
// positional parameters. This package adds $ syntax (e.g., $param) for
// identifiers that need backtick quoting.
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
