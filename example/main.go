package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/mevdschee/saferbq"
	"google.golang.org/api/iterator"
)

func main() {
	ctx := context.Background()

	// Create client (requires valid GCP credentials)
	client, err := saferbq.NewClient(ctx, "myproject")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Example 1: Table identifiers with $-syntax
	fmt.Println("Example 1: Table identifiers with $ parameters")
	queryWithIdentifiers(ctx, client)

	// Example 2: Mix identifiers and @ parameters
	fmt.Println("\nExample 2: Mixing $ identifiers and @ parameters")
	mixedParameters(ctx, client)

	// Example 3: Positional parameters with ?
	fmt.Println("\nExample 3: Positional parameters")
	queryPositionalParams(ctx, client)

	// Example 4: DDL operations with table identifiers
	fmt.Println("\nExample 4: DDL operations")
	ddlOperations(ctx, client)

	// Example 5: Multiple table identifiers
	fmt.Println("\nExample 5: Multiple table identifiers in JOIN")
	multipleIdentifiers(ctx, client)
}

func queryWithIdentifiers(ctx context.Context, client *saferbq.Client) {
	// Simple query with $ identifier
	sql := "SELECT * FROM $table WHERE id = 1"
	q := client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{
			Name:  "$table",
			Value: "mytable",
		},
	}

	// Resulting SQL: SELECT * FROM `mytable` WHERE id = 1
	it, err := q.Read(ctx)
	if err != nil {
		log.Printf("Query error: %v", err)
		return
	}

	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("Iteration error: %v", err)
			return
		}
		fmt.Println(values)
	}
}

func mixedParameters(ctx context.Context, client *saferbq.Client) {
	// Mix of $ identifiers and @ parameters
	sql := "SELECT * FROM $table WHERE corpus = @corpus"
	q := client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{
			Name:  "$table",
			Value: "mytable",
		},
		{
			Name:  "@corpus",
			Value: "en-US",
		},
	}

	// Resulting SQL: SELECT * FROM `mytable` WHERE corpus = @corpus
	it, err := q.Read(ctx)
	if err != nil {
		log.Printf("Query error: %v", err)
		return
	}

	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("Iteration error: %v", err)
			return
		}
		fmt.Println(values)
	}
}

func queryPositionalParams(ctx context.Context, client *saferbq.Client) {
	// Mix of $ identifiers and ? positional parameters
	sql := "SELECT * FROM $project.$dataset.$table WHERE id = ?"
	q := client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{
			Name:  "$project",
			Value: "myproject",
		},
		{
			Name:  "$dataset",
			Value: "mydataset",
		},
		{
			Name:  "$table",
			Value: "mytable",
		},
		{
			Value: 1, // Positional parameter
		},
	}

	// Resulting SQL: SELECT * FROM `myproject`.`mydataset`.`mytable` WHERE id = ?
	it, err := q.Read(ctx)
	if err != nil {
		log.Printf("Query error: %v", err)
		return
	}

	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("Iteration error: %v", err)
			return
		}
		fmt.Println(values)
	}
}

func ddlOperations(ctx context.Context, client *saferbq.Client) {
	// DDL operations use $-syntax for table identifiers
	sql := `CREATE TABLE IF NOT EXISTS $dataset.$table (
		id INT64,
		name STRING,
		created_at TIMESTAMP
	)`
	q := client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{
			Name:  "$dataset",
			Value: "mydataset",
		},
		{
			Name:  "$table",
			Value: "mynew-table",
		},
	}

	// Resulting SQL: CREATE TABLE IF NOT EXISTS `mydataset`.`mynew_table` (...)
	job, err := q.Run(ctx)
	if err != nil {
		log.Printf("DDL error: %v", err)
		return
	}

	status, err := job.Wait(ctx)
	if err != nil {
		log.Printf("Job wait error: %v", err)
		return
	}
	if err := status.Err(); err != nil {
		log.Printf("Job status error: %v", err)
		return
	}
	fmt.Println("Table created successfully")
}

func multipleIdentifiers(ctx context.Context, client *saferbq.Client) {
	// Multiple table identifiers in a JOIN query
	sql := "SELECT * FROM dataset.$table1 JOIN dataset.$table2 ON dataset.$table1.id = dataset.$table2.id"
	q := client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{
			Name:  "$table1",
			Value: "table1",
		},
		{
			Name:  "$table2",
			Value: "table2",
		},
	}

	// Resulting SQL: SELECT * FROM dataset.`table1` JOIN dataset.`table2` ON dataset.`table1`.id = dataset.`table2`.id
	it, err := q.Read(ctx)
	if err != nil {
		log.Printf("Query error: %v", err)
		return
	}

	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("Iteration error: %v", err)
			return
		}
		fmt.Println(values)
	}
}
