package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/maurits/saferbq"
	"google.golang.org/api/iterator"
)

func main() {
	ctx := context.Background()

	// Create client (requires valid GCP credentials)
	client, err := saferbq.NewClient(ctx, "my-project")
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
	// $table gets replaced with quoted identifier: `my_project_my_dataset_my_table`
	// Hyphens and dots are automatically converted to underscores
	sql := "SELECT * FROM $table WHERE id = 1"
	q := client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{
			Name:  "$table",
			Value: "my-project.my-dataset.my-table",
		},
	}

	// Resulting SQL: SELECT * FROM `my_project_my_dataset_my_table` WHERE id = 1
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
	// $table is replaced with identifier, @corpus stays as native BigQuery parameter
	sql := "SELECT * FROM $table WHERE corpus = @corpus"
	q := client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{
			Name:  "$table",
			Value: "my-table",
		},
		{
			Name:  "@corpus",
			Value: "en-US",
		},
	}

	// Resulting SQL: SELECT * FROM `my_table` WHERE corpus = @corpus
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
	// $table with identifier, ? for positional parameters
	sql := "SELECT * FROM $project.$dataset.$table WHERE id = ?"
	q := client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{
			Name:  "$project",
			Value: "my-project",
		},
		{
			Name:  "$dataset",
			Value: "my-dataset",
		},
		{
			Name:  "$table",
			Value: "my-table",
		},
		{
			Value: 1, // Positional parameter
		},
	}

	// Resulting SQL: SELECT * FROM `my_project`.`my_dataset`.`my_table` WHERE id = ?
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
	// DDL operations use $-syntax for table identifiers that need to be quoted
	// This is perfect for tables with hyphens or reserved words
	sql := `CREATE TABLE IF NOT EXISTS $dataset.$table (
		id INT64,
		name STRING,
		created_at TIMESTAMP
	)`
	q := client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{
			Name:  "$dataset",
			Value: "my-dataset",
		},
		{
			Name:  "$table",
			Value: "my-new-table",
		},
	}

	// Resulting SQL: CREATE TABLE IF NOT EXISTS `my_dataset`.`my_new_table` (...)
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
	sql := "SELECT * FROM $table1 JOIN $table2 ON $table1.id = $table2.id"
	q := client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{
		{
			Name:  "$table1",
			Value: "dataset.table1",
		},
		{
			Name:  "$table2",
			Value: "dataset.table2",
		},
	}

	// Resulting SQL: SELECT * FROM `dataset_table1` JOIN `dataset_table2` ON `dataset_table1`.id = `dataset_table2`.id
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
