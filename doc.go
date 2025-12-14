// Package saferbq provides a wrapper for the BigQuery SDK that prevents SQL injection
// in DDL operations by enabling dollar-sign $ syntax for table and dataset names.
//
// The package introduces $identifier syntax that automatically wraps identifiers in
// backticks, validates them for invalid characters, and works alongside native
// BigQuery @parameters and ? positional parameters.
//
// # Problem
//
// When building dynamic BigQuery queries, you often need to reference table or
// dataset names that are dynamically determined at runtime. BigQuery's official
// Go SDK uses @ for named parameters and ? for positional parameters, but these
// cannot be used for identifiers like table names that need backtick quoting.
// You should not use string concatenation, as that opens the door to SQL injection.
//
// # Solution
//
// saferbq introduces $identifier syntax that:
//   - Automatically wraps identifiers in backticks
//   - Validates identifiers and fails when invalid characters are present
//   - Works alongside native BigQuery @parameters and ? positional parameters
//   - Validates that all parameters are present before execution
//
// # Basic Usage
//
//	ctx := context.Background()
//	client, err := saferbq.NewClient(ctx, "my-project")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	q := client.Query("SELECT * FROM $table WHERE status = @status")
//	q.Parameters = []bigquery.QueryParameter{
//	    {Name: "$table", Value: "users"},
//	    {Name: "@status", Value: "active"},
//	}
//
//	job, err := q.Run(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// # SQL Injection Prevention
//
// The package prevents SQL injection by validating all identifier characters:
//
//	tableName := getUserInput() // "logs` WHERE 1=1; DROP TABLE customers; --"
//	q := client.Query("SELECT * FROM $table WHERE user_id = 123")
//	q.Parameters = []bigquery.QueryParameter{{Name: "$table", Value: tableName}}
//	_, err := q.Run(ctx)
//	// Returns error: identifier contains invalid characters: $table contains `=;
//
// # Parameter Types
//
// saferbq supports three parameter types:
//   - $identifier - For table/dataset names (replaced with backtick-quoted values)
//   - @parameter - For data values (native BigQuery named parameters)
//   - ? - For data values (native BigQuery positional parameters)
//
// # Naming Restrictions
//
// Parameter names in SQL must follow these rules:
//   - Start with $ (for identifiers) or @ (for named parameters)
//   - Contain only alphanumeric characters and underscores
//   - No dots allowed in parameter names
//
// For fully qualified table names, use separate parameters:
//
//	q := client.Query("SELECT * FROM $project.$dataset.$table")
//	q.Parameters = []bigquery.QueryParameter{
//	    {Name: "$project", Value: "my-project"},
//	    {Name: "$dataset", Value: "my-dataset"},
//	    {Name: "$table", Value: "my-table"},
//	}
//
// Identifier values (the actual table/dataset names) follow BigQuery's rules:
//   - Unicode letters, marks, numbers
//   - Underscores, dashes, spaces
//   - Maximum 1024 bytes
//   - Invalid characters cause query to fail with error
//
// # Error Handling
//
// saferbq provides sentinel errors that can be checked with errors.Is():
//
//	_, err := q.Run(ctx)
//	if err != nil {
//	    if errors.Is(err, saferbq.ErrInvalidCharacters) {
//	        // Handle invalid character error
//	    }
//	    if errors.Is(err, saferbq.ErrParameterNotProvided) {
//	        // Handle missing parameter error
//	    }
//	}
//
// Available sentinel errors include:
//   - ErrEmptyIdentifier
//   - ErrIdentifierTooLong
//   - ErrInvalidCharacters
//   - ErrParameterNotFound
//   - ErrParameterNotProvided
//   - ErrInvalidParameterName
//   - ErrPositionalParameterMismatch
//   - ErrEmptySQL
//
// For more information, see: https://github.com/mevdschee/saferbq
package saferbq
