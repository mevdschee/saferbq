# saferbq

A Go wrapper for the BigQuery SDK that prevents SQL injection in DDL by enabling
dollar-sign `$` syntax for table and dataset names that need backtick quoting.

## The Problem

When building dynamic BigQuery queries, you often need to reference table or
dataset names that are dynamically determined at runtime and are escaped by
backticks.

BigQuery's official Go SDK uses `@` for named parameters and `?` for positional
parameters, but these cannot be used for identifiers in SQL statements that are
escaped by backticks. You're forced to use string concatenation, which opens the
door to SQL injection.

## The Solution

`saferbq` introduces `$identifier` syntax that:

1. Automatically wraps identifiers in backticks
2. Safely sanitizes special characters (convert backticks to underscores)
3. Works alongside native BigQuery `@parameters` and `?` positional parameters
4. Validates all parameters are present before execution

```go
// Instead of unsafe string concatenation with user input:
q := client.Query(fmt.Sprintf("SELECT * FROM `%s` WHERE id = 1", userInput))
q.Run(ctx)

// Use safe $ parameters for user input (supported by saferbq):
q := client.Query("SELECT * FROM $table WHERE id = 1")
q.Parameters = []bigquery.QueryParameter{{Name: "$table", Value: userInput}}
q.Run(ctx)
```

### Example of SQL Injection

String concatenation in SQL is unsafe, as it is vulnerable to SQL injection:

```go
client := bigquery.NewClient(ctx, projId)
tableName := getUserInput() // User provides: "logs` WHERE 1=1; DROP TABLE customers; --"
q := client.Query(fmt.Sprintf("SELECT * FROM `%s` WHERE user_id = 123", tableName))
// Results in: SELECT * FROM `logs` WHERE 1=1; DROP TABLE customers; --` WHERE user_id = 123
// NB: Returns all logs AND drops the customers table!
```

This mitigation does NOT work, as identifiers cannot be named parameters:

```go
client := bigquery.NewClient(ctx, projId)
tableName := getUserInput() // User provides: "logs` WHERE 1=1; DROP TABLE customers; --"
q := client.Query("SELECT * FROM @table WHERE user_id = 123")
q.Parameters = []bigquery.QueryParameter{{Name: "table", Value: tableName}}
// Results: SELECT * FROM "logs` WHERE 1=1; DROP TABLE customers; --" WHERE user_id = 123
// NB: Returns an error as named parameters on table names are not supported.
```

This is how you prevent SQL injection with saferbq:

```go
client := saferbq.NewClient(ctx, projId)
tableName := getUserInput() // User provides: "logs` WHERE 1=1; DROP TABLE customers; --"
q := client.Query("SELECT * FROM $table WHERE user_id = 123")
q.Parameters = []bigquery.QueryParameter{{Name: "$table", Value: tableName}}
// Results: SELECT * FROM `logs_ WHERE 1_1_ DROP TABLE customers_ --` WHERE user_id = 123
// NB: Fails safely as no such table exists, customers table unaffected.
```

NB: You have to create the client from the `saferbq` package instead of the
`bigquery` package.

## Installation

```bash
go get github.com/mevdschee/saferbq
```

## Usage

### Create a Supporting Client

Create the client from the `saferbq` package instead of the `bigquery` package.

```go
import (
    "context"
    "cloud.google.com/go/bigquery"
    "github.com/mevdschee/saferbq"
)

ctx := context.Background()
client, _ := saferbq.NewClient(ctx, projId) // replaces: bigquery.NewClient(...)
defer client.Close()
```

### Basic Query with Table Identifier

Use a `$` parameter for the table name:

```go
q := client.Query("SELECT * FROM $table WHERE id = 1")
q.Parameters = []bigquery.QueryParameter{
    {Name: "$table", Value: "my-table"},
}
job, _ := q.Run(ctx)

// Results: SELECT * FROM `my-table` WHERE id = 1
```

### Specifying a Path with multiple Identifiers

Use separate `$` parameters for path segments:

```go
q := client.Query("SELECT * FROM $project.$dataset.$table WHERE id = 1")
q.Parameters = []bigquery.QueryParameter{
    {Name: "$project", Value: "my-project"},
    {Name: "$dataset", Value: "my-dataset"},
    {Name: "$table", Value: "my-table"},
}
job, _ := q.Run(ctx)

// Results: SELECT * FROM `my-project`.`my-dataset`.`my-table` WHERE id = 1
```

### Mixing $ Identifiers with @ Parameters

The `$table` parameter becomes a quoted identifier, while the `@corpus`
parameter stays as a BigQuery parameter (which is safe for data values).

```go
q := client.Query("SELECT * FROM $table WHERE corpus = @corpus")
q.Parameters = []bigquery.QueryParameter{
    {Name: "$table", Value: "my-table"},
    {Name: "@corpus", Value: "en-US"},
}
job, _ := q.Run(ctx)

// Results: SELECT * FROM `my-table` WHERE corpus = @corpus
```

### Combining with Positional Parameters

You can mix the named parameters with positional parameters.

```go
q := client.Query("SELECT * FROM $table WHERE id = ? AND status = ?")
q.Parameters = []bigquery.QueryParameter{
    {Name: "$table", Value: "my-table"},
    {Value: 1},      // First ?
    {Value: "active"}, // Second ?
}
job, _ := q.Run(ctx)

// Results: SELECT * FROM `my-table` WHERE id = ? AND status = ?
```

## How It Works

- **Identifier Detection**: Finds all `$identifier` parameters in your SQL
- **Sanitization**: Converts special characters (backticks, etc.) to underscores
- **Backtick Quoting**: Wraps sanitized identifiers in backticks
- **Validation**: Ensures all parameters are provided and present in SQL
- **Replacement**: Substitutes `$identifier` with `` `safe_identifier` `` before
  execution
- **Pass-through**: Native `@parameters` and `?` are handled by BigQuery SDK as
  usual

## Parameter Types

| Syntax        | Purpose                  | Example          | Handled by |
| ------------- | ------------------------ | ---------------- | ---------- |
| `$identifier` | Table/dataset names      | `FROM $table`    | saferbq    |
| `@parameter`  | Data values (named)      | `WHERE id = @id` | bigquery   |
| `?`           | Data values (positional) | `WHERE id = ?`   | bigquery   |

Only the `$` parameters are replaced, while the `@` parameters and `?`
(positional) parameters are handled by the normal BigQuery parameterized query
mechanism.

## Naming Restrictions

### Parameter Names (in SQL)

Parameter names in your SQL must follow these rules:

- Must start with `$` or `@` for identifiers or named parameters
- Must followed by a letter or underscore
- May be follow by one or more alphanumeric characters or underscore

Valid: `$table`, `$my_table`, `$table1`

### Identifier Values (BigQuery tables/datasets)

The actual identifier values you provide can contain a much wider range of
characters, following
[BigQuery's identifier rules](https://cloud.google.com/bigquery/docs/tables#table_naming):

- **Allowed**: Letters (any Unicode letter), marks, numbers, connector
  punctuation (including `_`), dashes (`-`), and spaces
- **Sanitized**: All other characters (including backticks, semicolons, quotes,
  etc.) are automatically converted to underscores
- **Length**: Up to 1,024 characters

Examples of valid identifier values:

```go
{Name: "$table", Value: "my-table"}           // Dashes allowed
{Name: "$table", Value: "my table"}           // Spaces allowed
{Name: "$table", Value: "table_123"}          // Underscores and numbers allowed
{Name: "$table", Value: "表格"}                // Unicode letters allowed
{Name: "$table", Value: "table`; DROP TABLE"} // Sanitized to: table_ DROP TABLE
```

**Important**: To dynamically reference a full path like `project.dataset.table`
or `roles/bigquery.dataViewer`, use 3 separate parameters.

## Safety Features

- **No SQL Injection**: Identifiers are sanitized and quoted, not concatenated
- **Character Sanitization**: Backticks and special characters → underscores
- **Parameter Validation**: Errors if parameters are missing or unused
- **Drop-in Replacement**: Same API as official BigQuery SDK

## Testing

Run tests:

```bash
go test -v ./...
```

Run benchmarks:

```bash
go test -bench=. ./...
```

## Examples

See [example/main.go](example/main.go) for complete working examples.

## License

MIT

## Contributing

Contributions welcome! Please open an issue or PR.

## Related

- [BigQuery Go SDK](https://pkg.go.dev/cloud.google.com/go/bigquery)
- [BigQuery Standard SQL Reference](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)
- [BigQuery Identifiers](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#identifiers)
