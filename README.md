# saferbq

A Go wrapper for the BigQuery SDK that prevents SQL injection in DDL by enabling
dollar-sign `$` syntax for table and dataset names that need backtick quoting.

## The Problem

When building dynamic BigQuery queries, you often need to reference table or
dataset names that are dynamically determined at runtime and are escaped by
backticks.

BigQuery's official Go SDK uses `@` for named parameters and `?` for positional
parameters, but these cannot be used for identifiers in SQL statements that are
escaped by backticks. You should not use string concatenation, as that opens the
door to SQL injection.

## The Solution

`saferbq` introduces `$identifier` syntax that:

1. Automatically wraps identifiers in backticks
2. Validates identifiers and fails when invalid characters are present
3. Works alongside native BigQuery `@parameters` and `?` positional parameters
4. Validates that all parameters are present before execution

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
// Error: identifier $table contains invalid characters: `=;
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

When you execute a query, saferbq intercepts the SQL and parameters before they
reach BigQuery. First, it scans through the SQL string to identify all
dollar-sign parameters (like `$table` or `$dataset`) and extracts their names.
Simultaneously, it identifies any native BigQuery parameters that start with `@`
and positional parameters marked with `?`.

Next, it validates that every parameter found in the SQL has a corresponding
value provided in the parameters list, and vice versa - ensuring no parameters
are missing or unused. For identifier parameters (those starting with `$`), it
checks that the values are not empty and validates each character.

Each identifier value is validated by iterating through its characters. Valid
characters include Unicode letters, marks, numbers, underscores, dashes, and
spaces. If any invalid character is found (such as backticks, semicolons,
quotes, or slashes), the query immediately fails with a detailed error message
listing the problematic characters. This prevents any attempt at SQL injection
from being executed. The query also fails when the BigQuery's 1024-byte limit on
identifiers is exceeded.

After validation succeeds, each identifier is wrapped in backticks and
substituted into the SQL in place of its `$parameter` placeholder. Native
BigQuery parameters (`@param` and `?`) are left untouched in the SQL string but
have their names normalized (removing the `@` prefix) so BigQuery can process
them correctly.

Finally, the transformed SQL and updated parameter list are passed to BigQuery's
standard query execution, where the native parameters are securely bound by the
BigQuery SDK itself. This approach ensures identifiers are safely quoted while
preserving the security benefits of parameterized queries for data values.

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

### Parameter Names (in the SQL string)

Parameter names in your SQL must follow these rules:

- Must start with `$` or `@` for identifiers or named parameters
- Must followed by a letter or underscore
- May be follow by one or more alphanumeric characters or underscore

Valid: `$table`, `$my_table`, `$table1`, `$__private`

### Identifier Values (BigQuery tables/datasets)

The actual identifier values you provide must follow
[BigQuery's identifier rules](https://cloud.google.com/bigquery/docs/tables#table_naming):

- **Allowed**: Letters (any Unicode letter), marks, numbers, connector
  punctuation (including `_`), dashes (`-`), and spaces
- **Disallowed**: All other characters (including backticks, semicolons, quotes,
  slashes, etc.) will cause the query to fail with an error
- **Length**: Not empty and up to 1024 bytes

Examples of valid identifier values:

```go
{Name: "$table", Value: "my-table"}    // Dashes allowed
{Name: "$table", Value: "my table"}    // Spaces allowed
{Name: "$table", Value: "table_123"}   // Underscores and numbers allowed
{Name: "$table", Value: "表格"}         // Unicode letters allowed
```

Examples that will cause errors:

```go
{Name: "$table", Value: "table`; DROP TABLE"} // Error: $table contains `;
{Name: "$table", Value: "my.table"}           // Error: $table contains .
{Name: "$table", Value: "table/name"}         // Error: $table contains /
```

Invalid characters include: ``!"#$%&'()*+,./:;<=>?@[\]^`{|}~`` and others

**Important**: To dynamically reference a full path like `project.dataset.table`
or `roles/bigquery.dataViewer`, use 3 separate parameters.

## Safety Features

- **No SQL Injection**: Identifiers are validated and quoted, never concatenated
- **Strict Character Validation**: Invalid characters cause immediate query
  failure
- **Parameter Validation**: Errors on missing, unused, empty, invalid, or too
  long parameters
- **Drop-in Replacement**: Same API as official BigQuery SDK

## Error Handling

The package provides sentinel errors that can be checked using `errors.Is()` for
better error handling:

```go
import (
    "errors"
    "github.com/mevdschee/saferbq"
)

q := client.Query("SELECT * FROM $table WHERE id = 1")
q.Parameters = []bigquery.QueryParameter{
    {Name: "$table", Value: "my`table"},
}

_, err := q.Run(ctx)
if errors.Is(err, saferbq.ErrIdentifierInvalidChars) {
    // Handle invalid character error
    log.Printf("Invalid table name provided: %v", err)
}
```

### Available Sentinel Errors

| Error                          | Description                                        |
| ------------------------------ | -------------------------------------------------- |
| `ErrInvalidParameterName`      | Parameter name doesn't start with `@` or `$`       |
| `ErrParameterNotFound`         | Parameter in params slice not found in query       |
| `ErrParameterNotProvided`      | Parameter in query not provided in params slice    |
| `ErrIdentifierNotFound`        | Identifier in params slice not found in query      |
| `ErrIdentifierNotProvided`     | Identifier in query not provided in params slice   |
| `ErrIdentifierEmpty`           | Identifier value is empty                          |
| `ErrIdentifierTooLong`         | Identifier exceeds 1024 byte limit                 |
| `ErrIdentifierInvalidChars`    | Identifier contains invalid characters             |
| `ErrNotEnoughPositionalParams` | Fewer positional parameters provided than required |
| `ErrTooManyPositionalParams`   | More positional parameters provided than required  |
| `ErrEmptySQL`                  | Query SQL is empty                                 |

### Error Examples

```go
// Invalid character error
q := client.Query("SELECT * FROM $table")
q.Parameters = []bigquery.QueryParameter{
    {Name: "$table", Value: "table; DROP TABLE users"},
}
_, err := q.Run(ctx)
// err: "identifier contains invalid characters: $table contains ;"

// Missing parameter error
q := client.Query("SELECT * FROM $table WHERE status = @status")
q.Parameters = []bigquery.QueryParameter{
    {Name: "$table", Value: "my-table"},
    // Missing @status parameter
}
_, err := q.Run(ctx)
// err: "parameter not provided in parameters: @status"

// Unused parameter error
q := client.Query("SELECT * FROM $table")
q.Parameters = []bigquery.QueryParameter{
    {Name: "$table", Value: "my-table"},
    {Name: "$unused", Value: "extra"}, // Not used in query
}
_, err := q.Run(ctx)
// err: "identifier not found in query: $unused"
```

## Testing

Run tests:

```bash
go test -v ./...
```

Run tests with coverage:

```bash
go test -coverprofile=coverage.txt ./...
```

Calculate coverage percentage:

```bash
go tool cover -func=coverage.txt
```

Generate and view HTML coverage report:

```bash
go tool cover -html=coverage.txt
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

Contributions welcome! Please open an issue or PR. Ensure you run the tests and
keep 100% coverage.

## Related

- [TQdev - Avoid BigQuery SQL injection in Go with saferbq](https://www.tqdev.com/2025-avoid-bigquery-sql-injection-go-saferbq/)
- [Google - BigQuery Go SDK](https://pkg.go.dev/cloud.google.com/go/bigquery)
- [Google - BigQuery Parameterized Queries](https://docs.cloud.google.com/bigquery/docs/parameterized-queries)
- [Google - BigQuery Identifiers](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)
- [Google - BigQuery Standard SQL Reference](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)
