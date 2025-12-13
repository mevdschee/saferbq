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
2. Safely sanitizes special characters (convert hyphens to underscores)
3. Works alongside native BigQuery `@parameters` and `?` positional parameters
4. Validates all parameters are present before execution

```go
// Instead of unsafe string concatenation:
sql := fmt.Sprintf("SELECT * FROM `%s` WHERE id = 1", unsafetable)

// Use safe $ parameters:
sql := "SELECT * FROM $table WHERE id = 1"
q.Parameters = []bigquery.QueryParameter{
    {Name: "$table", Value: "myproject.mydataset.mytable"},
}
```

### SQL Injection Attack

String concatenation in SQL is UNSAFE, as it allows for SQL injection:

```go
client := bigquery.NewClient(ctx, "myproject")
tableName := getUserInput() // User provides: "logs` WHERE 1=1; DROP TABLE customers; --"
q := client.Query(fmt.Sprintf("SELECT * FROM `%s` WHERE user_id = 123", tableName))
// Results in: SELECT * FROM `logs` WHERE 1=1; DROP TABLE customers; --` WHERE user_id = 123
// NB: Returns all logs AND drops the customers table!
```

This mitigation does NOT work, as identifiers cannot be named parameters:

```go
client := bigquery.NewClient(ctx, "myproject")
tableName := getUserInput() // User provides: "logs` WHERE 1=1; DROP TABLE customers; --"
q := client.Query("SELECT * FROM @table WHERE user_id = 123")
q.Parameters = []bigquery.QueryParameter{{Name: "table", Value: tableName}}
// Results: SELECT * FROM "logs` WHERE 1=1; DROP TABLE customers; --" WHERE user_id = 123
// NB: Returns an error as named parameters on table names are not supported.
```

This is how you prevent SQL injection with saferbq:

```go
client := saferbq.NewClient(ctx, "myproject")
tableName := getUserInput() // User provides: "logs` WHERE 1=1; DROP TABLE customers; --"
q := client.Query("SELECT * FROM $table WHERE user_id = 123")
q.Parameters = []bigquery.QueryParameter{{Name: "$table", Value: tableName}}
// Results: SELECT * FROM `logs__WHERE_1_1__DROP_TABLE_customers____` WHERE user_id = 123
// NB: Fails safely as no such table exists, customers table unaffected.
```

## Installation

```bash
go get github.com/maurits/saferbq
```

## Usage

### Basic Query with Table Identifier

```go
import (
    "context"
    "cloud.google.com/go/bigquery"
    "github.com/maurits/saferbq"
)

ctx := context.Background()
client, _ := saferbq.NewClient(ctx, "myproject")
defer client.Close()

// $table will be replaced with `myproject.mydataset.mytable`
sql := "SELECT * FROM $table WHERE id = 1"
q := client.Query(sql)
q.Parameters = []bigquery.QueryParameter{
    {Name: "$table", Value: "myproject.mydataset.mytable"},
}

it, _ := q.Read(ctx)
// Results: SELECT * FROM `myproject.mydataset.mytable` WHERE id = 1
```

### Mixing $ Identifiers with @ Parameters

```go
// $table becomes a quoted identifier
// @corpus stays as a BigQuery parameter (safe for data values)
sql := "SELECT * FROM $table WHERE corpus = @corpus"
q := client.Query(sql)
q.Parameters = []bigquery.QueryParameter{
    {Name: "$table", Value: "mytable"},
    {Name: "@corpus", Value: "en-US"},
}

// Results: SELECT * FROM `mytable` WHERE corpus = @corpus
```

### DDL Operations

Perfect for CREATE/ALTER/DROP statements where identifiers can't be
parameterized:

```go
sql := `CREATE TABLE IF NOT EXISTS $table (
    id INT64,
    name STRING,
    created_at TIMESTAMP
)`
q := client.Query(sql)
q.Parameters = []bigquery.QueryParameter{
    {Name: "$table", Value: "mydataset.mynew-table"},
}

job, _ := q.Run(ctx)
// Results: CREATE TABLE IF NOT EXISTS `mydataset_mynew_table` (...)
```

### Multiple Table Identifiers

```go
sql := "SELECT * FROM $table1 JOIN $table2 ON $table1.id = $table2.id"
q := client.Query(sql)
q.Parameters = []bigquery.QueryParameter{
    {Name: "$table1", Value: "dataset.table1"},
    {Name: "$table2", Value: "dataset.table2"},
}

// Results: SELECT * FROM `dataset_table1` JOIN `dataset_table2` ON `dataset_table1`.id = `dataset_table2`.id
```

### Combining with Positional Parameters

```go
sql := "SELECT * FROM $project.$dataset.$table WHERE id = ? AND status = ?"
q := client.Query(sql)
q.Parameters = []bigquery.QueryParameter{
    {Name: "$project", Value: "myproject"},
    {Name: "$dataset", Value: "mydataset"},
    {Name: "$table", Value: "mytable"},
    {Value: 1},      // First ?
    {Value: "active"}, // Second ?
}

// Results: SELECT * FROM `myproject`.`mydataset`.`mytable` WHERE id = ? AND status = ?
```

## How It Works

1. **Identifier Detection**: Finds all `$identifier` parameters in your SQL
2. **Sanitization**: Converts special characters (hyphens, dots, etc.) to
   underscores
3. **Backtick Quoting**: Wraps sanitized identifiers in backticks
4. **Validation**: Ensures all parameters are provided and present in SQL
5. **Replacement**: Substitutes `$identifier` with `` `safe_identifier` ``
   before execution
6. **Pass-through**: Native `@parameters` and `?` are handled by BigQuery SDK as
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

## Safety Features

- **No SQL Injection**: Identifiers are sanitized and quoted, not concatenated
- **Character Sanitization**: Hyphens and special characters → underscores
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
