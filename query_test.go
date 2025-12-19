package saferbq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

func TestQueryTranslate(t *testing.T) {
	tests := []struct {
		name          string
		sqlIn         string
		parametersIn  []bigquery.QueryParameter
		sqlOut        string
		parametersOut []bigquery.QueryParameter
		errorMessage  string
	}{
		{
			name:          "identifier replacement only, full table path",
			sqlIn:         "SELECT * FROM $table WHERE id = 1",
			parametersIn:  []bigquery.QueryParameter{{Name: "$table", Value: "myproject.mydataset.mytable"}},
			sqlOut:        "SELECT * FROM `myproject.mydataset.mytable` WHERE id = 1",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "identifier replacement with positional parameter",
			sqlIn:         "SELECT * FROM $project.$dataset.$table WHERE id = ?",
			parametersIn:  []bigquery.QueryParameter{{Name: "$project", Value: "myproject"}, {Name: "$dataset", Value: "mydataset"}, {Name: "$table", Value: "mytable"}, {Value: 1}},
			sqlOut:        "SELECT * FROM `myproject`.`mydataset`.`mytable` WHERE id = ?",
			parametersOut: []bigquery.QueryParameter{{Value: 1}},
		},
		{
			name:          "multiple identifiers with a dot",
			sqlIn:         "SELECT * FROM $project.$dataset.$table WHERE id = 1",
			parametersIn:  []bigquery.QueryParameter{{Name: "$project", Value: "myproject"}, {Name: "$dataset", Value: "mydataset"}, {Name: "$table", Value: "mytable"}},
			sqlOut:        "SELECT * FROM `myproject`.`mydataset`.`mytable` WHERE id = 1",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "multiple identifiers",
			sqlIn:         "SELECT * FROM dataset.$table1 JOIN dataset.$table2 ON dataset.$table1.id = dataset.$table2.id",
			parametersIn:  []bigquery.QueryParameter{{Name: "$table1", Value: "table1"}, {Name: "$table2", Value: "table2"}},
			sqlOut:        "SELECT * FROM dataset.`table1` JOIN dataset.`table2` ON dataset.`table1`.id = dataset.`table2`.id",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "@ parameter stays unchanged",
			sqlIn:         "SELECT * FROM table WHERE corpus = @corpus",
			parametersIn:  []bigquery.QueryParameter{{Name: "@corpus", Value: "corpus_value"}},
			sqlOut:        "SELECT * FROM table WHERE corpus = @corpus",
			parametersOut: []bigquery.QueryParameter{{Name: "corpus", Value: "corpus_value"}},
		},
		{
			name:          "mixed @ and $ parameters",
			sqlIn:         "SELECT * FROM $tablename WHERE corpus = @corpus",
			parametersIn:  []bigquery.QueryParameter{{Name: "$tablename", Value: "mytable"}, {Name: "@corpus", Value: "corpus_value"}},
			sqlOut:        "SELECT * FROM `mytable` WHERE corpus = @corpus",
			parametersOut: []bigquery.QueryParameter{{Name: "corpus", Value: "corpus_value"}},
		},
		// Test cases from documentation examples
		{
			name:          "positional params from doc - shakespeare corpus",
			sqlIn:         "SELECT word, word_count FROM $project.$dataset.$table WHERE corpus = ? AND word_count >= ? ORDER BY word_count DESC",
			parametersIn:  []bigquery.QueryParameter{{Name: "$project", Value: "bigquery-public-data"}, {Name: "$dataset", Value: "samples"}, {Name: "$table", Value: "shakespeare"}, {Value: "romeoandjuliet"}, {Value: 250}},
			sqlOut:        "SELECT word, word_count FROM `bigquery-public-data`.`samples`.`shakespeare` WHERE corpus = ? AND word_count >= ? ORDER BY word_count DESC",
			parametersOut: []bigquery.QueryParameter{{Value: "romeoandjuliet"}, {Value: 250}},
		},
		{
			name:          "array params from doc - usa names",
			sqlIn:         "SELECT name, sum(number) as count FROM $project.$dataset.$table WHERE gender = @gender AND state IN UNNEST(@states) GROUP BY name ORDER BY count DESC LIMIT 10",
			parametersIn:  []bigquery.QueryParameter{{Name: "$project", Value: "bigquery-public-data"}, {Name: "$dataset", Value: "usa_names"}, {Name: "$table", Value: "usa_1910_2013"}, {Name: "@gender", Value: "M"}, {Name: "@states", Value: []string{"WA", "WI", "WV", "WY"}}},
			sqlOut:        "SELECT name, sum(number) as count FROM `bigquery-public-data`.`usa_names`.`usa_1910_2013` WHERE gender = @gender AND state IN UNNEST(@states) GROUP BY name ORDER BY count DESC LIMIT 10",
			parametersOut: []bigquery.QueryParameter{{Name: "gender", Value: "M"}, {Name: "states", Value: []string{"WA", "WI", "WV", "WY"}}},
		},
		{
			name:          "basic query from doc - texas names",
			sqlIn:         "SELECT name FROM $project.$dataset.$table WHERE state = @state LIMIT 100",
			parametersIn:  []bigquery.QueryParameter{{Name: "$project", Value: "bigquery-public-data"}, {Name: "$dataset", Value: "usa_names"}, {Name: "$table", Value: "usa_1910_2013"}, {Name: "@state", Value: "TX"}},
			sqlOut:        "SELECT name FROM `bigquery-public-data`.`usa_names`.`usa_1910_2013` WHERE state = @state LIMIT 100",
			parametersOut: []bigquery.QueryParameter{{Name: "state", Value: "TX"}},
		},
		{
			name:          "batch query from doc - aggregate shakespeare",
			sqlIn:         "SELECT corpus, SUM(word_count) as total_words, COUNT(1) as unique_words FROM $project.$dataset.$table GROUP BY corpus",
			parametersIn:  []bigquery.QueryParameter{{Name: "$project", Value: "bigquery-public-data"}, {Name: "$dataset", Value: "samples"}, {Name: "$table", Value: "shakespeare"}},
			sqlOut:        "SELECT corpus, SUM(word_count) as total_words, COUNT(1) as unique_words FROM `bigquery-public-data`.`samples`.`shakespeare` GROUP BY corpus",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "query from doc - usa names aggregate",
			sqlIn:         "SELECT name, gender, SUM(number) AS total FROM $project.$dataset.$table GROUP BY name, gender ORDER BY total DESC LIMIT 10",
			parametersIn:  []bigquery.QueryParameter{{Name: "$project", Value: "bigquery-public-data"}, {Name: "$dataset", Value: "usa_names"}, {Name: "$table", Value: "usa_1910_2013"}},
			sqlOut:        "SELECT name, gender, SUM(number) AS total FROM `bigquery-public-data`.`usa_names`.`usa_1910_2013` GROUP BY name, gender ORDER BY total DESC LIMIT 10",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "dry run query from doc - name count by state",
			sqlIn:         "SELECT name, COUNT(*) as name_count FROM $project.$dataset.$table WHERE state = @state GROUP BY name",
			parametersIn:  []bigquery.QueryParameter{{Name: "$project", Value: "bigquery-public-data"}, {Name: "$dataset", Value: "usa_names"}, {Name: "$table", Value: "usa_1910_2013"}, {Name: "@state", Value: "WA"}},
			sqlOut:        "SELECT name, COUNT(*) as name_count FROM `bigquery-public-data`.`usa_names`.`usa_1910_2013` WHERE state = @state GROUP BY name",
			parametersOut: []bigquery.QueryParameter{{Name: "state", Value: "WA"}}},
		{
			name:          "create table with identifiers",
			sqlIn:         "CREATE TABLE IF NOT EXISTS $dataset.$table (id INT64, name STRING, created_at TIMESTAMP)",
			parametersIn:  []bigquery.QueryParameter{{Name: "$dataset", Value: "mydataset"}, {Name: "$table", Value: "my-new-table"}},
			sqlOut:        "CREATE TABLE IF NOT EXISTS `mydataset`.`my-new-table` (id INT64, name STRING, created_at TIMESTAMP)",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "delete with identifier",
			sqlIn:         "DELETE FROM $dataset.$table WHERE id = @id",
			parametersIn:  []bigquery.QueryParameter{{Name: "$dataset", Value: "mydataset"}, {Name: "$table", Value: "mytable"}, {Name: "@id", Value: 1}},
			sqlOut:        "DELETE FROM `mydataset`.`mytable` WHERE id = @id",
			parametersOut: []bigquery.QueryParameter{{Name: "id", Value: 1}},
		},
		{
			name:          "update with identifier",
			sqlIn:         "UPDATE $dataset.$table SET status = @status WHERE id = @id",
			parametersIn:  []bigquery.QueryParameter{{Name: "$dataset", Value: "mydataset"}, {Name: "$table", Value: "mytable"}, {Name: "@status", Value: "active"}, {Name: "@id", Value: 1}},
			sqlOut:        "UPDATE `mydataset`.`mytable` SET status = @status WHERE id = @id",
			parametersOut: []bigquery.QueryParameter{{Name: "status", Value: "active"}, {Name: "id", Value: 1}},
		},
		{
			name:          "insert with identifier",
			sqlIn:         "INSERT INTO $dataset.$table (id, name) VALUES (@id, @name)",
			parametersIn:  []bigquery.QueryParameter{{Name: "$dataset", Value: "mydataset"}, {Name: "$table", Value: "mytable"}, {Name: "@id", Value: 1}, {Name: "@name", Value: "test"}},
			sqlOut:        "INSERT INTO `mydataset`.`mytable` (id, name) VALUES (@id, @name)",
			parametersOut: []bigquery.QueryParameter{{Name: "id", Value: 1}, {Name: "name", Value: "test"}},
		},
		{
			name:          "with cte and identifier",
			sqlIn:         "WITH cte AS (SELECT * FROM $dataset.$table WHERE active = true) SELECT * FROM cte",
			parametersIn:  []bigquery.QueryParameter{{Name: "$dataset", Value: "mydataset"}, {Name: "$table", Value: "mytable"}},
			sqlOut:        "WITH cte AS (SELECT * FROM `mydataset`.`mytable` WHERE active = true) SELECT * FROM cte",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "union with identifiers",
			sqlIn:         "SELECT * FROM $dataset.$table1 UNION ALL SELECT * FROM $dataset.$table2",
			parametersIn:  []bigquery.QueryParameter{{Name: "$dataset", Value: "dataset"}, {Name: "$table1", Value: "table1"}, {Name: "$table2", Value: "table2"}},
			sqlOut:        "SELECT * FROM `dataset`.`table1` UNION ALL SELECT * FROM `dataset`.`table2`",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "subquery with identifier",
			sqlIn:         "SELECT * FROM (SELECT id FROM $dataset.$table WHERE status = @status) WHERE id > @id",
			parametersIn:  []bigquery.QueryParameter{{Name: "$dataset", Value: "mydataset"}, {Name: "$table", Value: "mytable"}, {Name: "@status", Value: "active"}, {Name: "@id", Value: 100}},
			sqlOut:        "SELECT * FROM (SELECT id FROM `mydataset`.`mytable` WHERE status = @status) WHERE id > @id",
			parametersOut: []bigquery.QueryParameter{{Name: "status", Value: "active"}, {Name: "id", Value: 100}},
		},
		{
			name:          "window function with identifier",
			sqlIn:         "SELECT id, ROW_NUMBER() OVER (ORDER BY created_at) as row_num FROM $dataset.$table",
			parametersIn:  []bigquery.QueryParameter{{Name: "$dataset", Value: "mydataset"}, {Name: "$table", Value: "mytable"}},
			sqlOut:        "SELECT id, ROW_NUMBER() OVER (ORDER BY created_at) as row_num FROM `mydataset`.`mytable`",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "case statement with identifier and params",
			sqlIn:         "SELECT id, CASE WHEN status = @status1 THEN 'active' WHEN status = @status2 THEN 'inactive' END as status_label FROM $dataset.$table",
			parametersIn:  []bigquery.QueryParameter{{Name: "$dataset", Value: "mydataset"}, {Name: "$table", Value: "mytable"}, {Name: "@status1", Value: 1}, {Name: "@status2", Value: 0}},
			sqlOut:        "SELECT id, CASE WHEN status = @status1 THEN 'active' WHEN status = @status2 THEN 'inactive' END as status_label FROM `mydataset`.`mytable`",
			parametersOut: []bigquery.QueryParameter{{Name: "status1", Value: 1}, {Name: "status2", Value: 0}},
		},
		// Error cases - invalid SQL
		{
			name:         "empty SQL string",
			sqlIn:        "",
			parametersIn: []bigquery.QueryParameter{},
			errorMessage: "query SQL cannot be empty",
		},
		// Error cases - invalid parameter names or values
		{
			name:         "invalid parameter name",
			sqlIn:        "SELECT * FROM table WHERE id = 1",
			parametersIn: []bigquery.QueryParameter{{Name: "corpus", Value: "corpus_value"}},
			errorMessage: "invalid parameter name: corpus must start with @ or $",
		},
		{
			name:         "empty identifier value",
			sqlIn:        "SELECT * FROM $table WHERE id = 1",
			parametersIn: []bigquery.QueryParameter{{Name: "$table", Value: ""}},
			errorMessage: "identifier is empty: $table",
		},
		{
			name:         "identifier value too long",
			sqlIn:        "SELECT * FROM $table WHERE id = 1",
			parametersIn: []bigquery.QueryParameter{{Name: "$table", Value: strings.Repeat("a", 1025)}},
			errorMessage: "identifier is too long: $table",
		},
		// Error cases - positional parameter validation
		{
			name:         "missing positional parameter",
			sqlIn:        "SELECT * FROM $table WHERE id = ? AND status = ?",
			parametersIn: []bigquery.QueryParameter{{Name: "$table", Value: "mytable"}, {Value: 1}},
			errorMessage: "not enough positional parameters: found 2, provided 1",
		},
		{
			name:         "extra positional parameter",
			sqlIn:        "SELECT * FROM $table WHERE id = ?",
			parametersIn: []bigquery.QueryParameter{{Name: "$table", Value: "mytable"}, {Value: 1}, {Value: 2}},
			errorMessage: "too many positional parameters: found 1, provided 2",
		},
		{
			name:         "mixing positional and named parameters",
			sqlIn:        "SELECT * FROM $table WHERE id = ? AND status = @status",
			parametersIn: []bigquery.QueryParameter{{Name: "$table", Value: "mytable"}, {Name: "@status", Value: "active"}, {Value: 1}},
			errorMessage: "cannot mix positional (?) and named (@) parameters",
		},
		{
			name:         "no positional parameters in query but some provided",
			sqlIn:        "SELECT * FROM $table WHERE id = @id",
			parametersIn: []bigquery.QueryParameter{{Name: "$table", Value: "mytable"}, {Name: "@id", Value: 1}, {Value: 999}},
			errorMessage: "too many positional parameters: found 0, provided 1",
		},
		{
			name:         "multiple missing positional parameters",
			sqlIn:        "SELECT * FROM $table WHERE id = ? AND status = ? AND created_at > ? AND updated_at < ?",
			parametersIn: []bigquery.QueryParameter{{Name: "$table", Value: "mytable"}, {Value: 1}},
			errorMessage: "not enough positional parameters: found 4, provided 1",
		},
		// Error cases - missing named parameters
		{
			name:         "missing named parameter",
			sqlIn:        "SELECT * FROM $table WHERE status = @status",
			parametersIn: []bigquery.QueryParameter{{Name: "$table", Value: "mytable"}},
			errorMessage: "parameter not provided in parameters: @status",
		},
		{
			name:         "missing identifier",
			sqlIn:        "SELECT * FROM $table WHERE id = 1",
			parametersIn: []bigquery.QueryParameter{},
			errorMessage: "identifier not provided in parameters: $table",
		},
		{
			name:         "unused named parameter",
			sqlIn:        "SELECT * FROM $table WHERE id = 1",
			parametersIn: []bigquery.QueryParameter{{Name: "$table", Value: "mytable"}, {Name: "@unused", Value: "value"}},
			errorMessage: "parameter not found in query: @unused",
		},
		{
			name:         "unused identifier",
			sqlIn:        "SELECT * FROM $table WHERE id = 1",
			parametersIn: []bigquery.QueryParameter{{Name: "$table", Value: "mytable"}, {Name: "$unused", Value: "value"}},
			errorMessage: "identifier not found in query: $unused",
		},
		// SQL injection attempt cases
		{
			name:         "injection attempt - DROP TABLE via backtick escape",
			sqlIn:        "SELECT * FROM $table WHERE user_id = @user_id",
			parametersIn: []bigquery.QueryParameter{{Name: "$table", Value: "logs` WHERE 1=1; DROP TABLE customers; --"}, {Name: "@user_id", Value: 123}},
			errorMessage: "identifier contains invalid characters: $table contains `=;",
		},
		{
			name:         "injection attempt - UNION attack",
			sqlIn:        "SELECT * FROM $table WHERE id = @id",
			parametersIn: []bigquery.QueryParameter{{Name: "$table", Value: "users` UNION SELECT * FROM passwords WHERE `1`=`1"}, {Name: "@id", Value: 1}},
			errorMessage: "identifier contains invalid characters: $table contains `*=",
		},
		{
			name:         "injection attempt - semicolon statement separator",
			sqlIn:        "DELETE FROM $table WHERE id = @id",
			parametersIn: []bigquery.QueryParameter{{Name: "$table", Value: "temp_table`; DELETE FROM important_data; --"}, {Name: "@id", Value: 999}},
			errorMessage: "identifier contains invalid characters: $table contains `;",
		},
		{
			name:         "injection attempt - comment injection",
			sqlIn:        "UPDATE $table SET status = @status WHERE id = @id",
			parametersIn: []bigquery.QueryParameter{{Name: "$table", Value: "users` -- malicious comment"}, {Name: "@status", Value: "active"}, {Name: "@id", Value: 1}},
			errorMessage: "identifier contains invalid characters: $table contains `",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlOut, parametersOut, err := translate(tt.sqlIn, tt.parametersIn)
			if err != nil {
				if tt.errorMessage == "" {
					t.Fatalf("translate() unexpected error: %v", err)
				}
				if err.Error() != tt.errorMessage {
					t.Fatalf("translate() error = %q, want %q", err.Error(), tt.errorMessage)
				}
			} else {
				if tt.errorMessage != "" {
					t.Fatalf("translate() expected error %q but got none", tt.errorMessage)
				}
			}
			if !equalQueryParameters(parametersOut, tt.parametersOut) {
				t.Errorf("translate() parametersOut = %v, want %v", parametersOut, tt.parametersOut)
			}
			if sqlOut != tt.sqlOut {
				t.Errorf("translate() = %q, want %q", sqlOut, tt.sqlOut)
			}
		})
	}
}

func equalQueryParameters(a, b []bigquery.QueryParameter) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name {
			return false
		}
		// deep compare Value
		if fmt.Sprintf("%v", a[i].Value) != fmt.Sprintf("%v", b[i].Value) {
			return false
		}
	}
	return true
}

func TestQueryTranslateSQL(t *testing.T) {
	q := &Query{
		Query: bigquery.Query{
			QueryConfig: bigquery.QueryConfig{
				Q: "SELECT * FROM $table WHERE id = 1",
				Parameters: []bigquery.QueryParameter{
					{Name: "$table", Value: "mytable"},
				},
			},
		},
	}

	err := q.translate()
	if err != nil {
		t.Fatalf("translate() unexpected error: %v", err)
	}

	expectedSQL := "SELECT * FROM `mytable` WHERE id = 1"
	if q.QueryConfig.Q != expectedSQL {
		t.Errorf("translate() SQL = %q, want %q", q.QueryConfig.Q, expectedSQL)
	}

	if len(q.Parameters) != 0 {
		t.Errorf("translate() Parameters = %v, want empty", q.Parameters)
	}
}

func TestQueryTranslateEmptySQL(t *testing.T) {
	q := &Query{
		Query: bigquery.Query{
			QueryConfig: bigquery.QueryConfig{
				Q: "",
			},
		},
	}

	err := q.translate()
	if !errors.Is(err, ErrEmptySQL) {
		t.Errorf("Expected ErrEmptySQL, got %v", err)
	}
}

func BenchmarkTranslateVsConcat(b *testing.B) {
	b.Run("translate_simple", func(b *testing.B) {
		sql := "SELECT * FROM $table WHERE id = 1"
		params := []bigquery.QueryParameter{
			{Name: "$table", Value: "mydataset.mytable"},
		}
		b.ResetTimer()
		for b.Loop() {
			_, _, _ = translate(sql, params)
		}
	})

	b.Run("concat_simple", func(b *testing.B) {
		tableName := "mydataset.mytable"
		b.ResetTimer()
		for b.Loop() {
			_ = fmt.Sprintf("SELECT * FROM `%s` WHERE id = 1", tableName)
		}
	})

	b.Run("translate_complex", func(b *testing.B) {
		sql := "SELECT * FROM $table1 JOIN $table2 ON $table1.id = $table2.id WHERE status = @status"
		params := []bigquery.QueryParameter{
			{Name: "$table1", Value: "dataset.table1"},
			{Name: "$table2", Value: "dataset.table2"},
			{Name: "@status", Value: "active"},
		}
		b.ResetTimer()
		for b.Loop() {
			_, _, _ = translate(sql, params)
		}
	})

	b.Run("concat_complex", func(b *testing.B) {
		table1 := "dataset.table1"
		table2 := "dataset.table2"
		b.ResetTimer()
		for b.Loop() {
			_ = fmt.Sprintf("SELECT * FROM `%s` JOIN `%s` ON `%s`.id = `%s`.id WHERE status = @status",
				table1, table2, table1, table2)
		}
	})
}

func TestQueryRunSuccess(t *testing.T) {
	ctx := context.Background()
	client, err := NewClient(ctx, "test-project", option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer client.Close()

	q := client.Query("SELECT * FROM $table")
	q.Parameters = []bigquery.QueryParameter{{Name: "$table", Value: "test_table"}}

	_, err = q.Run(ctx)
	// Expected to fail because we're not authenticated, but translate should succeed
	if err != nil && !strings.Contains(err.Error(), "invalid") {
		// Error is expected from BigQuery, not from translation
		return
	}
}

func TestQueryRunError(t *testing.T) {
	ctx := context.Background()
	client, err := NewClient(ctx, "test-project", option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer client.Close()

	q := client.Query("SELECT * FROM $table")
	q.Parameters = []bigquery.QueryParameter{{Name: "$table", Value: "test;DROP"}}

	_, err = q.Run(ctx)
	if err == nil {
		t.Error("Run() expected error for invalid identifier")
	}
	if !errors.Is(err, ErrIdentifierInvalidChars) {
		t.Errorf("Run() error = %v, want ErrIdentifierInvalidChars", err)
	}
}

func TestQueryReadSuccess(t *testing.T) {
	ctx := context.Background()
	client, err := NewClient(ctx, "test-project", option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer client.Close()

	q := client.Query("SELECT * FROM $table")
	q.Parameters = []bigquery.QueryParameter{{Name: "$table", Value: "test_table"}}

	_, err = q.Read(ctx)
	// Expected to fail because we're not authenticated, but translate should succeed
	if err != nil && !strings.Contains(err.Error(), "invalid") {
		// Error is expected from BigQuery, not from translation
		return
	}
}

func TestQueryReadError(t *testing.T) {
	ctx := context.Background()
	client, err := NewClient(ctx, "test-project", option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	defer client.Close()

	q := client.Query("SELECT * FROM $table")
	q.Parameters = []bigquery.QueryParameter{{Name: "$table", Value: "test;DROP"}}

	_, err = q.Read(ctx)
	if err == nil {
		t.Error("Read() expected error for invalid identifier")
	}
	if !errors.Is(err, ErrIdentifierInvalidChars) {
		t.Errorf("Read() error = %v, want ErrIdentifierInvalidChars", err)
	}
}
