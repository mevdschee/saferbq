package saferbq

import (
	"fmt"
	"testing"

	"cloud.google.com/go/bigquery"
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
			name:          "identifier replacement only",
			sqlIn:         "SELECT * FROM $tablename WHERE id = 1",
			parametersIn:  []bigquery.QueryParameter{{Name: "$tablename", Value: "myproject.mydataset.mytable"}},
			sqlOut:        "SELECT * FROM `myproject.mydataset.mytable` WHERE id = 1",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "identifier replacement and positional parameter",
			sqlIn:         "SELECT * FROM $tablename WHERE id = ?",
			parametersIn:  []bigquery.QueryParameter{{Name: "$tablename", Value: "myproject.mydataset.mytable"}, {Value: 1}},
			sqlOut:        "SELECT * FROM `myproject.mydataset.mytable` WHERE id = ?",
			parametersOut: []bigquery.QueryParameter{{Value: 1}},
		},
		{
			name:          "identifier replacement and positional parameter",
			sqlIn:         "SELECT * FROM $tablename WHERE id = ?",
			parametersIn:  []bigquery.QueryParameter{{Name: "$tablename", Value: "myproject.mydataset.mytable"}, {Value: 1}},
			sqlOut:        "SELECT * FROM `myproject.mydataset.mytable` WHERE id = ?",
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
			name:          "single identifier with multiple dots",
			sqlIn:         "SELECT * FROM $tablename WHERE id = 1",
			parametersIn:  []bigquery.QueryParameter{{Name: "$tablename", Value: "myproject.mydataset.mytable"}},
			sqlOut:        "SELECT * FROM `myproject.mydataset.mytable` WHERE id = 1",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "multiple identifiers",
			sqlIn:         "SELECT * FROM $table1 JOIN $table2 ON $table1.id = $table2.id",
			parametersIn:  []bigquery.QueryParameter{{Name: "$table1", Value: "dataset.table1"}, {Name: "$table2", Value: "dataset.table2"}},
			sqlOut:        "SELECT * FROM `dataset.table1` JOIN `dataset.table2` ON `dataset.table1`.id = `dataset.table2`.id",
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
		// TODO: slice of identifiers not yet supported
		//{
		//	name:          "slice of identifiers",
		//	sqlIn:         "GRANT $roles ON DATASET `your_project.your_dataset` TO @user;",
		//	parametersIn:  []bigquery.QueryParameter{{Name: "$roles", Value: []string{"roles/bigquery.dataViewer", "roles/bigquery.dataEditor"}}, {Name: "@user", Value: "user@example.com"}},
		//	sqlOut:        "GRANT `roles/bigquery.dataViewer`, `roles/bigquery.dataEditor` ON DATASET `your_project.your_dataset` TO @user;",
		//	parametersOut: []bigquery.QueryParameter{{Name: "user", Value: "user@example.com"}},
		//},
		// TODO: slice expansion for parameters not yet supported
		//{
		//	name:          "slice of parameter values",
		//	sqlIn:         "SELECT * FROM `mytable` WHERE corpus IN (@corpus)",
		//	parametersIn:  []bigquery.QueryParameter{{Name: "@corpus", Value: []string{"value1", "value2"}}},
		//	sqlOut:        "SELECT * FROM `mytable` WHERE corpus IN (@corpus_1, @corpus_2)",
		//	parametersOut: []bigquery.QueryParameter{{Name: "corpus_1", Value: "value1"}, {Name: "corpus_2", Value: "value2"}},
		//},
		// Test cases from documentation examples
		{
			name:          "positional params from doc - shakespeare corpus",
			sqlIn:         "SELECT word, word_count FROM $table WHERE corpus = ? AND word_count >= ? ORDER BY word_count DESC",
			parametersIn:  []bigquery.QueryParameter{{Name: "$table", Value: "bigquery-public-data.samples.shakespeare"}, {Value: "romeoandjuliet"}, {Value: 250}},
			sqlOut:        "SELECT word, word_count FROM `bigquery_public_data.samples.shakespeare` WHERE corpus = ? AND word_count >= ? ORDER BY word_count DESC",
			parametersOut: []bigquery.QueryParameter{{Value: "romeoandjuliet"}, {Value: 250}},
		},
		{
			name:          "array params from doc - usa names",
			sqlIn:         "SELECT name, sum(number) as count FROM $project.$dataset.$table WHERE gender = @gender AND state IN UNNEST(@states) GROUP BY name ORDER BY count DESC LIMIT 10",
			parametersIn:  []bigquery.QueryParameter{{Name: "$project", Value: "bigquery-public-data"}, {Name: "$dataset", Value: "usa_names"}, {Name: "$table", Value: "usa_1910_2013"}, {Name: "@gender", Value: "M"}, {Name: "@states", Value: []string{"WA", "WI", "WV", "WY"}}},
			sqlOut:        "SELECT name, sum(number) as count FROM `bigquery_public_data`.`usa_names`.`usa_1910_2013` WHERE gender = @gender AND state IN UNNEST(@states) GROUP BY name ORDER BY count DESC LIMIT 10",
			parametersOut: []bigquery.QueryParameter{{Name: "gender", Value: "M"}, {Name: "states", Value: []string{"WA", "WI", "WV", "WY"}}},
		},
		{
			name:          "basic query from doc - texas names",
			sqlIn:         "SELECT name FROM $project.$dataset.$table WHERE state = @state LIMIT 100",
			parametersIn:  []bigquery.QueryParameter{{Name: "$project", Value: "bigquery-public-data"}, {Name: "$dataset", Value: "usa_names"}, {Name: "$table", Value: "usa_1910_2013"}, {Name: "@state", Value: "TX"}},
			sqlOut:        "SELECT name FROM `bigquery_public_data`.`usa_names`.`usa_1910_2013` WHERE state = @state LIMIT 100",
			parametersOut: []bigquery.QueryParameter{{Name: "state", Value: "TX"}},
		},
		{
			name:          "batch query from doc - aggregate shakespeare",
			sqlIn:         "SELECT corpus, SUM(word_count) as total_words, COUNT(1) as unique_words FROM $project.$dataset.$table GROUP BY corpus",
			parametersIn:  []bigquery.QueryParameter{{Name: "$project", Value: "bigquery-public-data"}, {Name: "$dataset", Value: "samples"}, {Name: "$table", Value: "shakespeare"}},
			sqlOut:        "SELECT corpus, SUM(word_count) as total_words, COUNT(1) as unique_words FROM `bigquery_public_data`.`samples`.`shakespeare` GROUP BY corpus",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "query from doc - usa names aggregate",
			sqlIn:         "SELECT name, gender, SUM(number) AS total FROM $table GROUP BY name, gender ORDER BY total DESC LIMIT 10",
			parametersIn:  []bigquery.QueryParameter{{Name: "$table", Value: "bigquery-public-data.usa_names.usa_1910_2013"}},
			sqlOut:        "SELECT name, gender, SUM(number) AS total FROM `bigquery_public_data.usa_names.usa_1910_2013` GROUP BY name, gender ORDER BY total DESC LIMIT 10",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "dry run query from doc - name count by state",
			sqlIn:         "SELECT name, COUNT(*) as name_count FROM $project.$dataset.$table WHERE state = @state GROUP BY name",
			parametersIn:  []bigquery.QueryParameter{{Name: "$project", Value: "bigquery-public-data"}, {Name: "$dataset", Value: "usa_names"}, {Name: "$table", Value: "usa_1910_2013"}, {Name: "@state", Value: "WA"}},
			sqlOut:        "SELECT name, COUNT(*) as name_count FROM `bigquery_public_data`.`usa_names`.`usa_1910_2013` WHERE state = @state GROUP BY name",
			parametersOut: []bigquery.QueryParameter{{Name: "state", Value: "WA"}}},
		{
			name:          "create table with identifiers",
			sqlIn:         "CREATE TABLE IF NOT EXISTS $dataset.$table (id INT64, name STRING, created_at TIMESTAMP)",
			parametersIn:  []bigquery.QueryParameter{{Name: "$dataset", Value: "mydataset"}, {Name: "$table", Value: "mynew-table"}},
			sqlOut:        "CREATE TABLE IF NOT EXISTS `mydataset`.`mynew_table` (id INT64, name STRING, created_at TIMESTAMP)",
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
			sqlIn:         "UPDATE $table SET status = @status WHERE id = @id",
			parametersIn:  []bigquery.QueryParameter{{Name: "$table", Value: "mydataset.mytable"}, {Name: "@status", Value: "active"}, {Name: "@id", Value: 1}},
			sqlOut:        "UPDATE `mydataset.mytable` SET status = @status WHERE id = @id",
			parametersOut: []bigquery.QueryParameter{{Name: "status", Value: "active"}, {Name: "id", Value: 1}},
		},
		{
			name:          "insert with identifier",
			sqlIn:         "INSERT INTO $table (id, name) VALUES (@id, @name)",
			parametersIn:  []bigquery.QueryParameter{{Name: "$table", Value: "mydataset.mytable"}, {Name: "@id", Value: 1}, {Name: "@name", Value: "test"}},
			sqlOut:        "INSERT INTO `mydataset.mytable` (id, name) VALUES (@id, @name)",
			parametersOut: []bigquery.QueryParameter{{Name: "id", Value: 1}, {Name: "name", Value: "test"}},
		},
		{
			name:          "with cte and identifier",
			sqlIn:         "WITH cte AS (SELECT * FROM $table WHERE active = true) SELECT * FROM cte",
			parametersIn:  []bigquery.QueryParameter{{Name: "$table", Value: "mydataset.mytable"}},
			sqlOut:        "WITH cte AS (SELECT * FROM `mydataset.mytable` WHERE active = true) SELECT * FROM cte",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "union with identifiers",
			sqlIn:         "SELECT * FROM $table1 UNION ALL SELECT * FROM $table2",
			parametersIn:  []bigquery.QueryParameter{{Name: "$table1", Value: "dataset.table1"}, {Name: "$table2", Value: "dataset.table2"}},
			sqlOut:        "SELECT * FROM `dataset.table1` UNION ALL SELECT * FROM `dataset.table2`",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "subquery with identifier",
			sqlIn:         "SELECT * FROM (SELECT id FROM $table WHERE status = @status) WHERE id > @id",
			parametersIn:  []bigquery.QueryParameter{{Name: "$table", Value: "mydataset.mytable"}, {Name: "@status", Value: "active"}, {Name: "@id", Value: 100}},
			sqlOut:        "SELECT * FROM (SELECT id FROM `mydataset.mytable` WHERE status = @status) WHERE id > @id",
			parametersOut: []bigquery.QueryParameter{{Name: "status", Value: "active"}, {Name: "id", Value: 100}},
		},
		{
			name:          "window function with identifier",
			sqlIn:         "SELECT id, ROW_NUMBER() OVER (ORDER BY created_at) as row_num FROM $table",
			parametersIn:  []bigquery.QueryParameter{{Name: "$table", Value: "mydataset.mytable"}},
			sqlOut:        "SELECT id, ROW_NUMBER() OVER (ORDER BY created_at) as row_num FROM `mydataset.mytable`",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "case statement with identifier and params",
			sqlIn:         "SELECT id, CASE WHEN status = @status1 THEN 'active' WHEN status = @status2 THEN 'inactive' END as status_label FROM $dataset.$table",
			parametersIn:  []bigquery.QueryParameter{{Name: "$dataset", Value: "mydataset"}, {Name: "$table", Value: "mytable"}, {Name: "@status1", Value: 1}, {Name: "@status2", Value: 0}},
			sqlOut:        "SELECT id, CASE WHEN status = @status1 THEN 'active' WHEN status = @status2 THEN 'inactive' END as status_label FROM `mydataset`.`mytable`",
			parametersOut: []bigquery.QueryParameter{{Name: "status1", Value: 1}, {Name: "status2", Value: 0}},
		},
		// Error cases - invalid parameter names
		{
			name:         "invalid parameter name",
			sqlIn:        "SELECT * FROM table WHERE id = 1",
			parametersIn: []bigquery.QueryParameter{{Name: "corpus", Value: "corpus_value"}},
			errorMessage: "invalid parameter name corpus: must start with @ or $",
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
			errorMessage: "parameter @status not provided in parameters",
		},
		{
			name:         "missing identifier",
			sqlIn:        "SELECT * FROM $table WHERE id = 1",
			parametersIn: []bigquery.QueryParameter{},
			errorMessage: "identifier $table not provided in parameters",
		},
		{
			name:         "unused named parameter",
			sqlIn:        "SELECT * FROM $table WHERE id = 1",
			parametersIn: []bigquery.QueryParameter{{Name: "$table", Value: "mytable"}, {Name: "@unused", Value: "value"}},
			errorMessage: "parameter @unused not found in query",
		},
		{
			name:         "unused identifier",
			sqlIn:        "SELECT * FROM $table WHERE id = 1",
			parametersIn: []bigquery.QueryParameter{{Name: "$table", Value: "mytable"}, {Name: "$unused", Value: "value"}},
			errorMessage: "identifier $unused not found in query",
		},
		// SQL injection attempt cases
		{
			name:          "injection attempt - DROP TABLE via backtick escape",
			sqlIn:         "SELECT * FROM $table WHERE user_id = @user_id",
			parametersIn:  []bigquery.QueryParameter{{Name: "$table", Value: "logs` WHERE 1=1; DROP TABLE customers; --"}, {Name: "@user_id", Value: 123}},
			sqlOut:        "SELECT * FROM `logs__WHERE_1_1__DROP_TABLE_customers____` WHERE user_id = @user_id",
			parametersOut: []bigquery.QueryParameter{{Name: "user_id", Value: 123}},
		},
		{
			name:          "injection attempt - UNION attack",
			sqlIn:         "SELECT * FROM $table WHERE id = @id",
			parametersIn:  []bigquery.QueryParameter{{Name: "$table", Value: "users` UNION SELECT * FROM passwords WHERE `1`=`1"}, {Name: "@id", Value: 1}},
			sqlOut:        "SELECT * FROM `users__UNION_SELECT___FROM_passwords_WHERE__1___1` WHERE id = @id",
			parametersOut: []bigquery.QueryParameter{{Name: "id", Value: 1}},
		},
		{
			name:          "injection attempt - semicolon statement separator",
			sqlIn:         "DELETE FROM $table WHERE id = @id",
			parametersIn:  []bigquery.QueryParameter{{Name: "$table", Value: "temp_table`; DELETE FROM important_data; --"}, {Name: "@id", Value: 999}},
			sqlOut:        "DELETE FROM `temp_table___DELETE_FROM_important_data____` WHERE id = @id",
			parametersOut: []bigquery.QueryParameter{{Name: "id", Value: 999}},
		},
		{
			name:          "injection attempt - comment injection",
			sqlIn:         "UPDATE $table SET status = @status WHERE id = @id",
			parametersIn:  []bigquery.QueryParameter{{Name: "$table", Value: "users` -- malicious comment"}, {Name: "@status", Value: "active"}, {Name: "@id", Value: 1}},
			sqlOut:        "UPDATE `users_____malicious_comment` SET status = @status WHERE id = @id",
			parametersOut: []bigquery.QueryParameter{{Name: "status", Value: "active"}, {Name: "id", Value: 1}},
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
