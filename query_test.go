package saferbq

import (
	"fmt"
	"testing"

	"cloud.google.com/go/bigquery"
)

func TestQueryTranslateWithIdentifiers(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		parametersIn  []bigquery.QueryParameter
		expected      string
		parametersOut []bigquery.QueryParameter
	}{
		{
			name:          "identifier replacement only",
			sql:           "SELECT * FROM $tablename WHERE id = 1",
			parametersIn:  []bigquery.QueryParameter{{Name: "$tablename", Value: "my-project.my-dataset.my-table"}},
			expected:      "SELECT * FROM `my_project.my_dataset.my_table` WHERE id = 1",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "identifier replacement and positional parameter",
			sql:           "SELECT * FROM $tablename WHERE id = ?",
			parametersIn:  []bigquery.QueryParameter{{Name: "$tablename", Value: "my-project.my-dataset.my-table"}, {Value: 1}},
			expected:      "SELECT * FROM `my_project.my_dataset.my_table` WHERE id = ?",
			parametersOut: []bigquery.QueryParameter{{Value: 1}},
		},
		{
			name:          "identifier replacement and positional parameter",
			sql:           "SELECT * FROM $tablename WHERE id = ?",
			parametersIn:  []bigquery.QueryParameter{{Name: "$tablename", Value: "my-project.my-dataset.my-table"}, {Value: 1}},
			expected:      "SELECT * FROM `my_project.my_dataset.my_table` WHERE id = ?",
			parametersOut: []bigquery.QueryParameter{{Value: 1}},
		},
		{
			name:          "multiple identifiers with a dot",
			sql:           "SELECT * FROM $project.$dataset.$table WHERE id = 1",
			parametersIn:  []bigquery.QueryParameter{{Name: "$project", Value: "my-project"}, {Name: "$dataset", Value: "my-dataset"}, {Name: "$table", Value: "my-table"}},
			expected:      "SELECT * FROM `my_project`.`my_dataset`.`my_table` WHERE id = 1",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "single identifier with multiple dots",
			sql:           "SELECT * FROM $tablename WHERE id = 1",
			parametersIn:  []bigquery.QueryParameter{{Name: "$tablename", Value: "my-project.my-dataset.my-table"}},
			expected:      "SELECT * FROM `my_project.my_dataset.my_table` WHERE id = 1",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "multiple identifiers",
			sql:           "SELECT * FROM $table1 JOIN $table2 ON $table1.id = $table2.id",
			parametersIn:  []bigquery.QueryParameter{{Name: "$table1", Value: "dataset.table1"}, {Name: "$table2", Value: "dataset.table2"}},
			expected:      "SELECT * FROM `dataset.table1` JOIN `dataset.table2` ON `dataset.table1`.id = `dataset.table2`.id",
			parametersOut: []bigquery.QueryParameter{},
		},
		{
			name:          "@ parameter stays unchanged",
			sql:           "SELECT * FROM table WHERE corpus = @corpus",
			parametersIn:  []bigquery.QueryParameter{{Name: "@corpus", Value: "corpus_value"}},
			expected:      "SELECT * FROM table WHERE corpus = @corpus",
			parametersOut: []bigquery.QueryParameter{{Name: "corpus", Value: "corpus_value"}},
		},
		{
			name:          "mixed @ and $ parameters",
			sql:           "SELECT * FROM $tablename WHERE corpus = @corpus",
			parametersIn:  []bigquery.QueryParameter{{Name: "$tablename", Value: "my-table"}, {Name: "@corpus", Value: "corpus_value"}},
			expected:      "SELECT * FROM `my_table` WHERE corpus = @corpus",
			parametersOut: []bigquery.QueryParameter{{Name: "corpus", Value: "corpus_value"}},
		},
		{
			name:          "slice of identifiers",
			sql:           "GRANT $roles ON DATASET `your_project.your_dataset` TO @user;",
			parametersIn:  []bigquery.QueryParameter{{Name: "$roles", Value: []string{"roles/bigquery.dataViewer", "roles/bigquery.dataEditor"}}, {Name: "@user", Value: "user@example.com"}},
			expected:      "GRANT `roles/bigquery.dataViewer`, `roles/bigquery.dataEditor` ON DATASET `your_project.your_dataset` TO @user;",
			parametersOut: []bigquery.QueryParameter{{Name: "user", Value: "user@example.com"}},
		},
		{
			name:          "slice of parameter values",
			sql:           "SELECT * FROM `my_table` WHERE corpus IN (@corpus)",
			parametersIn:  []bigquery.QueryParameter{{Name: "@corpus", Value: []string{"value1", "value2"}}},
			expected:      "SELECT * FROM `my_table` WHERE corpus IN (@corpus_1, @corpus_2)",
			parametersOut: []bigquery.QueryParameter{{Name: "corpus_1", Value: "value1"}, {Name: "corpus_2", Value: "value2"}},
		},
		{
			name:          "slice of parameter values ignored",
			sql:           "SELECT * FROM `my_table` WHERE corpus IN (@corpus)",
			parametersIn:  []bigquery.QueryParameter{{Name: "corpus", Value: []string{"value1", "value2"}}},
			expected:      "SELECT * FROM `my_table` WHERE corpus IN (@corpus)",
			parametersOut: []bigquery.QueryParameter{{Name: "corpus", Value: []string{"value1", "value2"}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, paramsOut, err := translate(tt.sql, tt.parametersIn)
			if err != nil {
				t.Fatalf("translate() error = %v", err)
			}
			if !equalQueryParameters(paramsOut, tt.parametersOut) {
				t.Errorf("translate() parametersOut = %v, want %v", paramsOut, tt.parametersOut)
			}
			if result != tt.expected {
				t.Errorf("translate() = %q, want %q", result, tt.expected)
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
