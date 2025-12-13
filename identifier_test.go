package saferbq

import (
	"testing"
)

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name       string
		identifier interface{}
		expectErr  string
		expected   string
	}{
		{
			name:       "empty",
			identifier: "",
			expected:   "``",
		},
		{
			name:       "simple",
			identifier: "mytable",
			expected:   "`mytable`",
		},
		{
			name:       "with hyphen",
			identifier: "my-table",
			expected:   "`my_table`",
		},
		{
			name:       "with dot",
			identifier: "my.dataset.table",
			expected:   "`my.dataset.table`",
		},
		{
			name:       "with slashes",
			identifier: "my/dataset/table",
			expected:   "`my/dataset/table`",
		},
		{
			name:       "with special chars",
			identifier: "my$table@name!",
			expected:   "`my_table_name_`",
		},
		{
			name:       "already quoted",
			identifier: "`mytable`",
			expected:   "`_mytable_`",
		},
		{
			name:       "slice of strings",
			identifier: []string{"my-project", "my-dataset", "my-table"},
			expected:   "`my_project`, `my_dataset`, `my_table`",
		},
		{
			name:       "non-string identifier",
			identifier: 12345,
			expected:   "`12345`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := quoteIdentifier(tt.identifier)
			if result != tt.expected {
				t.Errorf("quoteIdentifier(%v) = %q, want %q", tt.identifier, result, tt.expected)
			}
		})
	}
}
