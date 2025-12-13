package saferbq

import (
	"testing"
)

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name          string
		identifierIn  any
		identifierOut string
	}{
		{
			name:          "empty",
			identifierIn:  "",
			identifierOut: "``",
		},
		{
			name:          "simple",
			identifierIn:  "mytable",
			identifierOut: "`mytable`",
		},
		{
			name:          "with hyphen",
			identifierIn:  "my-table",
			identifierOut: "`my-table`",
		},
		{
			name:          "with dot",
			identifierIn:  "my.dataset.table",
			identifierOut: "`my_dataset_table`",
		},
		{
			name:          "with space",
			identifierIn:  "my table",
			identifierOut: "`my table`",
		},
		{
			name:          "with diacritics",
			identifierIn:  "my táble",
			identifierOut: "`my táble`",
		},
		{
			name:          "with unicode letters",
			identifierIn:  "表格",
			identifierOut: "`表格`",
		},
		{
			name:          "with sql injection attempt",
			identifierIn:  "mytable`; DROP TABLE",
			identifierOut: "`mytable__ DROP TABLE`",
		},
		{
			name:          "with number",
			identifierIn:  "mytable123",
			identifierOut: "`mytable123`",
		},
		{
			name:          "with slashes",
			identifierIn:  "my/dataset/table",
			identifierOut: "`my_dataset_table`",
		},
		{
			name:          "with special chars",
			identifierIn:  "my$table@name!",
			identifierOut: "`my_table_name_`",
		},
		{
			name:          "already quoted",
			identifierIn:  "`mytable`",
			identifierOut: "`_mytable_`",
		},
		{
			name:          "non-string identifier",
			identifierIn:  12345,
			identifierOut: "`12345`",
		},
		{
			name:          "identifier with emoji",
			identifierIn:  "mytable😀",
			identifierOut: "`mytable_`",
		},
		{
			name:          "identifier with newline",
			identifierIn:  "mytable\nname",
			identifierOut: "`mytable_name`",
		},
		{
			name:          "identifier with all invalid chars",
			identifierIn:  "!@#$%^&*()./<>?\\|`~",
			identifierOut: "`___________________`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			identifierOut := quoteIdentifier(tt.identifierIn)
			if identifierOut != tt.identifierOut {
				t.Errorf("quoteIdentifier(%v) = %q, want %q", tt.identifierIn, identifierOut, tt.identifierOut)
			}
		})
	}
}

func BenchmarkQuoteIdentifier(b *testing.B) {
	testCases := []struct {
		name  string
		input any
	}{
		{"simple", "mytable"},
		{"complex", "my-project.my-dataset.my-table"},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for b.Loop() {
				_ = quoteIdentifier(tc.input)
			}
		})
	}
}
