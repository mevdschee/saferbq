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
			name:          "nil",
			identifierIn:  nil,
			identifierOut: "``",
		},
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
			name:          "control characters only (0-31,127)",
			identifierIn:  "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1A\x1B\x1C\x1D\x1E\x1F\x7F",
			identifierOut: "`_________________________________`",
		},
		{
			name:          "printable 7 bit ASCII (32-126)",
			identifierIn:  " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~",
			identifierOut: "` ____________-__0123456789_______ABCDEFGHIJKLMNOPQRSTUVWXYZ______abcdefghijklmnopqrstuvwxyz____`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			identifierOut := QuoteIdentifier(tt.identifierIn)
			if identifierOut != tt.identifierOut {
				t.Errorf("QuoteIdentifier(%v) = %q, want %q", tt.identifierIn, identifierOut, tt.identifierOut)
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
				_ = QuoteIdentifier(tc.input)
			}
		})
	}
}
