package saferbq

import (
	"testing"
)

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name          string
		identifierIn  any
		identifierOut string
		replaced      string
	}{
		{
			name:          "nil",
			identifierIn:  nil,
			identifierOut: "``",
			replaced:      "",
		},
		{
			name:          "empty",
			identifierIn:  "",
			identifierOut: "``",
			replaced:      "",
		},
		{
			name:          "simple",
			identifierIn:  "mytable",
			identifierOut: "`mytable`",
			replaced:      "",
		},
		{
			name:          "with hyphen",
			identifierIn:  "my-table",
			identifierOut: "`my-table`",
			replaced:      "",
		},
		{
			name:          "with dot",
			identifierIn:  "my.dataset.table",
			identifierOut: "`my.dataset.table`",
			replaced:      "",
		},
		{
			name:          "with space",
			identifierIn:  "my table",
			identifierOut: "`my table`",
			replaced:      "",
		},
		{
			name:          "with diacritics",
			identifierIn:  "my táble",
			identifierOut: "`my táble`",
			replaced:      "",
		},
		{
			name:          "with unicode letters",
			identifierIn:  "表格",
			identifierOut: "`表格`",
			replaced:      "",
		},
		{
			name:          "with sql injection attempt",
			identifierIn:  "mytable`; DROP TABLE",
			identifierOut: "`mytable__ DROP TABLE`",
			replaced:      "`;",
		},
		{
			name:          "with number",
			identifierIn:  "mytable123",
			identifierOut: "`mytable123`",
			replaced:      "",
		},
		{
			name:          "with slashes",
			identifierIn:  "my/dataset/table",
			identifierOut: "`my/dataset/table`",
			replaced:      "",
		},
		{
			name:          "with special chars",
			identifierIn:  "my$table@name!",
			identifierOut: "`my_table_name_`",
			replaced:      "$@!",
		},
		{
			name:          "already quoted",
			identifierIn:  "`mytable`",
			identifierOut: "`_mytable_`",
			replaced:      "`",
		},
		{
			name:          "non-string identifier",
			identifierIn:  12345,
			identifierOut: "`12345`",
			replaced:      "",
		},
		{
			name:          "identifier with emoji",
			identifierIn:  "mytable😀",
			identifierOut: "`mytable_`",
			replaced:      "😀",
		},
		{
			name:          "control characters only (0-31,127)",
			identifierIn:  "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1A\x1B\x1C\x1D\x1E\x1F\x7F",
			identifierOut: "`_________________________________`",
			replaced:      "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1A\x1B\x1C\x1D\x1E\x1F\x7F",
		},
		{
			name:          "printable 7 bit ASCII (32-126)",
			identifierIn:  " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~",
			identifierOut: "` ____________-./0123456789:______ABCDEFGHIJKLMNOPQRSTUVWXYZ______abcdefghijklmnopqrstuvwxyz____`",
			replaced:      "!\"#$%&'()*+,;<=>?@[\\]^`{|}~",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			identifierOut, replaced := QuoteIdentifier(tt.identifierIn)
			if identifierOut != tt.identifierOut {
				t.Errorf("QuoteIdentifier(%v) = %q, want %q", tt.identifierIn, identifierOut, tt.identifierOut)
			}
			if replaced != tt.replaced {
				t.Errorf("QuoteIdentifier(%v) returned replaced = %v, want %v", tt.identifierIn, replaced, tt.replaced)
			}
		})
	}
}

func TestIsValidIdentifierChar(t *testing.T) {
	tests := []struct {
		name  string
		char  rune
		valid bool
	}{
		{"letter", 'a', true},
		{"capital letter", 'Z', true},
		{"mark", '\u0301', true}, // combining acute accent
		{"number", '1', true},
		{"underscore", '_', true},
		{"dash", '-', true},
		{"space", ' ', true},
		{"dollar sign", '$', false},
		{"at sign", '@', false},
		{"colon", ':', false},
		{"backtick", '`', false},
		{"semicolon", ';', false},
		{"unicode letter", '表', true},
		{"emoji", '😀', false},
		{"exclamation", '!', false},
		{"dot", '.', false},
		{"slash", '/', false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isValidIdentifierChar(tt.char); got != tt.valid {
				t.Errorf("isValidIdentifierChar(%q) = %v, want %v", tt.char, got, tt.valid)
			}
		})
	}
}

func TestIsPathExpressionSeparatorChar(t *testing.T) {
	tests := []struct {
		name        string
		char        rune
		isSeparator bool
	}{
		{"dot", '.', true},
		{"forward slash", '/', true},
		{"colon", ':', true},
		{"dash", '-', true},
		{"letter", 'a', false},
		{"number", '1', false},
		{"underscore", '_', false},
		{"space", ' ', false},
		{"dollar sign", '$', false},
		{"at sign", '@', false},
		{"backtick", '`', false},
		{"semicolon", ';', false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPathExpressionSeparatorChar(tt.char); got != tt.isSeparator {
				t.Errorf("isPathExpressionSeparatorChar(%q) = %v, want %v", tt.char, got, tt.isSeparator)
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
				_, _ = QuoteIdentifier(tc.input)
			}
		})
	}
}
