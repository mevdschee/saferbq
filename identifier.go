package saferbq

import (
	"fmt"
	"strings"
	"unicode"
)

// filterIdentifierChars filters out Unicode characters that do not fall in category
// - L (letter)
// - M (mark)
// - N (number),
// - Pc (connector, including underscore)
// - Pd (dash)
// - Zs (space).
// and replaces them with underscores.
// This follows BigQuery's table naming rules from:
// https://docs.cloud.google.com/bigquery/docs/tables#table_naming
func filterIdentifierChars(s string) string {
	// start building the result
	var builder strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) ||
			unicode.IsMark(r) ||
			unicode.IsNumber(r) ||
			unicode.In(r, unicode.Pc, unicode.Pd, unicode.Zs) {
			builder.WriteRune(r)
		} else {
			builder.WriteRune('_')
		}
	}
	return builder.String()
}

// quoteIdentifier safely quotes a table identifier with backticks.
// This is essential for DDL operations when table names contain backticks,
// special characters, or are reserved words in BigQuery.
// Invalid characters (like backticks) are automatically converted to underscores.
func quoteIdentifier(identifier any) string {
	// Replace any invalid characters with underscores
	var result string
	switch v := identifier.(type) {
	case string:
		result = filterIdentifierChars(v)
	default:
		result = filterIdentifierChars(fmt.Sprintf("%v", identifier))
	}
	return "`" + result + "`"
}
