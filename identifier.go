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
func filterIdentifierChars(s string) (string, string) {
	// start building the result
	var result strings.Builder
	result.Grow(len(s))
	var replaced strings.Builder
	replacedMap := make(map[rune]bool)
	for _, r := range s {
		if unicode.IsLetter(r) ||
			unicode.IsMark(r) ||
			unicode.IsNumber(r) ||
			unicode.In(r, unicode.Pc, unicode.Pd, unicode.Zs) {
			result.WriteRune(r)
		} else {
			result.WriteRune('_')
			if !replacedMap[r] {
				replaced.WriteRune(r)
				replacedMap[r] = true
			}
		}
	}
	return result.String(), replaced.String()
}

// QuoteIdentifier safely quotes a table identifier with backticks.
// This is essential for DDL operations when table names contain backticks,
// special characters, or are reserved words in BigQuery.
// Invalid characters (like backticks) are automatically converted to underscores.
func QuoteIdentifier(identifier any) (result string, replaced string) {
	switch v := identifier.(type) {
	case nil:
		result, replaced = filterIdentifierChars("")
	case string:
		result, replaced = filterIdentifierChars(v)
	default:
		result, replaced = filterIdentifierChars(fmt.Sprintf("%v", identifier))
	}
	return
}
