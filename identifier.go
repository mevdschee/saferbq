package saferbq

import (
	"fmt"
	"strings"
)

// characters that are allowed in unquoted identifiers
const identifierChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"

func filterIdentifierChars(s string) string {
	return strings.Map(func(r rune) rune {
		if strings.ContainsRune(identifierChars, r) {
			return r
		}
		return '_'
	}, s)
}

// QuoteIdentifier safely quotes a table identifier with backticks.
// This is essential for DDL operations when table names contain hyphens,
// special characters, or are reserved words in BigQuery.
// Invalid characters (like hyphens, dots) are automatically converted to underscores.
func quoteIdentifier(identifier interface{}) string {
	// Replace any invalid characters with underscores
	var result string
	switch v := identifier.(type) {
	case string:
		result = filterIdentifierChars(v)
	case []string:
		// foreach entry in the slice, filter chars and join with underscore
		parts := make([]string, len(v))
		for i, part := range v {
			parts[i] = filterIdentifierChars(part)
		}
		result = strings.Join(parts, "`, `")
	default:
		result = filterIdentifierChars(fmt.Sprintf("%v", identifier))
	}
	return "`" + result + "`"
}
