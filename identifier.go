package saferbq

import (
	"fmt"
	"strings"
)

// characters that are allowed in unquoted identifiers
const identifierChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_/."

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
// Invalid characters (like hyphens) are automatically converted to underscores.
func quoteIdentifier(identifier any) string {
	// Replace any invalid characters with underscores
	var result string
	// Detect slice of any type
	slice, ok := identifier.([]any)
	if ok {
		parts := make([]string, len(slice))
		for i, part := range slice {
			parts[i] = filterIdentifierChars(fmt.Sprintf("%v", part))
		}
		result = strings.Join(parts, "`, `")
		return "`" + result + "`"
	}
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
