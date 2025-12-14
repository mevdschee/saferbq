package saferbq

import (
	"fmt"
	"strings"
	"unicode"
)

const (
	// underscore is the character used to replace invalid identifier characters
	underscore = '_'
	// backtick is the character used to quote identifiers
	backtick = '`'
)

// isValidIdentifierChar checks if a rune is valid for BigQuery identifiers.
// Valid characters are defined by Unicode categories:
//   - L (letter): any Unicode letter
//   - M (mark): combining marks and diacritics
//   - N (number): any numeric digit
//   - Pc (connector punctuation): underscore and similar
//   - Pd (dash punctuation): hyphen and dash characters
//   - Zs (space separator): space characters
//
// This follows BigQuery's table naming rules from:
// https://docs.cloud.google.com/bigquery/docs/tables#table_naming
//
// Example valid characters: a-z, A-Z, 0-9, _, -, space, 表, á, ñ
// Example invalid characters: `, ;, /, ., *, =, @, #
func isValidIdentifierChar(r rune) bool {
	return unicode.IsLetter(r) ||
		unicode.IsMark(r) ||
		unicode.IsNumber(r) ||
		unicode.In(r, unicode.Pc, unicode.Pd, unicode.Zs)
}

// filterIdentifierChars validates and sanitizes identifier strings.
// It iterates through each rune in the input string and:
//   - Keeps valid characters unchanged
//   - Replaces invalid characters with underscores
//   - Tracks which characters were replaced (for error reporting)
//
// Returns the sanitized identifier and a string containing all unique
// characters that were replaced.
//
// This is an internal function used by QuoteIdentifier.
func filterIdentifierChars(s string) (string, string) {
	// start building the result
	var result strings.Builder
	result.Grow(len(s))
	var replaced strings.Builder
	replacedMap := make(map[rune]bool)
	for _, r := range s {
		if isValidIdentifierChar(r) {
			result.WriteRune(r)
		} else {
			result.WriteRune(underscore)
			if !replacedMap[r] {
				replaced.WriteRune(r)
				replacedMap[r] = true
			}
		}
	}
	return result.String(), replaced.String()
}

// QuoteIdentifier safely quotes a table identifier with backticks.
// This is essential for DDL operations when table names may contain
// special characters or are reserved words in BigQuery.
//
// Invalid characters are replaced with underscores and returned in the
// second return value. Valid characters include:
//   - Unicode letters, marks, and numbers
//   - Underscores, dashes, and spaces
//
// The function accepts any type and converts it to a string.
//
// Example:
//
//	quoted, replaced := QuoteIdentifier("my-table")
//	// quoted = "`my-table`", replaced = ""
//
//	quoted, replaced := QuoteIdentifier("table;DROP")
//	// quoted = "`table_DROP`", replaced = ";"
//
// Returns the quoted identifier and a string containing all replaced characters.
func QuoteIdentifier(identifier any) (string, string) {
	var result, replaced string
	switch v := identifier.(type) {
	case nil:
		result, replaced = filterIdentifierChars("")
	case string:
		result, replaced = filterIdentifierChars(v)
	default:
		result, replaced = filterIdentifierChars(fmt.Sprintf("%v", identifier))
	}
	return string(backtick) + result + string(backtick), replaced
}
