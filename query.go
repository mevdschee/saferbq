package saferbq

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"cloud.google.com/go/bigquery"
)

var (
	// ErrInvalidParameterName is returned when a parameter name doesn't start with @ or $.
	ErrInvalidParameterName = errors.New("invalid parameter name")

	// ErrParameterNotFound is returned when a parameter in the params slice is not found in the query.
	ErrParameterNotFound = errors.New("parameter not found in query")

	// ErrParameterNotProvided is returned when a parameter in the query is not provided in the params slice.
	ErrParameterNotProvided = errors.New("parameter not provided in parameters")

	// ErrIdentifierNotFound is returned when an identifier in the params slice is not found in the query.
	ErrIdentifierNotFound = errors.New("identifier not found in query")

	// ErrIdentifierNotProvided is returned when an identifier in the query is not provided in the params slice.
	ErrIdentifierNotProvided = errors.New("identifier not provided in parameters")

	// ErrIdentifierEmpty is returned when an identifier value is empty.
	ErrIdentifierEmpty = errors.New("identifier is empty")

	// ErrIdentifierTooLong is returned when an identifier exceeds the maximum length.
	ErrIdentifierTooLong = errors.New("identifier is too long")

	// ErrIdentifierInvalidChars is returned when an identifier contains invalid characters.
	ErrIdentifierInvalidChars = errors.New("identifier contains invalid characters")

	// ErrNotEnoughPositionalParams is returned when there are fewer positional parameters provided than required.
	ErrNotEnoughPositionalParams = errors.New("not enough positional parameters")

	// ErrTooManyPositionalParams is returned when there are more positional parameters provided than required.
	ErrTooManyPositionalParams = errors.New("too many positional parameters")

	// ErrEmptySQL is returned when the query SQL is empty.
	ErrEmptySQL = errors.New("query SQL cannot be empty")
)

// Query represents a BigQuery query with dollar-sign parameter support.
// It wraps bigquery.Query and adds support for $identifier parameters
// that are validated and safely quoted before execution.
//
// Use Client.Query() to create a new Query instance.
type Query struct {
	bigquery.Query
	originalSQL string
}

var (
	// Regex to find $identifier parameters
	identifierParamRegex = regexp.MustCompile(`\$[a-zA-Z_][a-zA-Z0-9_]*`)
	// Regex to find @named parameters
	namedParamRegex = regexp.MustCompile(`@[a-zA-Z_][a-zA-Z0-9_]*`)
)

const (
	// maxIdentifierBytes is the maximum length for a BigQuery identifier (without backticks)
	maxIdentifierBytes = 1024

	// dollarSign is the prefix for identifier parameters
	dollarSign = '$'

	// atSign is the prefix for named parameters
	atSign = '@'

	// questionMark is the character for positional parameters
	questionMark = '?'
)

// translate converts dollar-sign parameters to BigQuery's native syntax.
// It performs the following transformations:
//   - $identifier parameters are validated and replaced with backtick-quoted values
//   - @parameter names have the @ prefix removed for BigQuery compatibility
//   - ? positional parameters are passed through unchanged
//
// The function validates:
//   - All parameters in SQL are provided in params
//   - All provided parameters are used in SQL
//   - Identifiers contain only valid characters
//   - Identifiers don't exceed 1024 bytes
//   - Positional parameter counts match
//
// Returns the transformed SQL, processed parameters, and any validation error.
func translate(sql string, params []bigquery.QueryParameter) (string, []bigquery.QueryParameter, error) {
	// Validate non-empty SQL
	if sql == "" {
		return "", nil, ErrEmptySQL
	}
	// Build parameters and identifiers map
	parameters := map[string]bigquery.QueryParameter{}
	identifiers := map[string]any{}
	allParameters := []bigquery.QueryParameter{}
	positionalParameterCount := 0
	for _, p := range params {
		paramName := p.Name
		if len(paramName) > 0 {
			switch paramName[0] {
			case atSign: // Named parameter
				p.Name = paramName[1:]
				parameters[paramName] = p
				allParameters = append(allParameters, p)
			case dollarSign: // Identifier parameter
				identifiers[paramName] = p.Value
			default:
				return "", nil, fmt.Errorf("%w: %s must start with @ or $", ErrInvalidParameterName, paramName)
			}
		} else {
			// Positional parameter
			positionalParameterCount++
			allParameters = append(allParameters, p)
		}
	}
	// Find all identifiers in the SQL
	identifiersInSql := map[string]bool{}
	matches := identifierParamRegex.FindAllStringSubmatch(sql, -1)
	for _, match := range matches {
		identifiersInSql[match[0]] = true
	}
	// Find all parameters in the SQL (with @ prefix)
	parametersInSql := map[string]bool{}
	matches = namedParamRegex.FindAllStringSubmatch(sql, -1)
	for _, match := range matches {
		// Store with @ prefix to match the parameters map keys
		parametersInSql[match[0]] = true
	}
	// Detect parameters not present in the original SQL and return error
	for paramName := range parameters {
		if _, exists := parametersInSql[paramName]; !exists {
			return "", nil, fmt.Errorf("%w: %s", ErrParameterNotFound, paramName)
		}
	}
	// Detect parameters not present in the parameters slice and return error
	for paramName := range parametersInSql {
		_, exists := parameters[paramName]
		if !exists {
			return "", nil, fmt.Errorf("%w: %s", ErrParameterNotProvided, paramName)
		}
	}
	// Detect identifiers not present in the original SQL and return error
	for identifier := range identifiers {
		if _, exists := identifiersInSql[identifier]; !exists {
			return "", nil, fmt.Errorf("%w: %s", ErrIdentifierNotFound, identifier)
		}
	}
	// Detect identifiers not present in the identifiers map and return error
	for identifier := range identifiersInSql {
		_, exists := identifiers[identifier]
		if !exists {
			return "", nil, fmt.Errorf("%w: %s", ErrIdentifierNotProvided, identifier)
		}
	}
	// Count positional parameters in SQL
	positionalParamsInSql := strings.Count(sql, string(questionMark))
	if positionalParamsInSql > positionalParameterCount {
		return "", nil, fmt.Errorf("%w: found %d, provided %d", ErrNotEnoughPositionalParams, positionalParamsInSql, positionalParameterCount)
	} else if positionalParamsInSql < positionalParameterCount {
		return "", nil, fmt.Errorf("%w: found %d, provided %d", ErrTooManyPositionalParams, positionalParamsInSql, positionalParameterCount)
	}
	// Apply all replacements
	result := sql
	for identifier, value := range identifiers {
		quoted, replaced := QuoteIdentifier(value)
		if replaced != "" {
			return "", nil, fmt.Errorf("%w: %s contains %s", ErrIdentifierInvalidChars, identifier, replaced)
		}
		if len(quoted) == 2 {
			return "", nil, fmt.Errorf("%w: %s", ErrIdentifierEmpty, identifier)
		}
		if len(quoted) > maxIdentifierBytes+2 { // +2 for backticks
			return "", nil, fmt.Errorf("%w: %s", ErrIdentifierTooLong, identifier)
		}
		result = strings.ReplaceAll(result, identifier, quoted)
	}
	return result, allParameters, nil
}

// translate applies the translation of $ identifiers to the Query's SQL and parameters.
func (q *Query) translate() error {
	originalSQL := q.QueryConfig.Q
	if originalSQL == "" {
		return ErrEmptySQL
	}

	parameters := q.Parameters
	translatedSQL, translatedParams, err := translate(originalSQL, parameters)
	if err != nil {
		return fmt.Errorf("failed to translate query: %w", err)
	}

	q.originalSQL = originalSQL
	q.QueryConfig.Q = translatedSQL
	q.Parameters = translatedParams
	return nil
}

// Run initiates a query job after translating $ identifiers.
// It validates and transforms all $identifier parameters before
// delegating to the underlying bigquery.Query.Run method.
//
// Returns an error if parameter validation fails or if the
// underlying BigQuery query execution fails.
func (q *Query) Run(ctx context.Context) (*bigquery.Job, error) {
	// Apply translation
	if err := q.translate(); err != nil {
		return nil, err
	}
	// Call the parent Run method
	return q.Query.Run(ctx)
}

// Read submits a query for execution and returns results via a RowIterator.
// It validates and transforms all $identifier parameters before
// delegating to the underlying bigquery.Query.Read method.
//
// Returns an error if parameter validation fails or if the
// underlying BigQuery query execution fails.
func (q *Query) Read(ctx context.Context) (*bigquery.RowIterator, error) {
	// Apply translation
	if err := q.translate(); err != nil {
		return nil, err
	}
	// Call the parent Read method
	return q.Query.Read(ctx)
}
