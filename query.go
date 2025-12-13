package saferbq

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"cloud.google.com/go/bigquery"
)

// Query represents a BigQuery query with dollar-sign parameter support.
type Query struct {
	bigquery.Query
	originalSQL string
}

var (
	// namedIdentifierParamRegex matches named parameters like $param or $param_name
	identifierParamRegex = regexp.MustCompile(`\$[a-zA-Z_][a-zA-Z0-9_]*`)
	namedParamRegex      = regexp.MustCompile(`@[a-zA-Z_][a-zA-Z0-9_]*`)
)

// translate converts dollar-sign parameters to BigQuery's native syntax.
// @param stays as @param (native BigQuery parameters).
// $identifier gets replaced with QuoteIdentifier(value).
func translate(sql string, params []bigquery.QueryParameter) (string, []bigquery.QueryParameter, error) {
	// Build parameters and identifiers map
	parameters := map[string]bigquery.QueryParameter{}
	identifiers := map[string]any{}
	allParameters := []bigquery.QueryParameter{}
	positionalParameterCount := 0
	for _, p := range params {
		paramName := p.Name
		if len(paramName) > 0 {
			switch paramName[0] {
			case '@': // Named parameter
				p.Name = paramName[1:]
				parameters[paramName] = p
				allParameters = append(allParameters, p)
			case '$': // Identifier parameter
				identifiers[paramName] = p.Value
			default:
				return "", nil, fmt.Errorf("invalid parameter name %s: must start with @ or $", paramName)
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
			return "", nil, fmt.Errorf("parameter %s not found in query", paramName)
		}
	}
	// Detect parameters not present in the parameters slice and return error
	for paramName := range parametersInSql {
		_, exists := parameters[paramName]
		if !exists {
			return "", nil, fmt.Errorf("parameter %s not provided in parameters", paramName)
		}
	}
	// Detect identifiers not present in the original SQL and return error
	for identifier := range identifiers {
		if _, exists := identifiersInSql[identifier]; !exists {
			return "", nil, fmt.Errorf("identifier %s not found in query", identifier)
		}
	}
	// Detect identifiers not present in the identifiers map and return error
	for identifier := range identifiersInSql {
		_, exists := identifiers[identifier]
		if !exists {
			return "", nil, fmt.Errorf("identifier %s not provided in parameters", identifier)
		}
	}
	// Count positional parameters in SQL
	positionalParamsInSql := strings.Count(sql, "?")
	if positionalParamsInSql > positionalParameterCount {
		return "", nil, fmt.Errorf("not enough positional parameters: found %d, provided %d", positionalParamsInSql, positionalParameterCount)
	} else if positionalParamsInSql < positionalParameterCount {
		return "", nil, fmt.Errorf("too many positional parameters: found %d, provided %d", positionalParamsInSql, positionalParameterCount)
	}
	// Apply all replacements
	result := sql
	for identifier, value := range identifiers {
		quoted := quoteIdentifier(value)
		if len(quoted) == 2 {
			return "", nil, fmt.Errorf("identifier %s is empty", identifier)
		}
		if len(quoted) > 1026 {
			return "", nil, fmt.Errorf("identifier %s is too long", identifier)
		}
		result = strings.ReplaceAll(result, identifier, quoted)
	}
	return result, allParameters, nil
}

// translate applies the translation of $ identifiers to the Query's SQL and parameters.
func (q *Query) translate() error {
	originalSql := q.QueryConfig.Q
	parameters := q.Parameters
	translatedSQL, translatedParams, err := translate(originalSql, parameters)
	if err != nil {
		return fmt.Errorf("failed to translate query: %w", err)
	}
	q.originalSQL = originalSql
	q.QueryConfig.Q = translatedSQL
	q.Parameters = translatedParams
	return nil
}

// Run initiates a query job after applying Translate to handle $ identifiers.
func (q *Query) Run(ctx context.Context) (*bigquery.Job, error) {
	// Apply translation
	if err := q.translate(); err != nil {
		return nil, err
	}
	// Call the parent Run method
	return q.Query.Run(ctx)
}

// Read submits a query for execution and returns the results via a RowIterator.
// It applies Translate to handle $ identifiers before executing.
func (q *Query) Read(ctx context.Context) (*bigquery.RowIterator, error) {
	// Apply translation
	if err := q.translate(); err != nil {
		return nil, err
	}
	// Call the parent Read method
	return q.Query.Read(ctx)
}
