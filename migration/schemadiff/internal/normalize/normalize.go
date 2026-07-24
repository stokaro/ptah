package normalize

import (
	"strings"
)

// Type normalizes database type names for cross-platform comparison.
//
// This function converts database-specific type names to standardized forms that can be
// compared across different database systems. It handles the variations in type naming
// conventions between PostgreSQL, MySQL, MariaDB, and other databases.
//
// # Type Normalization Rules
//
//   - VARCHAR variations (VARCHAR, VARCHAR2, etc.) → "varchar"
//   - TEXT variations (TEXT, LONGTEXT, etc.) → "text"
//   - Integer variations (INT, INTEGER, BIGINT, etc.) → "integer"
//   - SERIAL types (SERIAL, BIGSERIAL) → "integer" (for comparison purposes)
//   - Boolean variations (BOOL, BOOLEAN, TINYINT(1)) → "boolean"
//   - Timestamp variations → "timestamp"
//   - Decimal variations (DECIMAL, NUMERIC) → "decimal"
//
// # Database-Specific Handling
//
//   - **MySQL/MariaDB**: TINYINT and TINYINT(1) are treated as BOOLEAN
//   - **PostgreSQL**: SERIAL types are normalized to INTEGER for comparison
//   - **Cross-platform**: Case-insensitive comparison with lowercase normalization
//
// # Example Usage
//
//	// These all normalize to "varchar"
//	Type("VARCHAR(255)")  // → "varchar"
//	Type("varchar(100)")  // → "varchar"
//	Type("VARCHAR2")      // → "varchar"
//
//	// These all normalize to "boolean"
//	Type("BOOLEAN")       // → "boolean"
//	Type("TINYINT(1)")    // → "boolean"
//	Type("BOOL")          // → "boolean"
//
// # Parameters
//
//   - typeName: The database-specific type name to normalize
//
// # Return Value
//
// Returns a normalized type name suitable for cross-database comparison.
func Type(typeName string) string {
	// Convert to lowercase for case-insensitive comparison
	typeName = strings.ToLower(typeName)

	switch {
	case strings.Contains(typeName, "varchar"):
		return "varchar"
	case strings.Contains(typeName, "text"):
		return "text"
	case strings.Contains(typeName, "serial"):
		// SERIAL types are auto-incrementing integers
		return "integer"
	case strings.Contains(typeName, "tinyint"):
		// MySQL/MariaDB stores BOOLEAN as TINYINT or TINYINT(1)
		return "boolean"
	case strings.Contains(typeName, "int"):
		return "integer"
	case strings.Contains(typeName, "bool"):
		return "boolean"
	case strings.Contains(typeName, "timestamp"):
		return "timestamp"
	case strings.Contains(typeName, "decimal") || strings.Contains(typeName, "numeric"):
		return "decimal"
	default:
		// Return as-is for unrecognized types (enums, custom types, etc.)
		return typeName
	}
}

// DefaultValue normalizes default values for cross-database comparison.
//
// This function handles the variations in how different database systems represent
// default values, ensuring that semantically equivalent defaults are recognized
// as identical during schema comparison.
//
// # Normalization Rules
//
//   - Empty/NULL values: Converted to empty string for consistent comparison
//   - Quoted values: Quotes are removed for comparison (both single and double)
//   - PostgreSQL type casting: Removes ::type syntax (e.g., 'user'::text → 'user')
//   - Boolean values: MySQL/MariaDB '1'/'0' normalized to 'true'/'false'
//   - NULL literals: Database-specific NULL representations normalized to empty string
//
// # Database-Specific Handling
//
//   - **MySQL/MariaDB**: Returns 'NULL' string for columns without explicit defaults
//   - **PostgreSQL**: Returns actual NULL for columns without defaults, includes type casting
//   - **Boolean types**: Handles '1'/'0' vs 'true'/'false' variations
//   - **PostgreSQL type casting**: Strips ::type syntax from default values
//
// # Example Usage
//
//	// Boolean normalization
//	DefaultValue("1", "boolean")     // → "true"
//	DefaultValue("0", "boolean")     // → "false"
//	DefaultValue("true", "boolean")  // → "true"
//
//	// Quote removal
//	DefaultValue("'hello'", "varchar")  // → "hello"
//	DefaultValue("\"world\"", "text")   // → "world"
//
//	// PostgreSQL type casting removal
//	DefaultValue("'user'::text", "text")     // → "user"
//	DefaultValue("'0'::bigint", "integer")   // → "0"
//	DefaultValue("'active'::text", "text")   // → "active"
//
//	// NULL handling
//	DefaultValue("NULL", "varchar")     // → ""
//	DefaultValue("", "integer")        // → ""
//
// # Parameters
//
//   - defaultValue: The raw default value from database introspection
//   - typeName: The normalized type name (used for type-specific handling)
//
// # Return Value
//
// Returns a normalized default value suitable for cross-database comparison.
func DefaultValue(defaultValue, typeName string) string {
	if defaultValue == "" {
		return ""
	}

	if normalizedExpression := normalizeTemporalDefaultExpression(defaultValue, typeName); normalizedExpression != "" {
		return normalizedExpression
	}

	if normalizedSequence := normalizeSequenceDefaultExpression(defaultValue); normalizedSequence != "" {
		return normalizedSequence
	}

	cleanValue := defaultValue

	// MariaDB/MySQL returns 'NULL' string for columns without explicit defaults
	// Normalize this to empty string for consistent comparison
	if strings.ToUpper(cleanValue) == "NULL" {
		return ""
	}

	// Handle PostgreSQL type casting syntax (e.g., 'user'::text, '0'::bigint)
	// Remove the ::type suffix before processing quotes
	// We need to find the last :: to handle cases like 'value::with::colons'::text
	if lastColonIndex := strings.LastIndex(cleanValue, "::"); lastColonIndex != -1 {
		cleanValue = cleanValue[:lastColonIndex]
	}

	// Remove surrounding quotes for comparison (both single and double quotes)
	cleanValue = strings.Trim(cleanValue, "'\"")

	// For boolean types, normalize database-specific representations
	if typeName == "boolean" {
		switch strings.ToLower(cleanValue) {
		case "1", "true":
			return "true"
		case "0", "false":
			return "false"
		}
		// If it's not a recognized boolean value, return as-is
		return cleanValue
	}
	if typeName == "decimal" {
		return normalizeDecimalDefaultValue(cleanValue)
	}

	// Return cleaned value for all other types
	return cleanValue
}

func normalizeTemporalDefaultExpression(defaultValue, typeName string) string {
	normalizedType := strings.ToLower(strings.TrimSpace(typeName))
	if normalizedType != "" && normalizedType != "timestamp" {
		return ""
	}
	normalizedValue := strings.ToUpper(strings.TrimSpace(defaultValue))
	normalizedValue = strings.TrimSuffix(normalizedValue, "()")
	switch normalizedValue {
	case "CURRENT_TIMESTAMP", "NOW", "LOCALTIME", "LOCALTIMESTAMP":
		return normalizedValue
	default:
		return ""
	}
}

// normalizeSequenceDefaultExpression canonicalizes a nextval(...) column
// default so a declared nextval('seq') matches the nextval('seq'::regclass)
// form PostgreSQL stores and reads back. It returns "" when the value is not a
// nextval call, so the general default handling applies.
//
// The general ::type stripping in DefaultValue cannot handle this case: the
// ::regclass cast is nested inside the call's parentheses, so truncating at the
// last "::" would drop the closing paren and never match the declared form.
func normalizeSequenceDefaultExpression(defaultValue string) string {
	trimmed := strings.TrimSpace(defaultValue)
	if !strings.HasPrefix(strings.ToLower(trimmed), "nextval(") {
		return ""
	}
	// Remove the regclass cast PostgreSQL adds around the sequence name. The
	// bare, quoted, and pg_catalog-qualified spellings are all handled. The
	// sequence name itself is left untouched: PostgreSQL reads the default back
	// with the sequence spelled exactly as declared for the default schema, so a
	// genuine cross-schema or dotted name must not be rewritten.
	for _, cast := range []string{
		`::pg_catalog."regclass"`,
		"::pg_catalog.regclass",
		`::"regclass"`,
		"::regclass",
	} {
		trimmed = strings.ReplaceAll(trimmed, cast, "")
	}
	return trimmed
}

func normalizeDecimalDefaultValue(value string) string {
	value = strings.TrimSpace(value)
	sign := ""
	if after, ok := strings.CutPrefix(value, "-"); ok {
		sign = "-"
		value = after
	} else if after, ok := strings.CutPrefix(value, "+"); ok {
		value = after
	}
	if value == "" || strings.Count(value, ".") > 1 {
		return sign + value
	}
	parts := strings.Split(value, ".")
	for _, part := range parts {
		if !isDecimalDigits(part) {
			return sign + value
		}
	}
	intPart := strings.TrimLeft(parts[0], "0")
	if intPart == "" {
		intPart = "0"
	}
	if len(parts) == 1 {
		if intPart == "0" {
			return intPart
		}
		return sign + intPart
	}
	fracPart := strings.TrimRight(parts[1], "0")
	if fracPart == "" {
		if intPart == "0" {
			return intPart
		}
		return sign + intPart
	}
	return sign + intPart + "." + fracPart
}

func isDecimalDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func IsDefaultExpr(value string) bool {
	cleanValue := strings.Trim(value, " \t\n\r")

	if strings.HasPrefix(cleanValue, `"`) && strings.HasSuffix(cleanValue, `"`) {
		return false
	}
	if strings.HasPrefix(cleanValue, "'") && strings.HasSuffix(cleanValue, "'") {
		return false
	}
	return true
}

// Expression normalizes SQL expressions for schema comparison.
func Expression(value string) string {
	cleanValue := strings.TrimSpace(value)
	for hasRedundantOuterParentheses(cleanValue) {
		cleanValue = strings.TrimSpace(cleanValue[1 : len(cleanValue)-1])
	}
	return cleanValue
}

func hasRedundantOuterParentheses(value string) bool {
	if len(value) < 2 || value[0] != '(' || value[len(value)-1] != ')' {
		return false
	}

	depth := 0
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '\'', '"':
			next, ok := skipQuotedSQL(value, i, value[i])
			if !ok {
				return false
			}
			i = next
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return false
			}
			if depth == 0 && i != len(value)-1 {
				return false
			}
		}
	}

	return depth == 0
}

func skipQuotedSQL(value string, start int, quote byte) (int, bool) {
	for i := start + 1; i < len(value); i++ {
		if value[i] != quote {
			continue
		}
		if i+1 < len(value) && value[i+1] == quote {
			i++
			continue
		}
		return i, true
	}
	return 0, false
}
