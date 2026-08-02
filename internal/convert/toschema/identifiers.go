package toschema

import (
	"strings"
	"unicode"

	"go.5x5.cz/ptah/core/goschema"
)

func normalizeSQLIdentifier(value string) string {
	parts := splitSQLIdentifier(value)
	for index, part := range parts {
		part = strings.TrimSpace(part)
		parts[index] = unquoteSQLIdentifierPart(part)
	}
	return strings.Join(parts, ".")
}

func normalizeSQLTableIdentifier(value string) (schema, name string) {
	parts := splitSQLIdentifier(value)
	for index, part := range parts {
		parts[index] = unquoteSQLIdentifierPart(strings.TrimSpace(part))
	}
	if len(parts) == 1 {
		return "", parts[0]
	}
	return strings.Join(parts[:len(parts)-1], "."), parts[len(parts)-1]
}

func normalizeSQLTableReference(value string) string {
	schema, name := normalizeSQLTableIdentifier(value)
	return goschema.QualifyTableName(schema, name)
}

func tableStructName(value string) string {
	parts := splitSQLIdentifier(value)
	for index, part := range parts {
		parts[index] = unquoteSQLIdentifierPart(strings.TrimSpace(part))
	}
	return generateStructName(strings.Join(parts, "_"))
}

func normalizeSQLIdentifierReference(value string) string {
	if !isSQLIdentifierReference(value) {
		return value
	}
	return normalizeSQLIdentifier(value)
}

func normalizeSQLIdentifiers(values []string) []string {
	if values == nil {
		return nil
	}
	normalized := make([]string, len(values))
	for index, value := range values {
		normalized[index] = normalizeSQLIdentifier(value)
	}
	return normalized
}

func normalizeSQLIdentifierReferences(values []string) []string {
	if values == nil {
		return nil
	}
	normalized := make([]string, len(values))
	for index, value := range values {
		normalized[index] = normalizeSQLIdentifierReference(value)
	}
	return normalized
}

func isSQLIdentifierReference(value string) bool {
	parts := splitSQLIdentifier(strings.TrimSpace(value))
	if len(parts) == 0 {
		return false
	}
	for _, part := range parts {
		if !isSQLIdentifierPart(strings.TrimSpace(part)) {
			return false
		}
	}
	return true
}

func isSQLIdentifierPart(value string) bool {
	if value == "" {
		return false
	}
	if isQuotedSQLIdentifierPart(value) {
		return true
	}
	for index, char := range value {
		if index == 0 && char != '_' && !unicode.IsLetter(char) {
			return false
		}
		if index > 0 && char != '_' && char != '$' &&
			!unicode.IsLetter(char) && !unicode.IsDigit(char) {
			return false
		}
	}
	return true
}

func isQuotedSQLIdentifierPart(value string) bool {
	if len(value) < 2 {
		return false
	}
	closeQuote := value[0]
	switch value[0] {
	case '"', '`':
	case '[':
		closeQuote = ']'
	default:
		return false
	}
	if value[len(value)-1] != closeQuote {
		return false
	}
	for index := 1; index < len(value)-1; index++ {
		if value[index] != closeQuote {
			continue
		}
		if index+1 >= len(value)-1 || value[index+1] != closeQuote {
			return false
		}
		index++
	}
	return true
}

func splitSQLIdentifier(value string) []string {
	var parts []string
	start := 0
	var quote byte
	for index := 0; index < len(value); index++ {
		char := value[index]
		if quote == 0 {
			switch char {
			case '"', '`', '[':
				quote = char
			case '.':
				parts = append(parts, value[start:index])
				start = index + 1
			}
			continue
		}

		closeQuote := quote
		if quote == '[' {
			closeQuote = ']'
		}
		if char != closeQuote {
			continue
		}
		if index+1 < len(value) && value[index+1] == closeQuote {
			index++
			continue
		}
		quote = 0
	}
	return append(parts, value[start:])
}

func unquoteSQLIdentifierPart(value string) string {
	if len(value) < 2 {
		return value
	}
	switch {
	case value[0] == '"' && value[len(value)-1] == '"':
		return strings.ReplaceAll(value[1:len(value)-1], `""`, `"`)
	case value[0] == '`' && value[len(value)-1] == '`':
		return strings.ReplaceAll(value[1:len(value)-1], "``", "`")
	case value[0] == '[' && value[len(value)-1] == ']':
		return strings.ReplaceAll(value[1:len(value)-1], "]]", "]")
	default:
		return value
	}
}
