package toschema

import (
	"strings"
	"unicode"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
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

// catalogPostgresTableReference answers which relation a PostgreSQL statement
// names, one identifier component at a time.
//
// The two spellings `ORDERS` and `"ORDERS"` reach this package as different
// strings, and PostgreSQL reads them as different relations: the unquoted one
// is folded to `orders` on its way into the catalog, the quoted one keeps its
// case. Measured on PostgreSQL 17.10 against a database holding only `orders`,
// `CREATE POLICY p ON ORDERS` exits 0 with a pg_policy row on `public.orders`
// while `CREATE POLICY p ON "ORDERS"` exits 1 with `relation "ORDERS" does not
// exist`.
//
// [normalizeSQLTableReference] unquotes every component and therefore erases
// that difference, which is why the reference has to be folded HERE, at the one
// point in the pipeline that still holds the quoting. Everything downstream
// sees the relation, so nothing downstream has to guess -- and a guess is what
// relocated an access-control declaration onto a relation the author did not
// name (stokaro/ptah#1311).
//
// The fold is per component because the components are independent:
// `"App".ORDERS` names the relation `orders` inside the schema `App`, and
// PostgreSQL 17.10 resolves it against a table created as `"App".orders`
// with exit 0.
//
// This is deliberately not applied to identifiers in general. Only PostgreSQL
// folds unquoted identifiers down, and only the RLS statements this package
// routes here are PostgreSQL-only syntax; the MySQL, MariaDB and SQL Server
// frontends share the reader and must keep their own case rules.
func catalogPostgresTableReference(value string) string {
	parts := splitSQLIdentifier(value)
	for index, part := range parts {
		parts[index] = catalogPostgresIdentifierPart(strings.TrimSpace(part))
	}
	if len(parts) == 1 {
		return goschema.QualifyTableName("", parts[0])
	}
	return goschema.QualifyTableName(
		strings.Join(parts[:len(parts)-1], "."),
		parts[len(parts)-1],
	)
}

// catalogPostgresIdentifierPart folds one component the way PostgreSQL does:
// an unquoted component loses its ASCII case, a quoted one keeps every
// character it was written with.
func catalogPostgresIdentifierPart(part string) string {
	if isQuotedSQLIdentifierPart(part) {
		return unquoteSQLIdentifierPart(part)
	}
	return identifier.ComparisonASCIIInsensitive.IdentityKey(part)
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
