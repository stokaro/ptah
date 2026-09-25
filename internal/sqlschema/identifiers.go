package sqlschema

import (
	"strings"
	"unicode"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
)

// identifierPart reads one component of a name the way the source dialect's
// server resolves it. It is the one rule for every name this package records --
// a table, a column, an index, a constraint, a policy and the table a policy or
// a grant is on -- because two statements naming one object have to land on
// one name, and a rule applied to some statements and not others turns one
// relation into two (stokaro/ptah#3592).
//
// A quoted component keeps every character it was written with. An unquoted
// one is folded where the server folds it, measured through pg_class and
// pg_attribute: PostgreSQL 18.6 and YugabyteDB 2026.1 lower its ASCII letters,
// so `CREATE TABLE Docs (Id int)` creates `docs` with a column `id` and
// `Ärger` stays `Ärger`; CockroachDB 26.3.2 lowers every letter, so `Ärger`
// becomes `ärger`. Kept as written, the name was rendered quoted: the file
// created `"Docs"` and a comparison against the `docs` the server holds for
// the same file planned `DROP TABLE "docs" CASCADE`.
//
// Other dialects keep an unquoted name as written. Spanner is in the
// PostgreSQL family but was not measured, and MySQL, MariaDB, SQL Server,
// ClickHouse, SQLite and Oracle each have their own case rules, which this
// package does not model. A read with no dialect keeps names as written too,
// so a file mixing conventions can be read at all.
func identifierPart(sourcePlatform, part string) string {
	part = strings.TrimSpace(part)
	if isQuotedSQLIdentifierPart(part) {
		return unquoteSQLIdentifierPart(part)
	}
	switch platform.NormalizeDialect(sourcePlatform) {
	case platform.Postgres, platform.YugabyteDB:
		return identifier.ComparisonASCIIInsensitive.IdentityKey(part)
	case platform.CockroachDB:
		return identifier.ComparisonUnicodeInsensitive.IdentityKey(part)
	default:
		return part
	}
}

// identifierParts reads each dot-separated component of value through
// [identifierPart]. The components are independent: `"App".ORDERS` names the
// relation `orders` inside the schema `App` on PostgreSQL.
func identifierParts(sourcePlatform, value string) []string {
	parts := splitSQLIdentifier(value)
	for index, part := range parts {
		parts[index] = identifierPart(sourcePlatform, part)
	}
	return parts
}

func normalizeSQLIdentifier(sourcePlatform, value string) string {
	return strings.Join(identifierParts(sourcePlatform, value), ".")
}

func normalizeSQLTableIdentifier(sourcePlatform, value string) (schema, name string) {
	parts := identifierParts(sourcePlatform, value)
	if len(parts) == 1 {
		return "", parts[0]
	}
	return strings.Join(parts[:len(parts)-1], "."), parts[len(parts)-1]
}

func normalizeSQLTableReference(sourcePlatform, value string) string {
	schema, name := normalizeSQLTableIdentifier(sourcePlatform, value)
	return schemamodel.QualifyTableName(schema, name)
}

// roleName reads one role name the way the source dialect's server resolves
// it: through [identifierPart] like every other name, except on CockroachDB.
//
// A role named unquoted in mixed case has to reach the role the server holds.
// Kept as written, `TO App_A` was rendered `TO "App_A"`, and PostgreSQL 18.6
// refused it with `role "App_A" does not exist` (stokaro/ptah#3574).
// CockroachDB 26.3.2 lowers every role name, quoted or not and past ASCII:
// `CREATE ROLE "Ärger_Q"` creates `ärger_q`, while a quoted table name keeps
// its case there.
//
// Every place this package records a role reads it through here, so a file
// that creates a role and names it elsewhere names one role.
func roleName(sourcePlatform, value string) string {
	value = strings.TrimSpace(value)
	if platform.NormalizeDialect(sourcePlatform) == platform.CockroachDB {
		return identifier.ComparisonUnicodeInsensitive.IdentityKey(unquoteSQLIdentifierPart(value))
	}
	return identifierPart(sourcePlatform, value)
}

func tableStructName(sourcePlatform, value string) string {
	return generateStructName(strings.Join(identifierParts(sourcePlatform, value), "_"))
}

func normalizeSQLIdentifierReference(sourcePlatform, value string) string {
	if !isSQLIdentifierReference(value) {
		return value
	}
	return normalizeSQLIdentifier(sourcePlatform, value)
}

func normalizeSQLIdentifiers(sourcePlatform string, values []string) []string {
	if values == nil {
		return nil
	}
	normalized := make([]string, len(values))
	for index, value := range values {
		normalized[index] = normalizeSQLIdentifier(sourcePlatform, value)
	}
	return normalized
}

func normalizeSQLIdentifierReferences(sourcePlatform string, values []string) []string {
	if values == nil {
		return nil
	}
	normalized := make([]string, len(values))
	for index, value := range values {
		normalized[index] = normalizeSQLIdentifierReference(sourcePlatform, value)
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
