package sqlschema

import (
	"slices"
	"strconv"
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

// resolveDeclaredName returns the index in declared of the name that written
// reaches on the source dialect's server, or -1. Both sides have been read
// through [identifierPart], so a PostgreSQL-family fold is already applied.
//
// An exact match wins. Where the server also compares without case, the one
// declared name that differs from written only in case is the match. Two such
// names are no match: the server either refuses the document that declares
// both, or keeps them apart and finds neither.
//
// Each rule was measured with CREATE TABLE and ALTER TABLE ... ADD COLUMN and
// DROP COLUMN:
//
//   - PostgreSQL, YugabyteDB and CockroachDB compare exactly; an unquoted
//     name arrives folded and a quoted one keeps its case.
//   - ClickHouse 26.8 compares table and column names exactly.
//   - SQLite folds ASCII letters only, quoted or not: `docs` reaches "Docs",
//     and `ärger` does not reach `Ärger`.
//   - SQL Server 2022 under its default collation, SQL_Latin1_General_CP1_CI_AS,
//     folds every letter, bracketed or not: `ärger` reaches `Ärger`, and
//     `café` stays apart from `cafe`. A case-sensitive collation is not
//     modeled.
//   - MySQL 8.4 and MariaDB 11.8 compare a column name without case, `äpfel`
//     against `Äpfel` included. A table name follows lower_case_table_names,
//     which a schema file does not carry: 0, the Linux default, keeps `Docs`
//     and `docs` apart and refuses `ALTER TABLE docs` on `Docs`, and 1 and 2
//     compare without case. The exact match keeps apart two tables a
//     case-sensitive server keeps apart, and the fallback accepts what a
//     case-insensitive server accepts. The cost is that a file meant for a
//     case-sensitive server can reach a table that server would not find. The
//     non-ASCII pairs the two engines fold differently (stokaro/ptah#2768)
//     are not modeled.
//   - Oracle folds ASCII letters, as [identifier.ForDialect] compares its
//     names. Oracle upper-cases a bare name and keeps a quoted one, which
//     [identifierPart] does not model, so a bare spelling can reach a quoted
//     table Oracle keeps apart.
//
// Any other dialect, and a read with no dialect, compares exactly.
//
// Every name a statement writes to reach something already declared goes
// through here: the table an ALTER TABLE or a COMMENT ON names, the column an
// ALTER TABLE operation or a COMMENT ON COLUMN names, and the column an ADD
// COLUMN must not repeat. It cannot become two
// functions, one for tables and one for columns: the lookups agree only while
// both copies carry the same dialect rules, and a statement whose table and
// column were resolved by different rules changes the wrong object
// (stokaro/ptah#3642).
func resolveDeclaredName(sourcePlatform, written string, declared []string) int {
	if index := slices.Index(declared, written); index >= 0 {
		return index
	}
	var fold identifier.Comparison
	switch platform.NormalizeDialect(sourcePlatform) {
	case platform.SQLite, platform.Oracle:
		fold = identifier.ComparisonASCIIInsensitive
	case platform.SQLServer, platform.MySQL, platform.MariaDB:
		fold = identifier.ComparisonUnicodeInsensitive
	default:
		return -1
	}
	match := -1
	for index, name := range declared {
		if fold.IdentityKey(name) != fold.IdentityKey(written) {
			continue
		}
		if match >= 0 {
			return -1
		}
		match = index
	}
	return match
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

// declaredTable returns the table declared under exactly the qualified name,
// looking in each database in order, or nil. It answers whether a CREATE TABLE
// declares a table again; a statement that names a table finds it through
// [resolveTable].
func declaredTable(databases []*schemamodel.Database, qualified string) *schemamodel.Table {
	for _, database := range databases {
		for i := range database.Tables {
			if database.Tables[i].QualifiedName() == qualified {
				return &database.Tables[i]
			}
		}
	}
	return nil
}

// resolveTable returns the table a statement naming qualified reaches, looking
// in each database in order, or nil. See [resolveDeclaredName].
func resolveTable(databases []*schemamodel.Database, qualified, sourcePlatform string) *schemamodel.Table {
	var tables []*schemamodel.Table
	var names []string
	for _, database := range databases {
		for i := range database.Tables {
			tables = append(tables, &database.Tables[i])
			names = append(names, database.Tables[i].QualifiedName())
		}
	}
	if index := resolveDeclaredName(sourcePlatform, qualified, names); index >= 0 {
		return tables[index]
	}
	return nil
}

// tableColumns returns the columns declared for the table carrying structName,
// and their names, in the same order.
func tableColumns(databases []*schemamodel.Database, structName string) ([]*schemamodel.Field, []string) {
	var fields []*schemamodel.Field
	var names []string
	for _, database := range databases {
		for i := range database.Fields {
			if database.Fields[i].StructName == structName {
				fields = append(fields, &database.Fields[i])
				names = append(names, database.Fields[i].Name)
			}
		}
	}
	return fields, names
}

// resolveColumn returns the column of the table carrying structName that a
// statement writing name reaches, or nil. See [resolveDeclaredName].
func resolveColumn(databases []*schemamodel.Database, structName, name, sourcePlatform string) *schemamodel.Field {
	fields, names := tableColumns(databases, structName)
	if index := resolveDeclaredName(sourcePlatform, normalizeSQLIdentifier(sourcePlatform, name), names); index >= 0 {
		return fields[index]
	}
	return nil
}

// uniqueStructName returns the struct name a new table joins its columns,
// indexes and constraints through.
//
// The name [tableStructName] derives is not one-to-one. It camel-cases and
// singularizes, so "Docs", docs and doc all become Doc, and app.t and app_t
// both become AppT. Two tables sharing one struct name are one table to
// everything that joins on it, and Finalize keeps a single `id` column for the
// pair (stokaro/ptah#3642). So a table whose derived name another table
// already holds takes the first numbered form that is free, Doc2 and then
// Doc3, in the order the document declares them.
//
// A table declared again under the same qualified name keeps the struct name
// of the first declaration, so the repeat still collapses into one table.
func uniqueStructName(databases []*schemamodel.Database, table schemamodel.Table) string {
	if declared := declaredTable(databases, table.QualifiedName()); declared != nil {
		return declared.StructName
	}
	taken := func(name string) bool {
		for _, database := range databases {
			for i := range database.Tables {
				if database.Tables[i].StructName == name {
					return true
				}
			}
		}
		return false
	}
	name := table.StructName
	for suffix := 2; taken(name); suffix++ {
		name = table.StructName + strconv.Itoa(suffix)
	}
	return name
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
