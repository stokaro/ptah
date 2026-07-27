// Package sqlident quotes SQL identifiers (schema, table, and column names) for
// a given SQL dialect. It centralizes the per-dialect quote style and the
// standard "double the quote character" escaping so that identifiers taken from
// catalog metadata, annotations, or any other untrusted-shaped string cannot
// terminate the quoted identifier and inject SQL.
package sqlident

import "strings"

// Quote returns name as a safely-quoted identifier for dialect. The dialect
// selects the quote style: backticks for MySQL, MariaDB, and ClickHouse; square
// brackets for SQL Server; and double quotes for the PostgreSQL family, SQLite,
// and any unrecognized dialect. Embedded quote characters are doubled per the
// SQL standard so the value cannot terminate the quoted identifier. The dialect
// is matched case-insensitively and surrounding whitespace is ignored; name
// itself is quoted verbatim (it is not trimmed).
func Quote(dialect, name string) string {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "mysql", "mariadb", "clickhouse":
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	case "sqlserver", "mssql":
		return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
	default:
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
}

// Qualified returns a dialect-quoted identifier optionally qualified by schema,
// as in "schema"."name". Leading and trailing whitespace is trimmed from schema
// and name before quoting; an empty (or whitespace-only) schema yields just the
// quoted name.
func Qualified(dialect, schema, name string) string {
	schema = strings.TrimSpace(schema)
	name = strings.TrimSpace(name)
	if schema == "" {
		return Quote(dialect, name)
	}
	return Quote(dialect, schema) + "." + Quote(dialect, name)
}
