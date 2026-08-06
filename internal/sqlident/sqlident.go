// Package sqlident quotes SQL identifiers (schema, table, and column names) for
// a given SQL dialect. It centralizes the per-dialect quote style and the
// standard "double the quote character" escaping so that identifiers taken from
// catalog metadata, annotations, or any other untrusted-shaped string cannot
// terminate the quoted identifier and inject SQL.
package sqlident

import (
	"strings"

	"go.5x5.cz/ptah/core/platform"
)

// Quote returns name as a safely-quoted identifier for dialect. The dialect
// selects the quote style: backticks for MySQL, MariaDB, and ClickHouse; square
// brackets for SQL Server; and double quotes for the PostgreSQL family, SQLite,
// and any unrecognized dialect. Embedded quote characters are doubled per the
// SQL standard so the value cannot terminate the quoted identifier. The dialect
// is resolved through platform.NormalizeDialect, so every documented spelling of
// an engine (`mssql`, `tsql`, `sql-server`, `sql_server`; `ch`) picks the same
// quote style as its canonical name. name itself is quoted verbatim (it is not
// trimmed).
func Quote(dialect, name string) string {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB, platform.ClickHouse:
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	case platform.SQLServer:
		return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
	default:
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
}

// Qualified returns a dialect-quoted identifier optionally qualified by schema,
// as in "schema"."name". A whitespace-only schema yields just the quoted name.
// Otherwise, schema and name are quoted verbatim.
func Qualified(dialect, schema, name string) string {
	if strings.TrimSpace(schema) == "" {
		return Quote(dialect, name)
	}
	return Quote(dialect, schema) + "." + Quote(dialect, name)
}
