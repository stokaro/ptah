// Package sqlident quotes SQL identifiers (schema, table, and column names) for
// a given SQL dialect. It centralizes the per-dialect quote style and the
// standard "double the quote character" escaping so that identifiers taken from
// catalog metadata, annotations, or any other untrusted-shaped string cannot
// terminate the quoted identifier and inject SQL.
package sqlident

import (
	"strings"

	"ptah.run/core/platform"
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

// Ident returns the spelling that refers to name in dialect.
//
// For every dialect but Oracle this is [Quote]: a quoted name is the same name
// in the catalog, so quoting always is both safe and unambiguous.
//
// Oracle is the exception, because there quoting CHANGES which object is named.
// An unquoted identifier folds to upper case and a quoted one does not, and
// measured on 23.26 the two coexist: `CREATE TABLE ptah_fold (...)` and
// `CREATE TABLE "ptah_fold" (...)` produce two tables, reported by user_tables
// as PTAH_FOLD and ptah_fold. So the choice has to be the SAME one everywhere a
// name is written -- in the DDL that creates a table and in the query that
// reads it. When they disagreed, the Oracle DDL renderer wrote `ora_posts` and
// the query builder wrote `"ora_posts"`, which is a table nobody created.
//
// See the Oracle renderer's escapeIdentifier for why bare is the side both
// chose: a CHECK or a generated expression is author text this repository does
// not rewrite, and only the bare form agrees with it.
func Ident(dialect, name string) string {
	if platform.NormalizeDialect(dialect) == platform.Oracle {
		return BareOrQuoted(dialect, name)
	}
	return Quote(dialect, name)
}

// BareOrQuoted returns name unquoted when it is a plain, nonreserved identifier
// for dialect, and dialect-quoted otherwise.
//
// It exists for the positions where an engine records an identifier without
// quotes and reproducing that spelling matters: SQLite writes
// `USING fts5` rather than `USING "fts5"`, and a module named `fts-5` writes
// `USING "fts-5"`. Two callers have to agree about which form a render
// produces -- the renderer that writes the statement and the inspection check
// that looks for it -- and when they disagreed, a SQL document that carried the
// declaration perfectly was reported as lossy and refused under strict
// compatibility. See stokaro/ptah#1028.
//
// "Plain" is the conservative intersection: an ASCII letter or underscore
// followed by ASCII letters, digits or underscores. Anything else, including
// the empty string, is quoted. SQLite keywords are also quoted because their
// bare forms are parsed as syntax in the virtual-table module position.
func BareOrQuoted(dialect, name string) string {
	if !isPlainIdentifier(name) || isReservedKeyword(dialect, name) {
		return Quote(dialect, name)
	}
	return name
}

// isReservedKeyword reports identifiers the target parses as SQL syntax.
//
// SQLite publishes one exhaustive list for every supported build. Virtual
// table module names occupy an identifier position after USING, so a module
// named `select` must render as `"select"`; the bare form is a syntax error.
// Other dialects currently use BareOrQuoted only in tests and retain the
// existing plain-identifier behavior.
func isReservedKeyword(dialect, name string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.SQLite:
		return isSQLiteReservedKeyword(name)
	case platform.Oracle:
		return isOracleReservedKeyword(name)
	default:
		return false
	}
}

// isOracleReservedKeyword reports whether name is one of the words Oracle
// refuses as a bare identifier.
//
// The list is the server's own, and the predicate matters: `reserved = 'Y'`
// alone is NOT the set Oracle refuses as an identifier. v$reserved_words has
// 3082 rows and four flags, and the words below are spread across two of them --
// SIZE and RESOURCE answer reserved='Y', while COMMENT, LEVEL, SESSION and USER
// answer reserved='N' with res_semi='Y'. Measured on 23.26, the server refuses
// every one of them the same way:
//
//	CREATE TABLE t (comment CLOB)  ->  ORA-03050: invalid identifier: "COMMENT" is a reserved word
//	CREATE TABLE t (user CLOB)     ->  ORA-03050: invalid identifier: "USER" is a reserved word
//	CREATE TABLE t (description CLOB)  ->  created  (control)
//
// So the list is every word-shaped keyword any of the four flags marks:
//
//	SELECT keyword FROM v$reserved_words
//	 WHERE reserved = 'Y' OR res_type = 'Y' OR res_attr = 'Y' OR res_semi = 'Y'
//
// which answers 111 word-shaped rows on 23.26 and the identical 111 on 21.3, so
// one list serves both measured lines. Narrowing it to `reserved='Y'` alone
// answers 81 and drops `user`, `comment`, `level` and `session`, which are
// ordinary column names (stokaro/ptah#1875).
//
// It matters more here than it does for SQLite, because the Oracle renderer
// writes a plain name WITHOUT quotes -- see escapeIdentifier there for why -- so
// a column named "size" or "comment" would otherwise reach the server as
// syntax.
func isOracleReservedKeyword(name string) bool {
	switch strings.ToUpper(name) {
	case "ACCESS", "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUDIT", "BETWEEN", "BY",
		"CHAR", "CHECK", "CLUSTER", "COLUMN", "COLUMN_VALUE", "COMMENT", "COMPRESS", "CONNECT",
		"CREATE", "CURRENT", "DATE", "DECIMAL", "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP",
		"ELSE", "EXCEPT", "EXCLUSIVE", "EXISTS", "FILE", "FLOAT", "FOR", "FROM", "GRANT", "GROUP",
		"HAVING", "IDENTIFIED", "IMMEDIATE", "IN", "INCREMENT", "INDEX", "INITIAL", "INSERT",
		"INTEGER", "INTERSECT", "INTO", "IS", "LEVEL", "LIKE", "LOCK", "LONG", "MAXEXTENTS", "MINUS",
		"MLSLABEL", "MODE", "MODIFY", "NESTED_TABLE_ID", "NOAUDIT", "NOCOMPRESS", "NOT", "NOWAIT",
		"NULL", "NUMBER", "OF", "OFFLINE", "ON", "ONLINE", "OPTION", "OR", "ORDER", "PCTFREE",
		"PRIOR", "PUBLIC", "RAW", "RENAME", "RESOURCE", "REVOKE", "ROW", "ROWID", "ROWNUM", "ROWS",
		"SELECT", "SESSION", "SET", "SHARE", "SIZE", "SMALLINT", "START", "SUCCESSFUL", "SYNONYM",
		"SYSDATE", "TABLE", "THEN", "TO", "TRIGGER", "UID", "UNION", "UNIQUE", "UPDATE", "USER",
		"VALIDATE", "VALUES", "VARCHAR", "VARCHAR2", "VIEW", "WHENEVER", "WHERE", "WITH":
		return true
	default:
		return false
	}
}

func isSQLiteReservedKeyword(name string) bool {
	switch strings.ToUpper(name) {
	case "ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ALWAYS", "ANALYZE", "AND", "AS", "ASC", "ATTACH",
		"AUTOINCREMENT", "BEFORE", "BEGIN", "BETWEEN", "BY", "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN",
		"COMMIT", "CONFLICT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
		"DATABASE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DETACH", "DISTINCT", "DO", "DROP", "EACH",
		"ELSE", "END", "ESCAPE", "EXCEPT", "EXCLUDE", "EXCLUSIVE", "EXISTS", "EXPLAIN", "FAIL", "FILTER", "FIRST",
		"FOLLOWING", "FOR", "FOREIGN", "FROM", "FULL", "GENERATED", "GLOB", "GROUP", "GROUPS", "HAVING", "IF", "IGNORE",
		"IMMEDIATE", "IN", "INDEX", "INDEXED", "INITIALLY", "INNER", "INSERT", "INSTEAD", "INTERSECT", "INTO", "IS",
		"ISNULL", "JOIN", "KEY", "LAST", "LEFT", "LIKE", "LIMIT", "MATCH", "MATERIALIZED", "NATURAL", "NO", "NOT",
		"NOTHING", "NOTNULL", "NULL", "NULLS", "OF", "OFFSET", "ON", "OR", "ORDER", "OTHERS", "OUTER", "OVER",
		"PARTITION", "PLAN", "PRAGMA", "PRECEDING", "PRIMARY", "QUERY", "RAISE", "RANGE", "RECURSIVE", "REFERENCES",
		"REGEXP", "REINDEX", "RELEASE", "RENAME", "REPLACE", "RESTRICT", "RETURNING", "RIGHT", "ROLLBACK", "ROW", "ROWS",
		"SAVEPOINT", "SELECT", "SET", "TABLE", "TEMP", "TEMPORARY", "THEN", "TIES", "TO", "TRANSACTION", "TRIGGER",
		"UNBOUNDED", "UNION", "UNIQUE", "UPDATE", "USING", "VACUUM", "VALUES", "VIEW", "VIRTUAL", "WHEN", "WHERE",
		"WINDOW", "WITH", "WITHOUT":
		return true
	default:
		return false
	}
}

func isPlainIdentifier(name string) bool {
	if name == "" {
		return false
	}
	for index := 0; index < len(name); index++ {
		char := name[index]
		switch {
		case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', char == '_':
			continue
		case char >= '0' && char <= '9' && index > 0:
			continue
		}
		return false
	}
	return true
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
