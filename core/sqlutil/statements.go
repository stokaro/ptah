package sqlutil

import "strings"

// SplitStatements is the one-call composition the migrator's SQL split
// wrappers used to be: client delimiter directives are normalized first —
// they live in comments, so they must be read before comments are stripped —
// then comments are removed and the remainder is split into statements.
func SplitStatements(sql string) []string {
	normalized := NormalizeClientDelimiters(sql)
	return SplitSQLStatements(StripComments(normalized))
}

// SplitStatementsForDialect is the dialect-aware sibling of [SplitStatements]
// and the one-call composition the migrator's connection-scoped wrapper used
// to be: statements are split with the dialect's string-literal scanning (a
// backslash is a C-style escape only for MySQL/MariaDB/ClickHouse), then each
// statement is stripped of comments for that dialect and dropped when nothing
// remains. Callers that execute the resulting statements one by one against a
// live connection should pass the connection's dialect, so a semicolon inside
// a backslash-escaped literal cannot leak out into a separately-executed
// statement. A blank dialect falls back to the dialect-blind
// [SplitStatements].
func SplitStatementsForDialect(dialect, sql string) []string {
	if strings.TrimSpace(dialect) == "" {
		return SplitStatements(sql)
	}
	statements := SplitSQLStatementsForDialect(sql, dialect)
	filtered := statements[:0]
	for _, stmt := range statements {
		stmt = strings.TrimSpace(StripCommentsForDialect(stmt, dialect))
		if stmt != "" {
			filtered = append(filtered, stmt)
		}
	}
	return filtered
}
