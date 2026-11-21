package mysql

// sameName is a predicate that left and right name the same database, where
// each is a column or a SQL expression; a bind in either is bound twice, as the
// predicate repeats both.
//
// A server started with lower_case_table_names=0 keeps r_t and R_T apart, and
// the comparison of a name in information_schema ignores case wherever the
// view's collation does: on MariaDB 11.8.9 every view, on MySQL 8.4.13
// SCHEMA_PRIVILEGES, TABLE_PRIVILEGES and COLUMN_PRIVILEGES. There
// `TABLE_SCHEMA = 'r_t'` matches the rows of R_T as well. Without the byte
// comparison a read of r_t describes the routines, triggers and grants of R_T
// as its own, and a plan built on it revokes a grant on R_T. The plain
// equality stays first, so a server can still look the database up by it; the
// byte comparison after it keeps the rows of that one database. A server that
// folds names (1 or 2) holds no two databases that differ in case and compares
// as it would without this predicate.
func sameName(left, right string) string {
	return "(" + left + " = " + right +
		" AND (@@lower_case_table_names <> 0 OR CONVERT(" + left + " USING utf8mb4) COLLATE utf8mb4_bin = CONVERT(" +
		right + " USING utf8mb4)))"
}

// scopedDatabase is the database a cleanup or a scope read names: the bound
// schema, or the session's DATABASE() when the bound value is empty.
const scopedDatabase = "COALESCE(NULLIF(?, ''), DATABASE())"

// nameBinds is the bind list of a query whose every bind is name, for the
// given number of [sameName] predicates over one bind each.
func nameBinds(name string, predicates int) []any {
	binds := make([]any, 0, 2*predicates)
	for range 2 * predicates {
		binds = append(binds, name)
	}
	return binds
}
