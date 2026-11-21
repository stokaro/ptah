package mysql

// scopedDatabase is the database a cleanup or a scope read names: the bound
// schema, or the session's DATABASE() when the bound value is empty.
const scopedDatabase = "COALESCE(NULLIF(?, ''), DATABASE())"

// nameBinds is the bind list of a query whose every bind is name, for the
// given number of [mysqlname.SameDatabase] predicates over one bind each.
func nameBinds(name string, predicates int) []any {
	binds := make([]any, 0, 2*predicates)
	for range 2 * predicates {
		binds = append(binds, name)
	}
	return binds
}
