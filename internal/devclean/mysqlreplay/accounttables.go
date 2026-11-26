package mysqlreplay

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// accountTables are the tables of the mysql schema that hold the server's
// accounts, their credentials and their privileges: MySQL's user, db,
// tables_priv, columns_priv, procs_priv, proxies_priv, role_edges,
// default_roles, global_grants and password_history, and MariaDB's
// global_priv and roles_mapping beside the db and privilege tables it shares,
// with the user view it keeps over global_priv.
//
// A statement that writes one changes an account as an account statement
// does, and the server checks a new connection against the change once it
// reloads the tables: at FLUSH PRIVILEGES or a restart, and on MySQL after an
// account statement that fails. Measured on MySQL 8.4.13 and 26.10.0, DELETE
// FROM mysql.user WHERE User = 'u' leaves u logging in, and CREATE ROLE r
// then GRANT r TO an account that does not exist, which fails with error
// 3523, refuses every new connection of u. On MariaDB 10.11.19 and 11.8.9
// the same GRANT fails with error 1133 and u still logs in until the tables
// are reloaded. So the guard refuses a write to one of them in every realm
// and under trust, before either is asked: an INSERT, a REPLACE, an INSERT
// ... SELECT, an UPDATE, a DELETE, a TRUNCATE, a LOAD DATA or LOAD XML, a
// LOCK TABLES, a RENAME TABLE, a CREATE, ALTER or DROP of the table or of an
// index on it, and a view over one, which a write through the view lands in.
// The other tables of the mysql schema, such as the time zone tables, are
// another database to the guard.
var accountTables = []string{
	"user", "db", "tables_priv", "columns_priv", "procs_priv", "proxies_priv",
	"role_edges", "default_roles", "global_grants", "password_history",
	"global_priv", "roles_mapping",
}

// accountTableOperation names the refusal of a statement that writes one of
// [accountTables], or of a view over one, and returns "" for any other
// statement. database is the database the statement runs in, the one an
// unqualified name is in.
func accountTableOperation(tokens []lexer.Token, database string) string {
	tokens = replay.LeadingCTEExecutableTokens(tokens)
	for _, target := range mysqlMutationTargets(tokens) {
		if table := accountTable(target, database); table != "" {
			return fmt.Sprintf("write to account table %q", table)
		}
	}
	view, tables := mysqlViewTables(tokens)
	for _, target := range tables {
		if table := accountTable(target, database); table != "" {
			return fmt.Sprintf("view %q over account table %q", strings.Join(view, "."), table)
		}
	}
	return ""
}

// accountTable names target, a table as a statement names it, as
// database.table where it is one of [accountTables], and returns "" for any
// other table. Names are compared in either case, as a server whose
// lower_case_table_names is set compares them.
func accountTable(target []string, database string) string {
	schema, table := database, ""
	switch len(target) {
	case 1:
		table = target[0]
	case 2:
		schema, table = target[0], target[1]
	default:
		return ""
	}
	if !strings.EqualFold(schema, "mysql") ||
		!slices.ContainsFunc(accountTables, func(name string) bool { return strings.EqualFold(name, table) }) {
		return ""
	}
	return schema + "." + table
}
