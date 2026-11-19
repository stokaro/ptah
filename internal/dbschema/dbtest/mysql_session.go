package dbtest

import (
	"database/sql/driver"
	"strings"
)

// The server versions a scripted MySQL-family session reports.
const (
	// MySQLVersion is a MySQL banner.
	MySQLVersion = "8.4.13"
	// MariaDBVersion is a MariaDB banner.
	MariaDBVersion = "12.3.3-MariaDB-ubu2404"
)

// MySQLSQLMode is the sql_mode a scripted MySQL-family session reports:
// MySQL's default, under which a backslash in a string literal escapes.
const MySQLSQLMode = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"

// MySQLAllGrants is the SHOW GRANTS answer of an account that sees everything.
var MySQLAllGrants = []string{"GRANT ALL PRIVILEGES ON *.* TO `root`@`%` WITH GRANT OPTION"}

// MySQLSession answers the questions a MySQL-family reader asks about its own
// session, as an account holding grants on a server of version, and hands every
// other question to next. The session's sql_mode is [MySQLSQLMode]. The
// character set facts are MySQL 8.4's: utf8mb4 and latin1 with their default
// collations, the default_collation_for_utf8mb4 a fresh session holds, and
// utf8 naming utf8mb3.
func MySQLSession(version string, grants []string, next QueryHandler) QueryHandler {
	return MySQLSessionInMode(version, MySQLSQLMode, grants, next)
}

// MySQLSessionInMode is [MySQLSession] for a session whose sql_mode is
// sqlMode, such as one naming NO_BACKSLASH_ESCAPES.
func MySQLSessionInMode(version, sqlMode string, grants []string, next QueryHandler) QueryHandler {
	return func(query string, args []driver.NamedValue) (QueryResult, error) {
		switch {
		case strings.Contains(query, "SELECT CURRENT_USER(), VERSION()"):
			return QueryResult{
				Columns: []string{"CURRENT_USER()", "VERSION()", "@@SESSION.sql_mode"},
				Rows:    [][]driver.Value{{"app@%", version, sqlMode}},
			}, nil
		case strings.TrimSpace(query) == "SHOW GRANTS":
			rows := make([][]driver.Value, 0, len(grants))
			for _, grant := range grants {
				rows = append(rows, []driver.Value{grant})
			}
			return QueryResult{Columns: []string{"Grants"}, Rows: rows}, nil
		case strings.HasPrefix(strings.TrimSpace(query), "SHOW VARIABLES WHERE Variable_name IN ('partial_revokes'"):
			return QueryResult{Columns: []string{"Variable_name", "Value"}}, nil
		case strings.Contains(query, "FROM information_schema.CHARACTER_SETS"):
			return QueryResult{
				Columns: []string{"CHARACTER_SET_NAME", "DEFAULT_COLLATE_NAME"},
				Rows:    [][]driver.Value{{"utf8mb4", "utf8mb4_0900_ai_ci"}, {"latin1", "latin1_swedish_ci"}},
			}, nil
		case strings.TrimSpace(query) == "SHOW SESSION VARIABLES LIKE 'default_collation_for_utf8mb4'":
			return QueryResult{
				Columns: []string{"Variable_name", "Value"},
				Rows:    [][]driver.Value{{"default_collation_for_utf8mb4", "utf8mb4_0900_ai_ci"}},
			}, nil
		case strings.TrimSpace(query) == "SELECT CHARSET(CONVERT('' USING utf8))":
			return QueryResult{Columns: []string{"CHARSET(CONVERT('' USING utf8))"}, Rows: [][]driver.Value{{"utf8mb3"}}}, nil
		case strings.TrimSpace(query) == "SHOW PRIVILEGES":
			return QueryResult{
				Columns: []string{"Privilege", "Context", "Comment"},
				Rows:    [][]driver.Value{{"SHOW_ROUTINE", "Server Admin", ""}, {"Show Create Routine", "Databases", ""}},
			}, nil
		}
		return next(query, args)
	}
}
