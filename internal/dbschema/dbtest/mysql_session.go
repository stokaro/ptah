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

// MySQLAllGrants is the SHOW GRANTS answer of an account that sees everything.
var MySQLAllGrants = []string{"GRANT ALL PRIVILEGES ON *.* TO `root`@`%` WITH GRANT OPTION"}

// MySQLSession answers the questions a MySQL-family reader asks about its own
// session, as an account holding grants on a server of version, and hands every
// other question to next.
func MySQLSession(version string, grants []string, next QueryHandler) QueryHandler {
	return func(query string, args []driver.NamedValue) (QueryResult, error) {
		switch {
		case strings.Contains(query, "SELECT CURRENT_USER(), VERSION()"):
			return QueryResult{
				Columns: []string{"CURRENT_USER()", "VERSION()"},
				Rows:    [][]driver.Value{{"app@%", version}},
			}, nil
		case strings.TrimSpace(query) == "SHOW GRANTS":
			rows := make([][]driver.Value, 0, len(grants))
			for _, grant := range grants {
				rows = append(rows, []driver.Value{grant})
			}
			return QueryResult{Columns: []string{"Grants"}, Rows: rows}, nil
		case strings.HasPrefix(strings.TrimSpace(query), "SHOW VARIABLES WHERE Variable_name IN ('partial_revokes'"):
			return QueryResult{Columns: []string{"Variable_name", "Value"}}, nil
		case strings.TrimSpace(query) == "SHOW PRIVILEGES":
			return QueryResult{
				Columns: []string{"Privilege", "Context", "Comment"},
				Rows:    [][]driver.Value{{"SHOW_ROUTINE", "Server Admin", ""}, {"Show Create Routine", "Databases", ""}},
			}, nil
		}
		return next(query, args)
	}
}
