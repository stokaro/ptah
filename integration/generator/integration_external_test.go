//go:build integration

package generator_test

import (
	"strings"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
)

// execScript executes generated SQL using the same statement splitter as the
// migrator. Database-specific integration tests use the raw connection because
// MySQL and MariaDB commit DDL independently of transaction boundaries.
func execScript(c *qt.C, conn *dbschema.DatabaseConnection, sqlText, label string) {
	c.Helper()
	for _, stmt := range sqlutil.SplitStatements(sqlText) {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		_, err := conn.Exec(stmt)
		c.Assert(err, qt.IsNil, qt.Commentf("%s statement must apply cleanly:\n%s", label, stmt))
	}
}

func dropTableSQL(dialect, table string) string {
	if dialect == "mysql" || dialect == "mariadb" {
		return "DROP TABLE IF EXISTS " + table
	}
	return "DROP TABLE IF EXISTS " + table + " CASCADE"
}
