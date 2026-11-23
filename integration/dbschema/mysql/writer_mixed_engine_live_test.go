//go:build integration

package mysql_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dbschema/mysql"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlident"
)

// mixedEngineLiveView is a view of the database newMixedEngineLiveDatabase
// creates. query is a format whose first operand is the MyISAM table audit and
// whose second is the InnoDB table users.
type mixedEngineLiveView struct {
	name  string
	query string
}

// newMixedEngineLiveDatabase creates a database on db holding a MyISAM table,
// an InnoDB table and views, and drops it when the test ends. It returns the
// database's name.
func newMixedEngineLiveDatabase(c *qt.C, db *sql.DB, dialect string, views ...mixedEngineLiveView) string {
	c.Helper()
	target := fmt.Sprintf("ptah_mixed_engine_%d", time.Now().UnixNano())
	createMySQLWriterLiveDatabase(c, db, target)
	c.Cleanup(func() {
		dropMySQLWriterLiveDatabase(c, db, target)
	})
	qualified := func(name string) string { return sqlident.Qualified(dialect, target, name) }
	statements := []string{
		"CREATE TABLE " + qualified("audit") + " (id BIGINT PRIMARY KEY) ENGINE=MyISAM",
		"CREATE TABLE " + qualified("users") + " (id BIGINT PRIMARY KEY) ENGINE=InnoDB",
	}
	for _, view := range views {
		statements = append(statements,
			"CREATE VIEW "+qualified(view.name)+" AS "+fmt.Sprintf(view.query, qualified("audit"), qualified("users")))
	}
	for _, statement := range statements {
		_, err := db.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return target
}

// mixedEngineLiveObjects counts the tables and views in target.
func mixedEngineLiveObjects(c *qt.C, db *sql.DB, target string) int {
	c.Helper()
	var objects int
	err := db.QueryRowContext(
		c.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ?",
		target,
	).Scan(&objects)
	c.Assert(err, qt.IsNil)
	return objects
}

// A database holding a MyISAM table, an InnoDB table and a view over each
// empties on every MySQL-family server. MySQL 26.10 enforces GTID consistency
// by default and refuses one WRITE lock over both kinds of table, so the
// cleanup locks, checks and drops each kind with the view that reads it.
func TestWriterDropAllTables_LiveEmptiesADatabaseHoldingBothKindsOfTable(t *testing.T) {
	tests := []struct {
		name    string
		engine  dbtarget.Engine
		dialect string
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin, dialect: platform.MySQL},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin, dialect: platform.MariaDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := openMySQLWriterLiveDatabase(c, test.engine)
			c.Cleanup(func() {
				c.Check(db.Close(), qt.IsNil)
			})
			target := newMixedEngineLiveDatabase(c, db, test.dialect,
				mixedEngineLiveView{name: "reads_audit", query: "SELECT id FROM %[1]s"},
				mixedEngineLiveView{name: "reads_users", query: "SELECT id FROM %[2]s"},
			)
			writer := mysql.NewMySQLWriter(db, target, test.dialect)

			err := writer.DropAllTables(c.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(mixedEngineLiveObjects(c, db, target), qt.Equals, 0)
		})
	}
}

// A view that reads a MyISAM and an InnoDB table cannot be WRITE-locked on a
// MySQL server that enforces GTID consistency, which is what keeps another
// session from reading it between the checks and the drop. The cleanup refuses
// it by name and drops nothing. The row needs such a server, as MySQL 26.10 is
// by default, and says so before it relies on it.
func TestWriterDropAllTables_LiveRefusesAViewOverBothKindsOfTable_FailurePath(t *testing.T) {
	c := qt.New(t)
	db := openMySQLWriterLiveDatabase(c, dbtarget.MySQLAdmin)
	c.Cleanup(func() {
		c.Check(db.Close(), qt.IsNil)
	})
	var enforced string
	c.Assert(db.QueryRowContext(c.Context(), "SELECT @@GLOBAL.enforce_gtid_consistency").Scan(&enforced), qt.IsNil)
	c.Assert(enforced, qt.Equals, "ON")
	target := newMixedEngineLiveDatabase(c, db, platform.MySQL,
		mixedEngineLiveView{name: "reads_both", query: "SELECT a.id FROM %[1]s a JOIN %[2]s u ON u.id = a.id"},
	)
	writer := mysql.NewMySQLWriter(db, target, platform.MySQL)

	err := writer.DropAllTables(c.Context())

	c.Assert(err, qt.ErrorMatches, `mysql: refusing to clean database "`+target+`": the server enforces GTID consistency, `+
		"so it refuses one WRITE lock over a transactional and a non-transactional table, and view `"+target+
		"`.`reads_both` reads both; without that lock another session can read the view while it is dropped")
	c.Assert(mixedEngineLiveObjects(c, db, target), qt.Equals, 3)
}
