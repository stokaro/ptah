//go:build integration

package mysql_test

import (
	"database/sql"
	"fmt"
	"regexp"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dbschema/mysql"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlident"
)

// newMixedEngineLiveDatabase creates a database on db holding the MyISAM table
// audit, the InnoDB table users and objects, and drops it when the test ends.
// Each object is a CREATE statement in which %[1]s stands for the quoted name
// of the database. It returns the database's name.
func newMixedEngineLiveDatabase(c *qt.C, db *sql.DB, dialect string, objects ...string) string {
	c.Helper()
	target := fmt.Sprintf("ptah_mixed_engine_%d", time.Now().UnixNano())
	createMySQLWriterLiveDatabase(c, db, target)
	c.Cleanup(func() {
		dropMySQLWriterLiveDatabase(c, db, target)
	})
	statements := append([]string{
		"CREATE TABLE %[1]s.`audit` (id BIGINT PRIMARY KEY) ENGINE=MyISAM",
		"CREATE TABLE %[1]s.`users` (id BIGINT PRIMARY KEY) ENGINE=InnoDB",
	}, objects...)
	for _, statement := range statements {
		statement = fmt.Sprintf(statement, sqlident.Quote(dialect, target))
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
func TestWriterDropAllTables_LiveEmptiesADatabaseHoldingBothKindsOfTable_HappyPath(t *testing.T) {
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
				"CREATE VIEW %[1]s.`reads_audit` AS SELECT id FROM %[1]s.`audit`",
				"CREATE VIEW %[1]s.`reads_users` AS SELECT id FROM %[1]s.`users`",
			)
			writer := mysql.NewMySQLWriter(db, target, test.dialect)

			err := writer.DropAllTables(c.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(mixedEngineLiveObjects(c, db, target), qt.Equals, 0)
		})
	}
}

// A view that reads a MyISAM and an InnoDB table cannot be WRITE-locked on a
// MySQL server that enforces GTID consistency, and neither can an InnoDB table
// whose trigger writes the MyISAM table, because the lock takes in the tables
// its triggers use. That lock is what keeps another session away between the
// checks and the drop, so the cleanup refuses the object by name before it
// drops a table or a view a session can open; these databases hold nothing
// else it drops first, so every object stays. The rows need such a server, as
// MySQL 26.10 is by default, and say so before they rely on it.
func TestWriterDropAllTables_LiveRefusesAnObjectNoLockCanProtect_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		objects     []string
		wantErr     string
		wantObjects int
	}{
		{
			name:    "a view over both kinds",
			objects: []string{"CREATE VIEW %[1]s.`reads_both` AS SELECT a.id FROM %[1]s.`audit` a JOIN %[1]s.`users` u ON u.id = a.id"},
			wantErr: "view `%[1]s`.`reads_both` reads both; without that lock another session can read the view " +
				"while it is dropped",
			wantObjects: 3,
		},
		{
			name: "a table whose trigger writes the other kind",
			objects: []string{
				"CREATE TABLE %[1]s.`logged_users` (id BIGINT PRIMARY KEY) ENGINE=InnoDB",
				"CREATE TRIGGER %[1]s.`log_user` AFTER INSERT ON %[1]s.`logged_users` FOR EACH ROW " +
					"INSERT INTO %[1]s.`audit` (id) VALUES (NEW.id)",
			},
			wantErr: "table `%[1]s`.`logged_users`, or a trigger on it, uses both kinds of table; without that lock " +
				"another session can open the table between the checks and its drop",
			wantObjects: 3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := openMySQLWriterLiveDatabase(c, dbtarget.MySQLAdmin)
			c.Cleanup(func() {
				c.Check(db.Close(), qt.IsNil)
			})
			var enforced string
			c.Assert(db.QueryRowContext(c.Context(), "SELECT @@GLOBAL.enforce_gtid_consistency").Scan(&enforced), qt.IsNil)
			c.Assert(enforced, qt.Equals, "ON")
			target := newMixedEngineLiveDatabase(c, db, platform.MySQL, test.objects...)
			writer := mysql.NewMySQLWriter(db, target, platform.MySQL)

			err := writer.DropAllTables(c.Context())

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(`mysql: refusing to clean database "`+target+
				`": the server enforces GTID consistency, so it refuses one WRITE lock over a transactional and a `+
				`non-transactional table, and `+fmt.Sprintf(test.wantErr, target)))
			c.Assert(mixedEngineLiveObjects(c, db, target), qt.Equals, test.wantObjects)
		})
	}
}
