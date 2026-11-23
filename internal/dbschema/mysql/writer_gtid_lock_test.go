package mysql_test

import (
	"database/sql/driver"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
)

// A MySQL server that enforces GTID consistency refuses one WRITE lock over a
// MyISAM and an InnoDB table. Each kind of table is locked, checked and dropped
// on its own, after the server is asked which names it locks together.
func TestWriterDropAllTables_LocksEachKindOfTableApartWhenTheServerRefusesOneLock(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks: 1,
		nonTransactional: []string{"audit"},
		objects: [][]driver.Value{
			{"audit", "TABLE"},
			{"users", "TABLE"},
		},
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "26.10.0")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"LOCK TABLES `test`.`audit` WRITE, `test`.`users` WRITE",
		"LOCK TABLES `test`.`audit` WRITE",
		"LOCK TABLES `test`.`audit` WRITE, `test`.`users` WRITE",
		"LOCK TABLES `test`.`users` WRITE",
		"LOCK TABLES `test`.`audit` WRITE",
		"DROP TABLE IF EXISTS `test`.`audit`",
		"UNLOCK TABLES",
		"LOCK TABLES `test`.`users` WRITE",
		"DROP TABLE IF EXISTS `test`.`users`",
		"UNLOCK TABLES",
	})
}

// Each kind of table is checked again under its own lock, so a view from
// another database that comes to read the InnoDB table after the MyISAM table
// is dropped stops the cleanup before the InnoDB table goes.
func TestWriterDropAllTables_RechecksEachKindOfTableUnderItsOwnLock(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks:   1,
		nonTransactional:   []string{"audit"},
		externalViewCounts: []int{0, 0, 1},
		objects: [][]driver.Value{
			{"audit", "TABLE"},
			{"users", "TABLE"},
		},
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "26.10.0")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches,
		`mysql: refusing to clean database "test": 1 views from other databases reference it`,
	)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"LOCK TABLES `test`.`audit` WRITE, `test`.`users` WRITE",
		"LOCK TABLES `test`.`audit` WRITE",
		"LOCK TABLES `test`.`audit` WRITE, `test`.`users` WRITE",
		"LOCK TABLES `test`.`users` WRITE",
		"LOCK TABLES `test`.`audit` WRITE",
		"DROP TABLE IF EXISTS `test`.`audit`",
		"UNLOCK TABLES",
		"LOCK TABLES `test`.`users` WRITE",
		"UNLOCK TABLES",
	})
}

// A view over the InnoDB table is handed off under a lock with that table, and
// the MyISAM table, which no view reads, waits for the table step.
func TestWriterDropAllTables_HandsOffAViewWithTheTablesItLocksWith(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks: 1,
		viewDropStarted:  make(chan struct{}),
		viewDropRelease:  make(chan struct{}),
		nonTransactional: []string{"audit"},
		objects: [][]driver.Value{
			{"active_users", "VIEW"},
			{"audit", "TABLE"},
			{"users", "TABLE"},
		},
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "26.10.0")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"LOCK TABLES `test`.`active_users` WRITE, `test`.`audit` WRITE, `test`.`users` WRITE",
		"LOCK TABLES `test`.`active_users` WRITE",
		"LOCK TABLES `test`.`active_users` WRITE, `test`.`audit` WRITE",
		"LOCK TABLES `test`.`audit` WRITE",
		"LOCK TABLES `test`.`active_users` WRITE, `test`.`users` WRITE",
		"LOCK TABLES `test`.`active_users` WRITE, `test`.`users` WRITE",
		"DROP VIEW IF EXISTS `test`.`active_users`",
		"UNLOCK TABLES",
		"LOCK TABLES `test`.`audit` WRITE, `test`.`users` WRITE",
		"LOCK TABLES `test`.`audit` WRITE",
		"LOCK TABLES `test`.`audit` WRITE, `test`.`users` WRITE",
		"LOCK TABLES `test`.`users` WRITE",
		"LOCK TABLES `test`.`audit` WRITE",
		"DROP TABLE IF EXISTS `test`.`audit`",
		"UNLOCK TABLES",
		"LOCK TABLES `test`.`users` WRITE",
		"DROP TABLE IF EXISTS `test`.`users`",
		"UNLOCK TABLES",
	})
}

// A view in a later group is handed off under a lock of its own over it and
// the table it reads, taken after the first group's views are dropped. The
// position is asserted rather than the presence: the same LOCK TABLES appears
// earlier, while the server is asked which names it locks together, so a
// handoff that skips the lock still contains it.
func TestWriterDropAllTables_LocksEachViewGroupBeforeItsHandoff(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks: 1,
		viewDropStarted:  make(chan struct{}),
		viewDropRelease:  make(chan struct{}),
		nonTransactional: []string{"audit", "reads_audit"},
		objects: [][]driver.Value{
			{"reads_audit", "VIEW"},
			{"reads_users", "VIEW"},
			{"audit", "TABLE"},
			{"users", "TABLE"},
		},
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "26.10.0")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	statements := recorder.statements()
	firstDrop := slices.Index(statements, "DROP VIEW IF EXISTS `test`.`reads_audit`")
	c.Assert(firstDrop, qt.Not(qt.Equals), -1, qt.Commentf("%q", statements))
	c.Assert(statements[firstDrop+1:firstDrop+3], qt.DeepEquals, []string{
		"UNLOCK TABLES",
		"LOCK TABLES `test`.`reads_users` WRITE, `test`.`users` WRITE",
	})
}

// A table with a trigger that writes a table of the other kind cannot be
// WRITE-locked even alone on a server that enforces GTID consistency, because
// the lock takes in the tables its triggers use. The cleanup refuses it as a
// table before it drops one.
func TestWriterDropAllTables_RefusesATableNoLockCanProtect(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks: 1,
		nonTransactional: []string{"audit"},
		usesBothKinds:    []string{"logged_users"},
		objects: [][]driver.Value{
			{"audit", "TABLE"},
			{"logged_users", "TABLE"},
			{"users", "TABLE"},
		},
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "26.10.0")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches,
		"mysql: refusing to clean database \"test\": the server enforces GTID consistency, so it refuses one "+
			"WRITE lock over a transactional and a non-transactional table, and table `test`.`logged_users`, or a "+
			"trigger on it, uses both kinds of table; without that lock another session can open the table between "+
			"the checks and its drop",
	)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"LOCK TABLES `test`.`audit` WRITE, `test`.`logged_users` WRITE, `test`.`users` WRITE",
		"LOCK TABLES `test`.`audit` WRITE",
		"LOCK TABLES `test`.`audit` WRITE, `test`.`logged_users` WRITE",
		"LOCK TABLES `test`.`logged_users` WRITE",
		"UNLOCK TABLES",
	})
}

// A view that reads a MyISAM and an InnoDB table cannot be WRITE-locked on a
// server that enforces GTID consistency, and nothing else protects its drop,
// so the cleanup refuses it by name before it drops anything.
func TestWriterDropAllTables_RefusesAViewNoLockCanProtect(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks: 1,
		nonTransactional: []string{"audit"},
		usesBothKinds:    []string{"audited_users"},
		objects: [][]driver.Value{
			{"audited_users", "VIEW"},
			{"audit", "TABLE"},
			{"users", "TABLE"},
		},
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "26.10.0")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches,
		"mysql: refusing to clean database \"test\": the server enforces GTID consistency, so it refuses one "+
			"WRITE lock over a transactional and a non-transactional table, and view `test`.`audited_users` reads "+
			"both; without that lock another session can read the view while it is dropped",
	)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"LOCK TABLES `test`.`audited_users` WRITE, `test`.`audit` WRITE, `test`.`users` WRITE",
		"LOCK TABLES `test`.`audited_users` WRITE",
		"UNLOCK TABLES",
	})
}
