package mysql_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
	"go.5x5.cz/ptah/internal/dbschema/mysql"
	"go.5x5.cz/ptah/internal/sqlrunner"
)

func TestWriterDropAllTables_RestoresForeignKeyChecksAfterCancellation(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	recorder := &mysqlCleanupRecorder{
		cancelOnDrop: cancel,
		dropErr:      context.Canceled,
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")

	err := writer.DropAllTables(ctx)

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"SET FOREIGN_KEY_CHECKS = 1",
		"LOCK TABLES `test`.`users` WRITE",
		"DROP TABLE IF EXISTS `test`.`users`",
		"UNLOCK TABLES",
		"SET FOREIGN_KEY_CHECKS = 0",
	})
}

func TestWriterDropAllTables_JoinsPrimaryAndRestoreErrors(t *testing.T) {
	c := qt.New(t)
	dropErr := errors.New("drop failed")
	restoreErr := errors.New("restore failed")
	recorder := &mysqlCleanupRecorder{
		dropErr:    dropErr,
		restoreErr: restoreErr,
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorIs, dropErr)
	c.Assert(err, qt.ErrorIs, restoreErr)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"SET FOREIGN_KEY_CHECKS = 1",
		"LOCK TABLES `test`.`users` WRITE",
		"DROP TABLE IF EXISTS `test`.`users`",
		"UNLOCK TABLES",
		"SET FOREIGN_KEY_CHECKS = 0",
	})
}

func TestWriterDropAllTables_TemporarilyEnablesForeignKeyChecks(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"SET FOREIGN_KEY_CHECKS = 1",
		"LOCK TABLES `test`.`users` WRITE",
		"DROP TABLE IF EXISTS `test`.`users`",
		"UNLOCK TABLES",
		"SET FOREIGN_KEY_CHECKS = 0",
	})
}

func TestWriterDropDatabaseRealm_UsesPinnedConnectionForVerification(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks: 1,
		objects:          [][]driver.Value{},
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	db.SQL.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	t.Cleanup(cancel)
	conn, err := db.SQL.Conn(ctx)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})
	writer := mysql.NewMySQLWriterForPinnedRunner(
		sqlrunner.NewConn(ctx, conn),
		db.SQL,
		conn,
		"test",
		platform.MySQL,
		"8.0.13",
	)

	err = writer.DropDatabaseRealm(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(recorder.catalogCount(), qt.Equals, 2)
	c.Assert(recorder.statements(), qt.HasLen, 0)
}

func TestWriterDropDatabaseRealm_RejectsProtectedDatabasesBeforeMutation(t *testing.T) {
	c := qt.New(t)
	tests := []string{
		"information_schema",
		"METRICS_SCHEMA",
		"mysql",
		"mysql_innodb_cluster_metadata",
		"mysql_innodb_cluster_metadata_backup",
		"mysql_innodb_cluster_metadata_bkp",
		"mysql_innodb_cluster_metadata_previous",
		"ndbinfo",
		"performance_schema",
		"sys",
	}

	for _, database := range tests {
		c.Run(database, func(c *qt.C) {
			recorder := &mysqlCleanupRecorder{}
			db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
			writer := mysql.NewMySQLWriterWithServerVersion(
				db.SQL,
				database,
				platform.MySQL,
				"8.0.13",
			)

			err := writer.DropDatabaseRealm(t.Context())

			c.Assert(
				err,
				qt.ErrorMatches,
				`mysql: refusing to clean protected database ".*"`,
			)
			c.Assert(db.QueryCount(), qt.Equals, 0)
			c.Assert(recorder.statements(), qt.HasLen, 0)
		})
	}
}

func TestWriterDropDatabaseRealm_RejectsProtectedSelectedDatabaseBeforeMutation(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{}
	db := dbtest.OpenWithExec(t, func(
		query string,
		_ []driver.NamedValue,
	) (dbtest.QueryResult, error) {
		c.Assert(query, qt.Equals, "SELECT DATABASE()")
		return dbtest.QueryResult{
			Columns: []string{"database"},
			Rows:    [][]driver.Value{{"mysql"}},
		}, nil
	}, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(
		db.SQL,
		"",
		platform.MySQL,
		"8.0.13",
	)

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorMatches, `mysql: refusing to clean protected database "mysql"`)
	c.Assert(db.QueryCount(), qt.Equals, 1)
	c.Assert(recorder.statements(), qt.HasLen, 0)
}

func TestWriterDropAllTables_RestoresAfterEnableFailure(t *testing.T) {
	c := qt.New(t)
	enableErr := errors.New("enable outcome unknown")
	recorder := &mysqlCleanupRecorder{
		enableErr: enableErr,
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorIs, enableErr)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"SET FOREIGN_KEY_CHECKS = 1",
		"SET FOREIGN_KEY_CHECKS = 0",
	})
}

func TestWriterDropAllTables_RejectsCrossDatabaseForeignKeys(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks:    1,
		externalForeignKeys: 2,
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches, `mysql: refusing to clean database "test": 2 foreign key constraints from other databases reference it`)
	c.Assert(recorder.statements(), qt.HasLen, 0)
}

func TestWriterDropAllTables_RejectsCrossDatabaseViews(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks: 1,
		externalViews:    2,
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches, `mysql: refusing to clean database "test": 2 views from other databases reference it`)
	c.Assert(recorder.statements(), qt.HasLen, 0)
	viewQuery, viewArgs := recorder.viewCatalog()
	c.Assert(viewQuery, qt.Contains, "information_schema.view_table_usage")
	c.Assert(viewArgs, qt.DeepEquals, []driver.NamedValue{
		{Ordinal: 1, Value: "test"},
		{Ordinal: 2, Value: "test"},
	})
}

func TestWriterDropAllTables_RejectsMariaDBCrossDatabaseViews(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks: 1,
		externalViews:    2,
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MariaDB, "10.11.15-MariaDB")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches, `mysql: refusing to clean database "test": 2 views from other databases reference it`)
	c.Assert(recorder.statements(), qt.HasLen, 0)
	viewQuery, viewArgs := recorder.viewCatalog()
	c.Assert(viewQuery, qt.Contains, "information_schema.views")
	c.Assert(viewQuery, qt.Contains, "INSTR(view_definition, ?)")
	c.Assert(viewArgs, qt.DeepEquals, []driver.NamedValue{
		{Ordinal: 1, Value: "test"},
		{Ordinal: 2, Value: "`test`."},
	})
}

func TestWriterDropAllTables_RejectsExternalStoredProgramsBeforeMutation(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name   string
		schema string
		object string
		kind   string
	}{
		{name: "function", schema: "reporting", object: "load_events", kind: "FUNCTION"},
		{name: "procedure", schema: "reporting", object: "refresh_events", kind: "PROCEDURE"},
		{name: "event", schema: "scheduler", object: "nightly_rollup", kind: "EVENT"},
		{name: "trigger", schema: "audit", object: "capture_event", kind: "TRIGGER"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			recorder := &mysqlCleanupRecorder{
				foreignKeyChecks: 1,
				externalStoredPrograms: [][]driver.Value{{
					test.schema,
					test.object,
					test.kind,
				}},
			}
			db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
			writer := mysql.NewMySQLWriterWithServerVersion(
				db.SQL,
				"test",
				platform.MySQL,
				"8.0.13",
			)

			err := writer.DropAllTables(t.Context())

			c.Assert(
				err,
				qt.ErrorMatches,
				`mysql: refusing to clean database "test": external `+
					strings.ToLower(test.kind)+
					" `"+test.schema+"`.`"+test.object+"` may reference the cleanup realm",
			)
			c.Assert(recorder.statements(), qt.HasLen, 0)
		})
	}
}

func TestWriterDropAllTables_FailsClosedWithoutGlobalMetadataVisibility(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks:        1,
		missingGlobalPrivileges: true,
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches,
		`mysql: refusing to clean database "test": global SELECT, DROP, ALTER, ALTER ROUTINE, EVENT, LOCK TABLES, PROCESS, and TRIGGER privileges are required `+
			`to prove complete metadata visibility and protect destructive DDL`,
	)
	c.Assert(recorder.statements(), qt.HasLen, 0)
}

func TestWriterDropAllTables_RequiresShowRoutineOnModernMySQL(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks:   1,
		missingShowRoutine: true,
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "9.7.1")

	err := writer.DropAllTables(t.Context())

	c.Assert(
		err,
		qt.ErrorMatches,
		`mysql: refusing to clean database "test": global SELECT, DROP, ALTER, ALTER ROUTINE, `+
			`EVENT, LOCK TABLES, PROCESS, SHOW_ROUTINE, and TRIGGER privileges are required `+
			`to prove complete metadata visibility and protect destructive DDL`,
	)
	c.Assert(recorder.statements(), qt.HasLen, 0)
}

func TestWriterDropAllTables_RequiresTriggerVisibilityOnMySQL(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks: 1,
		missingTrigger:   true,
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")

	err := writer.DropAllTables(t.Context())

	c.Assert(
		err,
		qt.ErrorMatches,
		`mysql: refusing to clean database "test": global SELECT, DROP, ALTER, ALTER ROUTINE, `+
			`EVENT, LOCK TABLES, PROCESS, and TRIGGER privileges are required `+
			`to prove complete metadata visibility and protect destructive DDL`,
	)
	c.Assert(recorder.statements(), qt.HasLen, 0)
}

func TestWriterDropAllTables_DoesNotRequireTriggerVisibilityOnMariaDB(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks: 1,
		missingTrigger:   true,
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(
		db.SQL,
		"test",
		platform.MariaDB,
		"10.11.15-MariaDB",
	)

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
}

func TestWriterDropAllTables_FailsClosedForPartialPrivilegeRevokes(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks:        1,
		partialPrivilegeRevokes: true,
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches,
		`mysql: refusing to clean database "test": partial privilege revokes `+
			`prevent proving complete metadata visibility`,
	)
	c.Assert(recorder.statements(), qt.HasLen, 0)
}

func TestWriterDropAllTables_RequiresMariaDBViewVisibility(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks:        1,
		missingGlobalPrivileges: true,
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MariaDB, "10.11.15-MariaDB")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches,
		`mysql: refusing to clean database "test": global SELECT, DROP, ALTER, ALTER ROUTINE, EVENT, LOCK TABLES, PROCESS, and SHOW VIEW privileges are required `+
			`to prove complete metadata visibility and protect destructive DDL`,
	)
	c.Assert(recorder.statements(), qt.HasLen, 0)
}

func TestWriterDropAllTables_RefusesLegacyMySQLViewMetadata(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{foreignKeyChecks: 1}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.12")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches,
		`mysql: refusing to clean database "test": server version "8.0.12" lacks `+
			`information_schema.VIEW_TABLE_USAGE required for complete external-view dependency checks`,
	)
	c.Assert(recorder.statements(), qt.HasLen, 0)
}

func TestWriterDropAllTables_FailsClosedWhenViewInspectionFails(t *testing.T) {
	c := qt.New(t)
	inspectionErr := errors.New("view metadata unavailable")
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks: 1,
		viewQueryErr:     inspectionErr,
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorIs, inspectionErr)
	c.Assert(err, qt.ErrorMatches, "mysql: inspect cross-database views: view metadata unavailable")
	c.Assert(recorder.statements(), qt.HasLen, 0)
}

func TestWriterDropAllTables_RejectsForeignKeyCreatedBeforeLockedRecheck(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks:         1,
		externalForeignKeyCounts: []int{0, 1},
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches,
		`mysql: refusing to clean database "test": 1 foreign key constraints from other databases reference it`,
	)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"LOCK TABLES `test`.`users` WRITE",
		"UNLOCK TABLES",
	})
}

func TestWriterDropAllTables_RejectsViewCreatedBeforeLockedRecheck(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks:   1,
		externalViewCounts: []int{0, 1},
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches,
		`mysql: refusing to clean database "test": 1 views from other databases reference it`,
	)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"LOCK TABLES `test`.`users` WRITE",
		"UNLOCK TABLES",
	})
}

func TestWriterDropAllTables_DropsManagedViewsThroughProtectedHandoff(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		dialect string
		version string
	}{
		{
			name:    "mysql",
			dialect: platform.MySQL,
			version: "8.0.13",
		},
		{
			name:    "mariadb",
			dialect: platform.MariaDB,
			version: "10.11.15-MariaDB",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			recorder := &mysqlCleanupRecorder{
				foreignKeyChecks: 1,
				viewDropStarted:  make(chan struct{}),
				viewDropRelease:  make(chan struct{}),
				internalForeignKeys: [][]driver.Value{
					{"users", "fk_users_account"},
				},
				objects: [][]driver.Value{
					{"active_users", "VIEW"},
					{"users", "TABLE"},
				},
			}
			db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
			writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", test.dialect, test.version)

			err := writer.DropAllTables(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(recorder.statements(), qt.DeepEquals, []string{
				"LOCK TABLES `test`.`active_users` WRITE, `test`.`users` WRITE",
				"DROP VIEW IF EXISTS `test`.`active_users`",
				"UNLOCK TABLES",
				"ALTER TABLE `test`.`users` DROP FOREIGN KEY `fk_users_account`",
				"LOCK TABLES `test`.`users` WRITE",
				"DROP TABLE IF EXISTS `test`.`users`",
				"UNLOCK TABLES",
			})
		})
	}
}

func TestWriterDropAllTables_RejectsCompetingMetadataLockWaiter(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks:         1,
		otherMetadataLockWaiters: 1,
		viewDropStarted:          make(chan struct{}),
		viewDropRelease:          make(chan struct{}),
		objects: [][]driver.Value{
			{"active_users", "VIEW"},
			{"users", "TABLE"},
		},
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches,
		`mysql: refusing view cleanup: 1 competing metadata-lock waiters appeared before the protected DROP VIEW handoff`,
	)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"LOCK TABLES `test`.`active_users` WRITE, `test`.`users` WRITE",
		"DROP VIEW IF EXISTS `test`.`active_users`",
		"UNLOCK TABLES",
	})
}

func TestWriterDropAllTables_ReturnsImmediateViewDropFailure(t *testing.T) {
	c := qt.New(t)
	viewDropErr := errors.New("view drop failed before waiting")
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks: 1,
		viewDropStarted:  make(chan struct{}),
		viewDropErr:      viewDropErr,
		objects: [][]driver.Value{
			{"active_users", "VIEW"},
			{"users", "TABLE"},
		},
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorIs, viewDropErr)
	c.Assert(err, qt.ErrorMatches,
		"failed to drop view active_users: SQL execution failed: view drop failed before waiting\n"+
			"SQL: DROP VIEW IF EXISTS `test`.`active_users`",
	)
	statements := recorder.statements()
	c.Assert(statements, qt.ContentEquals, []string{
		"LOCK TABLES `test`.`active_users` WRITE, `test`.`users` WRITE",
		"DROP VIEW IF EXISTS `test`.`active_users`",
		"UNLOCK TABLES",
	})
}

func TestWriterDropAllTables_AcceptsImmediateViewDropCompletion(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks: 1,
		viewDropStarted:  make(chan struct{}),
		objects: [][]driver.Value{
			{"active_users", "VIEW"},
			{"users", "TABLE"},
		},
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	statements := recorder.statements()
	c.Assert(statements, qt.ContentEquals, []string{
		"LOCK TABLES `test`.`active_users` WRITE, `test`.`users` WRITE",
		"DROP VIEW IF EXISTS `test`.`active_users`",
		"UNLOCK TABLES",
		"LOCK TABLES `test`.`users` WRITE",
		"DROP TABLE IF EXISTS `test`.`users`",
		"UNLOCK TABLES",
	})
}

func TestWriterDropAllTables_ReservesAuxiliaryConnectionsBeforeLocking(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks: 1,
		objects: [][]driver.Value{
			{"active_users", "VIEW"},
			{"users", "TABLE"},
		},
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	db.SQL.SetMaxOpenConns(2)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	t.Cleanup(cancel)

	err := writer.DropAllTables(ctx)

	c.Assert(err, qt.ErrorMatches,
		`mysql: acquire metadata-lock monitor connection: context deadline exceeded`,
	)
	c.Assert(recorder.statements(), qt.HasLen, 0)
}

func TestWriterDropAllTables_DropsAllSupportedObjects(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks: 1,
		internalForeignKeys: [][]driver.Value{
			{"child`records", "fk`parent"},
			{"users", "fk_users_account"},
		},
		objects: [][]driver.Value{
			{"nightly_cleanup", "EVENT"},
			{"normalize_email", "FUNCTION"},
			{"refresh_users", "PROCEDURE"},
			{"users", "TABLE"},
			{"order_numbers", "SEQUENCE"},
		},
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"ALTER TABLE `test`.`child``records` DROP FOREIGN KEY `fk``parent`",
		"ALTER TABLE `test`.`users` DROP FOREIGN KEY `fk_users_account`",
		"LOCK TABLES `test`.`users` WRITE",
		"DROP TABLE IF EXISTS `test`.`users`",
		"UNLOCK TABLES",
		"DROP EVENT IF EXISTS `test`.`nightly_cleanup`",
		"DROP FUNCTION IF EXISTS `test`.`normalize_email`",
		"DROP PROCEDURE IF EXISTS `test`.`refresh_users`",
		"DROP SEQUENCE IF EXISTS `test`.`order_numbers`",
	})
	catalogQuery, catalogArgs := recorder.catalog()
	c.Assert(catalogQuery, qt.Contains, "information_schema.tables")
	c.Assert(catalogQuery, qt.Contains, "information_schema.routines")
	c.Assert(catalogQuery, qt.Contains, "information_schema.events")
	c.Assert(catalogQuery, qt.Contains, "'SYSTEM VERSIONED'")
	c.Assert(catalogArgs, qt.DeepEquals, []driver.NamedValue{
		{Ordinal: 1, Value: "test"},
		{Ordinal: 2, Value: "test"},
		{Ordinal: 3, Value: "test"},
	})
}

func TestWriterDropAllTables_FailsClosedWhenForeignKeyInspectionFails(t *testing.T) {
	c := qt.New(t)
	inspectionErr := errors.New("access denied")
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks:           1,
		externalForeignKeyQueryErr: inspectionErr,
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorIs, inspectionErr)
	c.Assert(err, qt.ErrorMatches, "mysql: inspect cross-database foreign keys: access denied")
	c.Assert(recorder.statements(), qt.HasLen, 0)
}

func TestWriterDropAllTables_StopsWhenInternalForeignKeyDropFails(t *testing.T) {
	c := qt.New(t)
	dropErr := errors.New("cannot drop constraint")
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks: 1,
		internalForeignKeys: [][]driver.Value{
			{"users", "fk_users_account"},
		},
		foreignKeyDropErr: dropErr,
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorIs, dropErr)
	c.Assert(err, qt.ErrorMatches,
		"failed to drop foreign key fk_users_account on table users: "+
			"SQL execution failed: cannot drop constraint\n"+
			"SQL: ALTER TABLE `test`.`users` DROP FOREIGN KEY `fk_users_account`",
	)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"ALTER TABLE `test`.`users` DROP FOREIGN KEY `fk_users_account`",
	})
}

func TestWriterDropAllTables_ReturnsQualifiedExecutionFailure(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{
		foreignKeyChecks: 1,
		dropErr:          errors.New("boom"),
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriterWithServerVersion(db.SQL, "test", platform.MySQL, "8.0.13")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches, "failed to drop table users: SQL execution failed: boom\nSQL: DROP TABLE IF EXISTS `test`.`users`")
}

type mysqlCleanupRecorder struct {
	mu                         sync.Mutex
	statementsSeen             []string
	foreignKeyChecks           int
	externalForeignKeys        int
	externalForeignKeyCounts   []int
	externalViews              int
	externalViewCounts         []int
	internalForeignKeys        [][]driver.Value
	objects                    [][]driver.Value
	catalogQuery               string
	catalogArgs                []driver.NamedValue
	catalogQueries             int
	viewCatalogQuery           string
	viewCatalogArgs            []driver.NamedValue
	cancelOnDrop               context.CancelFunc
	externalForeignKeyQueryErr error
	viewQueryErr               error
	storedProgramQueryErr      error
	missingGlobalPrivileges    bool
	missingShowRoutine         bool
	missingTrigger             bool
	partialPrivilegeRevokes    bool
	externalStoredPrograms     [][]driver.Value
	viewDropStarted            chan struct{}
	viewDropStartedOnce        sync.Once
	viewDropRelease            chan struct{}
	viewDropReleaseOnce        sync.Once
	viewDropErr                error
	otherMetadataLockWaiters   int
	enableErr                  error
	foreignKeyDropErr          error
	dropErr                    error
	restoreErr                 error
}

func (rec *mysqlCleanupRecorder) query(
	query string,
	args []driver.NamedValue,
) (dbtest.QueryResult, error) {
	switch {
	case strings.Contains(query, "information_schema.user_privileges"):
		hasSelect := int64(1)
		hasDrop := int64(1)
		hasAlter := int64(1)
		hasAlterRoutine := int64(1)
		hasEvent := int64(1)
		hasLockTables := int64(1)
		hasProcess := int64(1)
		hasShowView := int64(1)
		hasShowRoutine := int64(1)
		hasTrigger := int64(1)
		if rec.missingGlobalPrivileges {
			hasDrop = 0
			hasAlter = 0
			hasAlterRoutine = 0
			hasEvent = 0
			hasLockTables = 0
			hasProcess = 0
			hasShowView = 0
			hasShowRoutine = 0
			hasTrigger = 0
		}
		if rec.missingShowRoutine {
			hasShowRoutine = 0
		}
		if rec.missingTrigger {
			hasTrigger = 0
		}
		return dbtest.QueryResult{
			Columns: []string{
				"has_select",
				"has_drop",
				"has_alter",
				"has_alter_routine",
				"has_event",
				"has_lock_tables",
				"has_process",
				"has_show_view",
				"has_show_routine",
				"has_trigger",
			},
			Rows: [][]driver.Value{{
				hasSelect,
				hasDrop,
				hasAlter,
				hasAlterRoutine,
				hasEvent,
				hasLockTables,
				hasProcess,
				hasShowView,
				hasShowRoutine,
				hasTrigger,
			}},
		}, nil
	case strings.Contains(query, "SHOW GRANTS"):
		grant := "GRANT SELECT, LOCK TABLES, PROCESS, SHOW VIEW ON *.* TO `ptah`@`%`"
		if rec.partialPrivilegeRevokes {
			grant = "REVOKE SELECT ON `hidden`.* FROM `ptah`@`%`"
		}
		return dbtest.QueryResult{
			Columns: []string{"grants"},
			Rows:    [][]driver.Value{{grant}},
		}, nil
	case strings.Contains(query, "SELECT CONNECTION_ID()"):
		return dbtest.QueryResult{
			Columns: []string{"connection_id"},
			Rows:    [][]driver.Value{{int64(42)}},
		}, nil
	case strings.Contains(query, "information_schema.processlist"):
		rec.waitForViewDropStart()
		rec.releaseViewDropForCompetingWaiter()
		return dbtest.QueryResult{
			Columns: []string{"owned_waiters", "other_waiters"},
			Rows: [][]driver.Value{{
				int64(1),
				int64(rec.otherMetadataLockWaiters),
			}},
		}, nil
	case strings.Contains(query, "@@SESSION.FOREIGN_KEY_CHECKS"):
		return dbtest.QueryResult{
			Columns: []string{"foreign_key_checks"},
			Rows:    [][]driver.Value{{int64(rec.foreignKeyChecks)}},
		}, nil
	case strings.Contains(query, "internal_foreign_keys"):
		return dbtest.QueryResult{
			Columns: []string{"table_name", "constraint_name"},
			Rows:    rec.internalForeignKeys,
		}, nil
	case strings.Contains(query, "external_foreign_keys"):
		if rec.externalForeignKeyQueryErr != nil {
			return dbtest.QueryResult{}, rec.externalForeignKeyQueryErr
		}
		count := rec.nextExternalForeignKeyCount()
		return dbtest.QueryResult{
			Columns: []string{"count"},
			Rows:    [][]driver.Value{{int64(count)}},
		}, nil
	case strings.Contains(query, "information_schema.view_table_usage"):
		rec.recordViewCatalog(query, args)
		if rec.viewQueryErr != nil {
			return dbtest.QueryResult{}, rec.viewQueryErr
		}
		count := rec.nextExternalViewCount()
		return dbtest.QueryResult{
			Columns: []string{"count"},
			Rows:    [][]driver.Value{{int64(count)}},
		}, nil
	case strings.Contains(query, "information_schema.views"):
		rec.recordViewCatalog(query, args)
		if rec.viewQueryErr != nil {
			return dbtest.QueryResult{}, rec.viewQueryErr
		}
		count := rec.nextExternalViewCount()
		return dbtest.QueryResult{
			Columns: []string{"count"},
			Rows:    [][]driver.Value{{int64(count)}},
		}, nil
	case strings.Contains(query, "external_stored_programs"):
		if rec.storedProgramQueryErr != nil {
			return dbtest.QueryResult{}, rec.storedProgramQueryErr
		}
		return dbtest.QueryResult{
			Columns: []string{"object_schema", "object_name", "object_kind"},
			Rows:    rec.externalStoredPrograms,
		}, nil
	case strings.Contains(query, "cleanup_objects"):
		rec.mu.Lock()
		rec.catalogQuery = query
		rec.catalogArgs = append([]driver.NamedValue(nil), args...)
		rec.catalogQueries++
		rec.mu.Unlock()
		objects := rec.objects
		if objects == nil {
			objects = [][]driver.Value{{"users", "TABLE"}}
		}
		return dbtest.QueryResult{
			Columns: []string{"object_name", "object_kind"},
			Rows:    objects,
		}, nil
	default:
		return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
	}
}

func (rec *mysqlCleanupRecorder) exec(
	query string,
	_ []driver.NamedValue,
) (driver.Result, error) {
	statement := strings.Join(strings.Fields(query), " ")

	rec.mu.Lock()
	rec.statementsSeen = append(rec.statementsSeen, statement)
	cancelOnDrop := rec.cancelOnDrop
	enableErr := rec.enableErr
	foreignKeyDropErr := rec.foreignKeyDropErr
	dropErr := rec.dropErr
	viewDropErr := rec.viewDropErr
	restoreErr := rec.restoreErr
	rec.mu.Unlock()
	rec.recordViewDropStart(statement)

	switch {
	case statement == "SET FOREIGN_KEY_CHECKS = 1" && enableErr != nil:
		return nil, enableErr
	case strings.HasPrefix(statement, "DROP VIEW"):
		rec.waitForViewDropRelease()
		return driver.RowsAffected(0), viewDropErr
	case strings.HasPrefix(statement, "ALTER TABLE") && foreignKeyDropErr != nil:
		return nil, foreignKeyDropErr
	case strings.HasPrefix(statement, "DROP TABLE") && dropErr != nil:
		if cancelOnDrop != nil {
			cancelOnDrop()
		}
		return nil, dropErr
	case statement == "UNLOCK TABLES":
		rec.releaseViewDrop()
		return driver.RowsAffected(0), nil
	case statement == "SET FOREIGN_KEY_CHECKS = 0" && restoreErr != nil:
		return nil, restoreErr
	default:
		return driver.RowsAffected(0), nil
	}
}

func (rec *mysqlCleanupRecorder) recordViewDropStart(statement string) {
	if rec.viewDropStarted == nil || !strings.HasPrefix(statement, "DROP VIEW") {
		return
	}
	rec.viewDropStartedOnce.Do(func() {
		close(rec.viewDropStarted)
	})
}

func (rec *mysqlCleanupRecorder) waitForViewDropStart() {
	if rec.viewDropStarted != nil {
		<-rec.viewDropStarted
	}
}

func (rec *mysqlCleanupRecorder) waitForViewDropRelease() {
	if rec.viewDropRelease != nil {
		<-rec.viewDropRelease
	}
}

func (rec *mysqlCleanupRecorder) releaseViewDrop() {
	if rec.viewDropRelease != nil {
		rec.viewDropReleaseOnce.Do(func() {
			close(rec.viewDropRelease)
		})
	}
}

func (rec *mysqlCleanupRecorder) releaseViewDropForCompetingWaiter() {
	if rec.otherMetadataLockWaiters > 0 {
		rec.releaseViewDrop()
	}
}

func (rec *mysqlCleanupRecorder) nextExternalForeignKeyCount() int {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.externalForeignKeyCounts) == 0 {
		return rec.externalForeignKeys
	}
	count := rec.externalForeignKeyCounts[0]
	rec.externalForeignKeyCounts = rec.externalForeignKeyCounts[1:]
	return count
}

func (rec *mysqlCleanupRecorder) nextExternalViewCount() int {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.externalViewCounts) == 0 {
		return rec.externalViews
	}
	count := rec.externalViewCounts[0]
	rec.externalViewCounts = rec.externalViewCounts[1:]
	return count
}

func (rec *mysqlCleanupRecorder) statements() []string {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]string(nil), rec.statementsSeen...)
}

func (rec *mysqlCleanupRecorder) catalog() (string, []driver.NamedValue) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.catalogQuery, append([]driver.NamedValue(nil), rec.catalogArgs...)
}

func (rec *mysqlCleanupRecorder) catalogCount() int {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.catalogQueries
}

func (rec *mysqlCleanupRecorder) recordViewCatalog(query string, args []driver.NamedValue) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.viewCatalogQuery = query
	rec.viewCatalogArgs = append([]driver.NamedValue(nil), args...)
}

func (rec *mysqlCleanupRecorder) viewCatalog() (string, []driver.NamedValue) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.viewCatalogQuery, append([]driver.NamedValue(nil), rec.viewCatalogArgs...)
}
