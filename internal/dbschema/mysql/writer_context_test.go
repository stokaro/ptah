package mysql_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/dbschema/dbtest"
	"github.com/stokaro/ptah/internal/dbschema/mysql"
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
	writer := mysql.NewMySQLWriter(db.SQL, "test", platform.MySQL)

	err := writer.DropAllTables(ctx)

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"SET FOREIGN_KEY_CHECKS = 1",
		"DROP TABLE IF EXISTS `test`.`users`",
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
	writer := mysql.NewMySQLWriter(db.SQL, "test", platform.MySQL)

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorIs, dropErr)
	c.Assert(err, qt.ErrorIs, restoreErr)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"SET FOREIGN_KEY_CHECKS = 1",
		"DROP TABLE IF EXISTS `test`.`users`",
		"SET FOREIGN_KEY_CHECKS = 0",
	})
}

func TestWriterDropAllTables_TemporarilyEnablesForeignKeyChecks(t *testing.T) {
	c := qt.New(t)
	recorder := &mysqlCleanupRecorder{}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriter(db.SQL, "test", platform.MySQL)

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"SET FOREIGN_KEY_CHECKS = 1",
		"DROP TABLE IF EXISTS `test`.`users`",
		"SET FOREIGN_KEY_CHECKS = 0",
	})
}

func TestWriterDropAllTables_RestoresAfterEnableFailure(t *testing.T) {
	c := qt.New(t)
	enableErr := errors.New("enable outcome unknown")
	recorder := &mysqlCleanupRecorder{
		enableErr: enableErr,
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriter(db.SQL, "test", platform.MySQL)

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
	writer := mysql.NewMySQLWriter(db.SQL, "test", platform.MySQL)

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
	writer := mysql.NewMySQLWriter(db.SQL, "test", platform.MySQL)

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
	writer := mysql.NewMySQLWriter(db.SQL, "test", platform.MariaDB)

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
			{"active_users", "VIEW"},
			{"normalize_email", "FUNCTION"},
			{"refresh_users", "PROCEDURE"},
			{"users", "TABLE"},
			{"order_numbers", "SEQUENCE"},
		},
	}
	db := dbtest.OpenWithExec(t, recorder.query, recorder.exec)
	writer := mysql.NewMySQLWriter(db.SQL, "test", platform.MySQL)

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"ALTER TABLE `test`.`child``records` DROP FOREIGN KEY `fk``parent`",
		"ALTER TABLE `test`.`users` DROP FOREIGN KEY `fk_users_account`",
		"DROP EVENT IF EXISTS `test`.`nightly_cleanup`",
		"DROP VIEW IF EXISTS `test`.`active_users`",
		"DROP FUNCTION IF EXISTS `test`.`normalize_email`",
		"DROP PROCEDURE IF EXISTS `test`.`refresh_users`",
		"DROP TABLE IF EXISTS `test`.`users`",
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
	writer := mysql.NewMySQLWriter(db.SQL, "test", platform.MySQL)

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
	writer := mysql.NewMySQLWriter(db.SQL, "test", platform.MySQL)

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
	writer := mysql.NewMySQLWriter(db.SQL, "test", platform.MySQL)

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches, "failed to drop table users: SQL execution failed: boom\nSQL: DROP TABLE IF EXISTS `test`.`users`")
}

type mysqlCleanupRecorder struct {
	mu                         sync.Mutex
	statementsSeen             []string
	foreignKeyChecks           int
	externalForeignKeys        int
	externalViews              int
	internalForeignKeys        [][]driver.Value
	objects                    [][]driver.Value
	catalogQuery               string
	catalogArgs                []driver.NamedValue
	viewCatalogQuery           string
	viewCatalogArgs            []driver.NamedValue
	cancelOnDrop               context.CancelFunc
	externalForeignKeyQueryErr error
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
		return dbtest.QueryResult{
			Columns: []string{"count"},
			Rows:    [][]driver.Value{{int64(rec.externalForeignKeys)}},
		}, nil
	case strings.Contains(query, "information_schema.view_table_usage"):
		rec.recordViewCatalog(query, args)
		return dbtest.QueryResult{
			Columns: []string{"count"},
			Rows:    [][]driver.Value{{int64(rec.externalViews)}},
		}, nil
	case strings.Contains(query, "information_schema.views"):
		rec.recordViewCatalog(query, args)
		return dbtest.QueryResult{
			Columns: []string{"count"},
			Rows:    [][]driver.Value{{int64(rec.externalViews)}},
		}, nil
	case strings.Contains(query, "cleanup_objects"):
		rec.mu.Lock()
		rec.catalogQuery = query
		rec.catalogArgs = append([]driver.NamedValue(nil), args...)
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
	restoreErr := rec.restoreErr
	rec.mu.Unlock()

	switch {
	case statement == "SET FOREIGN_KEY_CHECKS = 1" && enableErr != nil:
		return nil, enableErr
	case strings.HasPrefix(statement, "ALTER TABLE") && foreignKeyDropErr != nil:
		return nil, foreignKeyDropErr
	case strings.HasPrefix(statement, "DROP TABLE") && dropErr != nil:
		if cancelOnDrop != nil {
			cancelOnDrop()
		}
		return nil, dropErr
	case statement == "SET FOREIGN_KEY_CHECKS = 0" && restoreErr != nil:
		return nil, restoreErr
	default:
		return driver.RowsAffected(0), nil
	}
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
