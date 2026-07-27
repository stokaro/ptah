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
	writer := mysql.NewMySQLWriter(db.SQL, "test")

	err := writer.DropAllTables(ctx)

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"SET FOREIGN_KEY_CHECKS = 0",
		"DROP TABLE IF EXISTS `users`",
		"SET FOREIGN_KEY_CHECKS = 1",
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
	writer := mysql.NewMySQLWriter(db.SQL, "test")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorIs, dropErr)
	c.Assert(err, qt.ErrorIs, restoreErr)
	c.Assert(recorder.statements(), qt.DeepEquals, []string{
		"SET FOREIGN_KEY_CHECKS = 0",
		"DROP TABLE IF EXISTS `users`",
		"SET FOREIGN_KEY_CHECKS = 1",
	})
}

type mysqlCleanupRecorder struct {
	mu             sync.Mutex
	statementsSeen []string
	cancelOnDrop   context.CancelFunc
	dropErr        error
	restoreErr     error
}

func (rec *mysqlCleanupRecorder) query(
	query string,
	_ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	if !strings.Contains(query, "information_schema.tables") {
		return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
	}
	return dbtest.QueryResult{
		Columns: []string{"table_name"},
		Rows:    [][]driver.Value{{"users"}},
	}, nil
}

func (rec *mysqlCleanupRecorder) exec(
	query string,
	_ []driver.NamedValue,
) (driver.Result, error) {
	statement := strings.Join(strings.Fields(query), " ")

	rec.mu.Lock()
	rec.statementsSeen = append(rec.statementsSeen, statement)
	cancelOnDrop := rec.cancelOnDrop
	dropErr := rec.dropErr
	restoreErr := rec.restoreErr
	rec.mu.Unlock()

	switch {
	case strings.HasPrefix(statement, "DROP TABLE") && dropErr != nil:
		if cancelOnDrop != nil {
			cancelOnDrop()
		}
		return nil, dropErr
	case statement == "SET FOREIGN_KEY_CHECKS = 1" && restoreErr != nil:
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
