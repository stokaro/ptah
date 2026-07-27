package mssql_test

import (
	"context"
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/dbschema/dbtest"
	"github.com/stokaro/ptah/internal/dbschema/mssql"
)

func TestWriterDropAllTables_CanceledContext(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, emptySQLServerCleanupQuery)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := writer.DropAllTables(ctx)

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(db.QueryCount(), qt.Equals, 0)
	c.Assert(db.BeginCount(), qt.Equals, 0)
}

func emptySQLServerCleanupQuery(
	_ string,
	_ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	return dbtest.QueryResult{}, nil
}
