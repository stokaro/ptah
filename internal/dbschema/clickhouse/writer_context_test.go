package clickhouse_test

import (
	"context"
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbschema/clickhouse"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

func TestWriterDropAllTables_CanceledContext(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, emptyClickHouseCleanupQuery)
	writer := clickhouse.NewClickHouseWriter(db.SQL, "default")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := writer.DropAllTables(ctx)

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(db.QueryCount(), qt.Equals, 0)
}

func emptyClickHouseCleanupQuery(
	_ string,
	_ []driver.NamedValue,
) (dbtest.QueryResult, error) {
	return dbtest.QueryResult{}, nil
}
