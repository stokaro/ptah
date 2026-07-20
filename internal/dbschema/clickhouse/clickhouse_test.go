package clickhouse

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/dbschema/dbtest"
)

// TestClickHouseWriter_SchemaWriterInterface is a compile-time guard that
// *Writer continues to satisfy types.SchemaWriter as that interface evolves
// (e.g., the parameterized ExecuteSQL signature added in #130/#177). If the
// interface drifts, this fails to compile rather than at integration-test
// time. Mirrors the postgres / mysql convention.
func TestClickHouseWriter_SchemaWriterInterface(t *testing.T) {
	writer := NewClickHouseWriter(nil, "default")
	var _ types.SchemaWriter = writer
}

func TestQuoteIdent(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "simple identifier", in: "users", want: "`users`"},
		{name: "empty", in: "", want: "``"},
		{name: "mixed case preserved", in: "MyTable", want: "`MyTable`"},
		{name: "embedded backtick doubled", in: "weird`name", want: "`weird``name`"},
		{name: "multiple embedded backticks", in: "a`b`c", want: "`a``b``c`"},
		{name: "name with space and semicolon", in: "t; DROP TABLE x; --", want: "`t; DROP TABLE x; --`"},
		{name: "injection attempt via backtick", in: "t` SYNC; DROP TABLE y; --", want: "`t`` SYNC; DROP TABLE y; --`"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(quoteIdent(tc.in), qt.Equals, tc.want)
		})
	}
}

// TestWriter_DryRun_NoDBRequired verifies that the dry-run path of
// ExecuteSQL works without an underlying *sql.DB. This locks in the
// observable contract that ExecuteSQL never touches the DB when dry-run
// is enabled — important because the migrator relies on this to safely
// preview migrations even when no live ClickHouse is reachable.
func TestWriter_DryRun_NoDBRequired(t *testing.T) {
	c := qt.New(t)
	w := NewClickHouseWriter(nil, "default")
	w.SetDryRun(true)
	c.Assert(w.IsDryRun(), qt.IsTrue)

	// ExecuteSQL must not touch w.db when dry-run is on; passing args
	// also verifies the new (ctx, sql, args...) signature compiles end to
	// end.
	err := w.ExecuteSQL(t.Context(), "SELECT ?", 42)
	c.Assert(err, qt.IsNil)

	// Begin/Commit/Rollback are documented no-ops on ClickHouse; the dry-run
	// path simply logs.
	tx, err := w.BeginTransaction(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(tx.Commit(), qt.IsNil)
	c.Assert(tx.Rollback(), qt.IsNil)

	// DropAllTables dry-run prints a stub table list and emits no DDL,
	// so it must succeed without a live DB.
	c.Assert(w.DropAllTables(), qt.IsNil)
}

func TestClickHouseWriterConcurrentTransactionNoops(t *testing.T) {
	c := qt.New(t)
	w := NewClickHouseWriter(nil, "default")
	w.SetDryRun(true)
	ctx := t.Context()

	const goroutines = 64
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	start := make(chan struct{})

	for i := range goroutines {
		wg.Go(func() {
			<-start
			tx, err := w.BeginTransaction(ctx)
			if err != nil {
				errs <- err
				return
			}
			if i%2 == 0 {
				errs <- tx.Commit()
				return
			}
			errs <- tx.Rollback()
		})
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		c.Assert(err, qt.IsNil)
	}
}

func TestReaderReadTablesUsesBulkColumnQuery(t *testing.T) {
	c := qt.New(t)

	tableRows := make([][]driver.Value, 0, 50)
	columnRows := make([][]driver.Value, 0, 100)
	for i := range 50 {
		tableName := fmt.Sprintf("table_%02d", i)
		tableRows = append(tableRows, []driver.Value{tableName, ""})
		columnRows = append(columnRows,
			[]driver.Value{tableName, "id", "UInt64", "", "", uint64(1), ""},
			[]driver.Value{tableName, "payload", "Nullable(String)", "", "", uint64(2), ""},
		)
	}
	columnRows[3][3] = "DEFAULT"
	columnRows[3][4] = "0"

	db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.Contains(query, "FROM system.columns"):
			return dbtest.QueryResult{
				Columns: []string{"table", "name", "type", "default_kind", "default_expression", "position", "comment"},
				Rows:    columnRows,
			}, nil
		case strings.Contains(query, "FROM system.tables"):
			return dbtest.QueryResult{
				Columns: []string{"name", "comment"},
				Rows:    tableRows,
			}, nil
		default:
			return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
		}
	})
	reader := NewClickHouseReader(db.SQL, "default")

	tables, err := reader.readTables("default")

	c.Assert(err, qt.IsNil)
	c.Assert(db.QueryCount(), qt.Equals, 2)
	c.Assert(tables, qt.HasLen, 50)
	c.Assert(tables[0].Name, qt.Equals, "table_00")
	c.Assert(tables[0].Columns, qt.HasLen, 2)
	c.Assert(tables[0].Columns[1].IsNullable, qt.Equals, "YES")
	c.Assert(tables[1].Columns[1].ColumnDefault, qt.IsNotNil)
	c.Assert(*tables[1].Columns[1].ColumnDefault, qt.Equals, "0")
}
