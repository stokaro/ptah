package dbschema_test

import (
	"cmp"
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
)

// newRegionsConn opens an in-memory SQLite database, creates a "regions" table
// with three columns, and seeds it with two rows. The connection is closed via
// t.Cleanup. It is the shared fixture for the ReadTableRows tests.
func newRegionsConn(t *testing.T) *dbschema.DatabaseConnection {
	t.Helper()
	c := qt.New(t)

	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite:///:memory:")
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	_, err = conn.ExecContext(context.Background(),
		"CREATE TABLE regions (code TEXT PRIMARY KEY, name TEXT NOT NULL, population INTEGER NOT NULL)")
	c.Assert(err, qt.IsNil)

	_, err = conn.ExecContext(context.Background(),
		"INSERT INTO regions (code, name, population) VALUES ('US', 'United States', 331), ('CZ', 'Czechia', 10)")
	c.Assert(err, qt.IsNil)

	return conn
}

// sortByCode orders rows by their "code" column so an unordered read result can
// be compared against a fixed expectation (ReadTableRows does not guarantee
// order).
func sortByCode(rows []map[string]any) {
	slices.SortFunc(rows, func(a, b map[string]any) int {
		return cmp.Compare(a["code"].(string), b["code"].(string))
	})
}

func TestReadTableRows_ColumnSubset(t *testing.T) {
	c := qt.New(t)
	conn := newRegionsConn(t)

	rows, err := dbschema.ReadTableRows(context.Background(), conn, "", "regions", []string{"code", "name"})
	c.Assert(err, qt.IsNil)
	sortByCode(rows)
	// Only the requested columns are present; the live-only "population" column
	// is not projected into the returned maps.
	c.Assert(rows, qt.DeepEquals, []map[string]any{
		{"code": "CZ", "name": "Czechia"},
		{"code": "US", "name": "United States"},
	})
}

func TestReadTableRows_SchemaQualified(t *testing.T) {
	c := qt.New(t)
	conn := newRegionsConn(t)

	// SQLite's default schema is "main"; qualifying with it must resolve the
	// same table and exercises the schema-qualified identifier path.
	rows, err := dbschema.ReadTableRows(context.Background(), conn, "main", "regions", []string{"code"})
	c.Assert(err, qt.IsNil)
	sortByCode(rows)
	c.Assert(rows, qt.DeepEquals, []map[string]any{
		{"code": "CZ"},
		{"code": "US"},
	})
}

func TestReadTableRows_IntegerColumnScannedAsInt64(t *testing.T) {
	c := qt.New(t)
	conn := newRegionsConn(t)

	rows, err := dbschema.ReadTableRows(context.Background(), conn, "", "regions", []string{"code", "population"})
	c.Assert(err, qt.IsNil)
	sortByCode(rows)
	c.Assert(rows, qt.DeepEquals, []map[string]any{
		{"code": "CZ", "population": int64(10)},
		{"code": "US", "population": int64(331)},
	})
}

func TestReadTableRows_EmptyTable(t *testing.T) {
	c := qt.New(t)
	conn := newRegionsConn(t)

	_, err := conn.ExecContext(context.Background(), "DELETE FROM regions")
	c.Assert(err, qt.IsNil)

	rows, err := dbschema.ReadTableRows(context.Background(), conn, "", "regions", []string{"code", "name"})
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 0)
}

func TestReadTableRows_ValidationErrors(t *testing.T) {
	conn := newRegionsConn(t)

	tests := []struct {
		name    string
		conn    *dbschema.DatabaseConnection
		table   string
		columns []string
	}{
		{name: "nil connection", conn: nil, table: "regions", columns: []string{"code"}},
		{name: "empty table name", conn: conn, table: "  ", columns: []string{"code"}},
		{name: "no columns", conn: conn, table: "regions", columns: nil},
		{name: "duplicate columns", conn: conn, table: "regions", columns: []string{"code", "code"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			rows, err := dbschema.ReadTableRows(context.Background(), tt.conn, "", tt.table, tt.columns)
			c.Assert(err, qt.IsNotNil)
			c.Assert(rows, qt.IsNil)
		})
	}
}
