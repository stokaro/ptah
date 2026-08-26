package mssql

// White-box testing required: the column read is package-local and the identity
// range it carries is not reachable through an exported API.

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// columnsOf reads the fake catalog and returns one table's columns by name.
func columnsOf(c *qt.C, answer dbtest.QueryHandler) map[string]struct{ start, increment string } {
	c.Helper()
	db := dbtest.Open(c.TB, answer)
	reader := NewSQLServerReader(db.SQL, "dbo")

	columns, err := reader.readColumnsByTable(c.Context())
	c.Assert(err, qt.IsNil)

	byName := make(map[string]struct{ start, increment string })
	for _, column := range columns[catalogTableKey{schema: "dbo", table: "orders"}] {
		byName[column.Name] = struct{ start, increment string }{
			start: column.IdentityStart, increment: column.IdentityIncrement,
		}
	}
	return byName
}

// TestReadColumnsByTable_CarriesTheIdentityRange pins that the seed and the
// increment reach the column.
//
// The projection has always asked for IDENT_SEED and IDENT_INCR and the scan
// has always filled two variables with them; nothing then read either one, so
// the only identity fact that survived was "this column auto-increments". A
// column created IDENTITY(1000,5) was described with no range at all and
// replayed as IDENTITY(1,1): measured on SQL Server 2025, the first row of the
// source got id 1000 and the first row of the replay got id 1
// (stokaro/ptah#2196).
func TestReadColumnsByTable_CarriesTheIdentityRange(t *testing.T) {
	c := qt.New(t)

	byName := columnsOf(c, answeringIdentityRange)

	c.Assert(byName["id"].start, qt.Equals, "1000")
	c.Assert(byName["id"].increment, qt.Equals, "5")
}

// TestReadColumnsByTable_LeavesANonIdentityColumnWithoutARange is the control,
// and it is the reason the assignment is guarded.
//
// IDENT_SEED and IDENT_INCR answer for the TABLE, so every row of the read
// carries the same pair -- including the rows for columns that are not the
// identity one. Copying them unguarded would describe `note` as an identity
// column's range, which no engine would accept back.
func TestReadColumnsByTable_LeavesANonIdentityColumnWithoutARange(t *testing.T) {
	c := qt.New(t)

	byName := columnsOf(c, answeringIdentityRange)

	c.Assert(byName["note"].start, qt.Equals, "")
	c.Assert(byName["note"].increment, qt.Equals, "")
}

// answeringIdentityRange is one table with one IDENTITY(1000,5) column beside a
// plain one, with the table-scoped seed repeated on both rows the way the
// server repeats it.
func answeringIdentityRange(_ string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	return dbtest.QueryResult{
		Columns: []string{
			"schema_name", "table_name", "column_name", "type_name",
			"max_length", "precision", "scale", "is_nullable",
			"is_identity", "ident_seed", "ident_incr", "column_id",
			"default_definition", "generated_expression", "is_persisted", "comment",
		},
		Rows: [][]driver.Value{
			{"dbo", "orders", "id", "int",
				int64(4), int64(10), int64(0), false,
				int64(1), "1000", "5", int64(1), nil, nil, nil, nil},
			{"dbo", "orders", "note", "nvarchar",
				int64(80), int64(0), int64(0), true,
				int64(0), "1000", "5", int64(2), nil, nil, nil, nil},
		},
	}, nil
}
