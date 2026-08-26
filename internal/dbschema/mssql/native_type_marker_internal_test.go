package mssql

// White-box testing required: the column read is package-local and the marker
// it sets is not reachable through an exported API.

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// A type read from SQL Server's catalog is marked as the target's own --
// stokaro/ptah#2147.
//
// The portable mapping turns a declared VARCHAR into NVARCHAR, because a schema
// written for several engines means Unicode by it. Applied to a type SQL Server
// itself reported, that is a different column: varchar is one byte per
// character and nvarchar is two. Without the marker a description of this
// database replayed as Unicode -- measured on SQL Server 2025, `varchar/50`
// came back `nvarchar/100` and `char/8` came back `nchar/16`.
func TestReadColumnsByTable_MarksTheTypeAsTheTargetsOwn(t *testing.T) {
	c := qt.New(t)

	db := dbtest.Open(t, answeringNativeTypes)
	reader := NewSQLServerReader(db.SQL, "dbo")

	columns, err := reader.readColumnsByTable(t.Context())

	c.Assert(err, qt.IsNil)
	byTable := columns[catalogTableKey{schema: "dbo", table: "t"}]
	c.Assert(byTable, qt.HasLen, 2)
	for _, column := range byTable {
		c.Assert(column.TypeIsDeclaredText, qt.IsTrue,
			qt.Commentf("column %q", column.Name))
	}
}

func answeringNativeTypes(_ string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	return dbtest.QueryResult{
		Columns: []string{
			"schema_name", "table_name", "column_name", "type_name",
			"max_length", "precision", "scale", "is_nullable",
			"is_identity", "ident_seed", "ident_incr", "column_id",
			"default_definition", "generated_expression", "is_persisted", "comment",
		},
		Rows: [][]driver.Value{
			{"dbo", "t", "id", "int",
				int64(4), int64(10), int64(0), false,
				int64(1), int64(1), int64(1), int64(1), nil, nil, nil, nil},
			{"dbo", "t", "a", "varchar",
				int64(50), int64(0), int64(0), false,
				int64(0), nil, nil, int64(2), nil, nil, nil, nil},
		},
	}, nil
}
