package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/migration/schemadiff"
)

// unvalidatedKeys is what PostgreSQL 18.6 reads back for a table that added a
// CHECK and a single-column foreign key NOT VALID.
func unvalidatedKeys(notValid bool) *catalog.Database {
	parent, column, action, clause := "p", "id", "NO ACTION", "(n > 0)"
	return &catalog.Database{
		Tables: []catalog.Table{
			{Name: "p", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "c", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "n", DataType: "integer", IsNullable: "YES"},
				{Name: "p_id", DataType: "integer", IsNullable: "YES"},
			}},
		},
		Constraints: []catalog.Constraint{
			{TableName: "p", Name: "p_pkey", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}},
			{TableName: "c", Name: "c_n", Type: "CHECK", CheckClause: &clause, NotValid: notValid},
			{
				TableName: "c", Name: "c_p", Type: "FOREIGN KEY", ColumnName: "p_id", ColumnNames: []string{"p_id"},
				ForeignTable: &parent, ForeignColumn: &column, ForeignColumns: []string{"id"},
				DeleteRule: &action, UpdateRule: &action, NotValid: notValid,
			},
		},
	}
}

// TestConvert_KeepsNotValid describes each constraint the server has not
// validated as a constraint of the table that says so. A column's reference
// has no room for NOT VALID, so the foreign key is not carried on its column
// (stokaro/ptah#3853).
func TestConvert_KeepsNotValid(t *testing.T) {
	c := qt.New(t)

	database := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), unvalidatedKeys(true), platform.Postgres, must.Must(builtin.New())))

	c.Assert(fieldNamed(c, database, "p_id").Foreign, qt.Equals, "")
	c.Assert(database.Constraints, qt.HasLen, 2)
	c.Assert(database.Constraints[0].Name, qt.Equals, "c_n")
	c.Assert(database.Constraints[0].NotValid, qt.IsTrue)
	c.Assert(database.Constraints[1].Name, qt.Equals, "c_p")
	c.Assert(database.Constraints[1].NotValid, qt.IsTrue)
	c.Assert(database.Constraints[1].ForeignTable, qt.Equals, "p")
}

// TestConvert_LeavesAValidatedForeignKeyToItsColumn is the control.
func TestConvert_LeavesAValidatedForeignKeyToItsColumn(t *testing.T) {
	c := qt.New(t)

	database := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), unvalidatedKeys(false), platform.Postgres, must.Must(builtin.New())))

	c.Assert(fieldNamed(c, database, "p_id").ForeignKeyName, qt.Equals, "c_p")
	c.Assert(database.Constraints, qt.HasLen, 1)
	c.Assert(database.Constraints[0].NotValid, qt.IsFalse)
}

// TestCompare_ADatabaseWithUnvalidatedConstraintsIsSynced compares the
// description of a database with the database itself, the comparison `schema
// diff` runs between two databases: nothing is validated.
func TestCompare_ADatabaseWithUnvalidatedConstraintsIsSynced(t *testing.T) {
	c := qt.New(t)
	live := unvalidatedKeys(true)

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), live, platform.Postgres, must.Must(builtin.New()))), live, platform.Postgres, must.Must(builtin.New())))

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("%+v", diff))
}
