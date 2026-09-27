package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

// mysqlFamilyColumnChecks declares table e the way a YAML file or a Go
// annotation does: columns a and b each carry a CHECK, neither named.
func mysqlFamilyColumnChecks() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "E", Name: "e"}},
		Fields: []schemamodel.Field{
			{StructName: "E", Name: "id", Type: "INT", Primary: true},
			{StructName: "E", Name: "a", Type: "INT", Nullable: true, Check: "a > 0"},
			{StructName: "E", Name: "b", Type: "INT", Nullable: true, Check: "b > 0"},
		},
	}
}

// mysqlFamilyCatalog is table e as the engine reports it after running the
// CREATE TABLE the renderer writes for [mysqlFamilyColumnChecks], with the
// CHECK names given.
func mysqlFamilyCatalog(first, second string) *catalog.Database {
	check := func(name, clause string) catalog.Constraint {
		return catalog.Constraint{Name: name, TableName: "e", Type: "CHECK", CheckClause: new(clause)}
	}
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "e", Columns: []catalog.Column{
			{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
			{Name: "a", DataType: "int", IsNullable: "YES"},
			{Name: "b", DataType: "int", IsNullable: "YES"},
		}}},
		Constraints: []catalog.Constraint{
			{Name: "PRIMARY", TableName: "e", Type: "PRIMARY KEY", ColumnNames: []string{"id"}},
			check(first, "(`a` > 0)"),
			check(second, "(`b` > 0)"),
		},
	}
}

// TestCompare_AnUnnamedColumnCheckTakesTheMySQLFamilyName compares the table
// with the database the rendered table built on each engine: nothing is
// planned. Measured, MySQL 8.4.11 names the two CHECKs `e_chk_1` and `e_chk_2`
// and MariaDB 11.8.9 names them after their columns. Looked for as
// `e_a_check` and `e_b_check`, every apply after the first would drop both and
// add them back (stokaro/ptah#3792).
func TestCompare_AnUnnamedColumnCheckTakesTheMySQLFamilyName(t *testing.T) {
	tests := []struct {
		dialect       string
		first, second string
	}{
		{dialect: platform.MySQL, first: "e_chk_1", second: "e_chk_2"},
		{dialect: platform.MariaDB, first: "a", second: "b"},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(
				mysqlFamilyColumnChecks(), mysqlFamilyCatalog(test.first, test.second), test.dialect)

			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
		})
	}
}

// TestCompare_AnUnnamedColumnCheckThatChangedIsPlannedOnMySQL is the control
// for the test above: a column CHECK whose condition changed is still planned,
// under the name the server gave it.
func TestCompare_AnUnnamedColumnCheckThatChangedIsPlannedOnMySQL(t *testing.T) {
	c := qt.New(t)
	desired := mysqlFamilyColumnChecks()
	desired.Fields[2].Check = "b > 1"

	diff := schemadiff.CompareWithDialect(desired, mysqlFamilyCatalog("e_chk_1", "e_chk_2"), platform.MySQL)

	c.Assert(diff.ConstraintsAdded, qt.HasLen, 1)
	c.Assert(diff.ConstraintsAdded[0].Name, qt.Equals, "e_chk_2")
	c.Assert(diff.ConstraintsAdded[0].CheckExpression, qt.Equals, "b > 1")
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 1)
	c.Assert(diff.ConstraintsRemoved[0].Name, qt.Equals, "e_chk_2")
}
