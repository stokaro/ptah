package schemastate_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemastate"
)

// checkedWidget is a table whose column carries a CHECK, named or not.
func checkedWidget(checkName string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Widget", Name: "widget"}},
		Fields: []schemamodel.Field{
			{StructName: "Widget", Name: "id", Type: "int", Primary: true},
			{
				StructName: "Widget", Name: "b", Type: "int",
				Check: "b > 0", CheckName: checkName,
			},
		},
	}
}

// checkConstraintNames lists the CHECK constraints a state carries.
func checkConstraintNames(c *qt.C, database *schemamodel.Database) []string {
	c.Helper()

	state, err := schemastate.FromDescription(database, platform.Postgres, identifier.ForDialect(platform.Postgres))
	c.Assert(err, qt.IsNil)

	var names []string
	for _, object := range state.Objects() {
		if object.Constraint != nil && object.Constraint.Kind == "CHECK" {
			names = append(names, object.Constraint.ConstraintName)
		}
	}
	return names
}

// TestFromDescription_ANamedColumnCheckIsTheTableConstraintItIs pins the
// direction the two adapters can agree in.
//
// A catalog does not record where a check was written: measured on PostgreSQL
// 18.6, an inline `CHECK (a > 0)`, a named inline one, and an `ALTER TABLE ADD
// CONSTRAINT` are one shape in pg_constraint. So a declared column-level check
// has to become the table constraint the other side already reports, and the
// name is what both sides have (stokaro/ptah#1663).
func TestFromDescription_ANamedColumnCheckIsTheTableConstraintItIs(t *testing.T) {
	c := qt.New(t)

	c.Assert(checkConstraintNames(c, checkedWidget("b_positive")),
		qt.DeepEquals, []string{"b_positive"})
}

// TestFromDescription_AnUnnamedColumnCheckIsNotSynthesized is the control, and
// it is the case still open on #1663.
//
// The server invents the name -- `widget_b_check` on PostgreSQL,
// `widget_chk_1` on MySQL -- so a synthesized constraint carrying no name, or
// one named by a single server's convention, would differ from the catalog on
// every run. Carrying it on the column and leaving it out of the comparison is
// the honest answer until identity for an unnamed check is decided.
func TestFromDescription_AnUnnamedColumnCheckIsNotSynthesized(t *testing.T) {
	c := qt.New(t)

	c.Assert(checkConstraintNames(c, checkedWidget("")), qt.HasLen, 0)
}
