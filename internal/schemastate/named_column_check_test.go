package schemastate_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/schemastate"
)

// checkedWidget is a table whose column carries a CHECK, named or not.
func checkedWidget(checkName string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Widget", Name: "widget"}},
		Fields: []goschema.Field{
			{StructName: "Widget", Name: "id", Type: "int", Primary: true},
			{
				StructName: "Widget", Name: "b", Type: "int",
				Check: "b > 0", CheckName: checkName,
			},
		},
	}
}

// checkConstraintNames lists the CHECK constraints a state carries.
func checkConstraintNames(c *qt.C, database *goschema.Database) []string {
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

// TestFromDescription_AnUnnamedColumnCheckIsAlsoTheTableConstraint is what the
// identity decision made expressible.
//
// The server names an unnamed check itself, and differently per engine, so a
// synthesized constraint keyed by name would differ from the catalog on every
// run. Keyed by its CONDITION it matches, whatever either side calls it
// (stokaro/ptah#1663).
func TestFromDescription_AnUnnamedColumnCheckIsAlsoTheTableConstraint(t *testing.T) {
	c := qt.New(t)

	c.Assert(checkConstraintNames(c, checkedWidget("")), qt.HasLen, 1)
}

// TestFromDescription_ACheckIsIdentifiedByItsCondition pins the identity rule
// itself: two spellings of one guarantee are one object, and the name each side
// gives it does not split them.
func TestFromDescription_ACheckIsIdentifiedByItsCondition(t *testing.T) {
	c := qt.New(t)

	named := checkIdentityKeys(c, checkedWidget("b_positive"))
	unnamed := checkIdentityKeys(c, checkedWidget(""))

	c.Assert(named, qt.HasLen, 1)
	c.Assert(named, qt.DeepEquals, unnamed)
}

// TestFromDescription_ADifferentConditionIsADifferentCheck is the control.
//
// Identity by condition must still tell two conditions apart, or a changed
// CHECK would read as the one it replaced and nothing would be planned.
func TestFromDescription_ADifferentConditionIsADifferentCheck(t *testing.T) {
	c := qt.New(t)

	first := checkIdentityKeys(c, checkedWidget("b_positive"))

	other := checkedWidget("b_positive")
	other.Fields[1].Check = "b > 10"
	second := checkIdentityKeys(c, other)

	c.Assert(first, qt.Not(qt.DeepEquals), second)
}

// checkIdentityKeys lists the comparison keys the CHECK constraints carry.
func checkIdentityKeys(c *qt.C, database *goschema.Database) []string {
	c.Helper()

	state, err := schemastate.FromDescription(database, platform.Postgres, identifier.ForDialect(platform.Postgres))
	c.Assert(err, qt.IsNil)

	var keys []string
	for _, object := range state.Objects() {
		if object.Constraint != nil && object.Constraint.Kind == "CHECK" {
			keys = append(keys, object.ID.Name.Normalized)
		}
	}
	return keys
}
