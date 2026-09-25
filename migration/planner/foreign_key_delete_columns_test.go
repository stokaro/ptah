package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// deleteListAdditionDiff is a hand-assembled diff that adds a key whose ON
// DELETE SET NULL is limited to parent_id.
func deleteListAdditionDiff() *difftypes.SchemaDiff {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Child", Name: "children"}},
		Constraints: []schemamodel.Constraint{{
			StructName: "Child", Table: "children", Name: "children_parent_fk", Type: "FOREIGN KEY",
			Columns: []string{"tenant", "parent_id"}, ForeignTable: "parents",
			ForeignColumns: []string{"tenant", "id"}, OnDelete: "SET NULL", OnDeleteColumns: []string{"parent_id"},
		}},
	}
	return &difftypes.SchemaDiff{ConstraintsAdded: difftypes.ConstraintAdditionsFor(desired, "children_parent_fk")}
}

// The list reaches the statement a plan adds the key with.
func TestGenerateSchemaDiffSQLStatements_AddsTheDeleteColumnList(t *testing.T) {
	c := qt.New(t)

	statements, err := planner.GenerateSchemaDiffSQLStatements(deleteListAdditionDiff(), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 1)
	c.Assert(statements[0], qt.Contains, `ON DELETE SET NULL ("parent_id")`)
}

// A planner for a target without the clause hands the list to the renderer,
// which refuses it, rather than dropping it and adding a key that clears
// every column.
func TestGenerateSchemaDiffSQLStatements_RefusesTheDeleteColumnListOnMySQL(t *testing.T) {
	c := qt.New(t)

	statements, err := planner.GenerateSchemaDiffSQLStatements(deleteListAdditionDiff(), "mysql")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*mysql does not support a column list on ON DELETE SET NULL.*`)
	c.Assert(statements, qt.IsNil)
}
