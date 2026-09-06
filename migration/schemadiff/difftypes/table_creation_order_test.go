package difftypes_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// unfinalizedThreeEdgeSchema declares one parent per kind of edge a declaration
// can express, and one child reaching each of them by a different kind.
//
// It is deliberately NOT put through [schemamodel.Finalize]: the dependency map
// that finalization derives is empty here, which is the state an embedder's
// hand-assembled declaration is in. Every edge below therefore has to be derived
// while the creation is assembled, or it is not carried at all.
//
// The tables are declared child-first so that leaving the input order alone is a
// failing answer rather than an accidentally passing one.
func unfinalizedThreeEdgeSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Child", Name: "wf2315_children"},
			{StructName: "FieldParent", Name: "wf2315_field_parents"},
			{StructName: "EmbeddedParent", Name: "wf2315_embedded_parents"},
			{StructName: "ConstraintParent", Name: "wf2315_constraint_parents"},
		},
		Fields: []schemamodel.Field{
			{StructName: "FieldParent", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "EmbeddedParent", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "ConstraintParent", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Child", Name: "id", Type: "BIGINT", Primary: true},
			{
				StructName:     "Child",
				Name:           "field_parent_id",
				Type:           "BIGINT",
				Foreign:        "wf2315_field_parents(id)",
				ForeignKeyName: "fk_wf2315_children_field_parent",
			},
			{StructName: "Child", Name: "constraint_parent_id", Type: "BIGINT"},
		},
		EmbeddedFields: []schemamodel.EmbeddedField{
			{StructName: "Child", Mode: "relation", Ref: "wf2315_embedded_parents(id)", Field: "embedded_parent_id"},
		},
		Constraints: []schemamodel.Constraint{
			{
				StructName:   "Child",
				Table:        "wf2315_children",
				Name:         "fk_wf2315_children_constraint_parent",
				Type:         "FOREIGN KEY",
				Columns:      []string{"constraint_parent_id"},
				ForeignTable: "wf2315_constraint_parents",
			},
		},
	}
}

// TestInDependencyOrder_CarriesEveryKindOfEdgeFromAnUnfinalizedDeclaration is the
// property the creation carry rests on: a planner orders what it is about to
// create from the creations alone, so every edge the declaration expressed has
// to have travelled with them.
//
// A declaration expresses a table's parent in three ways, and reading the
// finalized dependency map catches all three only when the caller finalized.
// This asserts the parent comes first for each kind independently, so a
// derivation that covers one kind and not another fails on the kind it missed
// rather than passing on the kind it kept.
func TestInDependencyOrder_CarriesEveryKindOfEdgeFromAnUnfinalizedDeclaration(t *testing.T) {
	tests := []struct {
		name   string
		parent string
	}{
		{name: "field foreign key", parent: "wf2315_field_parents"},
		{name: "relation-mode embedded field", parent: "wf2315_embedded_parents"},
		{name: "table-level foreign key constraint", parent: "wf2315_constraint_parents"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := unfinalizedThreeEdgeSchema()
			c.Assert(desired.Dependencies, qt.HasLen, 0)

			ordered := difftypes.TableCreationsFor(desired,
				"wf2315_children", "wf2315_field_parents",
				"wf2315_embedded_parents", "wf2315_constraint_parents",
			).InDependencyOrder()

			names := ordered.Names()
			c.Assert(names, qt.HasLen, 4)
			c.Assert(indexOf(names, test.parent) < indexOf(names, "wf2315_children"), qt.IsTrue,
				qt.Commentf("%q must be created before wf2315_children, got %v", test.parent, names))
		})
	}
}

// TestInDependencyOrder_LeavesAnEdgeToATableItIsNotCreating is the control for
// the skip inside the sort: a creation whose parent already exists is ordered
// among the creations rather than held back waiting for a node that is not
// there.
func TestInDependencyOrder_LeavesAnEdgeToATableItIsNotCreating(t *testing.T) {
	c := qt.New(t)
	desired := unfinalizedThreeEdgeSchema()

	ordered := difftypes.TableCreationsFor(desired, "wf2315_children").InDependencyOrder()

	c.Assert(ordered.Names(), qt.DeepEquals, []string{"wf2315_children"})
}

func indexOf(values []string, want string) int {
	for i, value := range values {
		if value == want {
			return i
		}
	}
	return -1
}
