package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/deporder"
)

// TestGeneratedSelfReferencingForeignKeys_DerivesWhatFinalizeWouldHave is
// stokaro/ptah#2471.
//
// A declaration assembled in memory has an empty SelfReferencingForeignKeys
// map, and an empty map is indistinguishable from a table that has no
// self-reference. The table was created, the plan reported success, and the
// constraint was not there.
//
// Three rows because a declaration expresses a self-reference three ways, and
// covering one says nothing about the others.
func TestGeneratedSelfReferencingForeignKeys_DerivesWhatFinalizeWouldHave(t *testing.T) {
	tests := []struct {
		name   string
		schema *schemamodel.Database
		want   schemamodel.SelfReferencingFK
	}{
		{
			name: "a field's foreign reference",
			schema: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Node", Name: "nodes"}},
				Fields: []schemamodel.Field{
					{StructName: "Node", Name: "id", Type: "BIGINT", Primary: true},
					{StructName: "Node", Name: "parent_id", Type: "BIGINT",
						Foreign: "nodes(id)", ForeignKeyName: "fk_nodes_parent",
						OnDelete: "CASCADE"},
				},
			},
			want: schemamodel.SelfReferencingFK{
				FieldName: "parent_id", Foreign: "nodes(id)",
				ForeignKeyName: "fk_nodes_parent", OnDelete: "CASCADE",
			},
		},
		{
			name: "a relation-mode embedded field",
			schema: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Node", Name: "nodes"}},
				EmbeddedFields: []schemamodel.EmbeddedField{{
					StructName: "Node", Mode: "relation", Field: "parent_id", Ref: "nodes(id)",
				}},
			},
			want: schemamodel.SelfReferencingFK{FieldName: "parent_id", Foreign: "nodes(id)"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			derived := deporder.GeneratedSelfReferencingForeignKeys(test.schema)

			c.Assert(derived["nodes"], qt.DeepEquals, []schemamodel.SelfReferencingFK{test.want})
		})
	}
}

// TestGeneratedSelfReferencingForeignKeys_AReferenceToAnotherTableIsNotOne is
// the control.
//
// Every row above is satisfied by a derivation that recorded every foreign key
// as a self-reference. A reference to another table produces a dependency edge
// instead, and recording it here would make the planner emit a constraint the
// table creation already carries.
func TestGeneratedSelfReferencingForeignKeys_AReferenceToAnotherTableIsNotOne(t *testing.T) {
	c := qt.New(t)
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Node", Name: "nodes"},
			{StructName: "Owner", Name: "owners"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Node", Name: "owner_id", Type: "BIGINT",
				Foreign: "owners(id)", ForeignKeyName: "fk_nodes_owner"},
		},
	}

	derived := deporder.GeneratedSelfReferencingForeignKeys(schema)

	c.Assert(derived["nodes"], qt.HasLen, 0)
	c.Assert(derived["owners"], qt.HasLen, 0)
}

// TestGeneratedSelfReferencingForeignKeys_KeepsWhatTheDeclarationCarries is the
// other direction.
//
// A finalized declaration already holds the map. The derivation unions rather
// than replaces, so a self-reference Finalize recorded and the fields no longer
// express -- one written into the map directly -- is not lost.
func TestGeneratedSelfReferencingForeignKeys_KeepsWhatTheDeclarationCarries(t *testing.T) {
	c := qt.New(t)
	carried := schemamodel.SelfReferencingFK{
		FieldName: "manager_id", Foreign: "nodes(id)", ForeignKeyName: "fk_nodes_manager",
	}
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Node", Name: "nodes"}},
		SelfReferencingForeignKeys: map[string][]schemamodel.SelfReferencingFK{
			"nodes": {carried},
		},
	}

	derived := deporder.GeneratedSelfReferencingForeignKeys(schema)

	c.Assert(derived["nodes"], qt.DeepEquals, []schemamodel.SelfReferencingFK{carried})
}

// TestGeneratedSelfReferencingForeignKeys_DoesNotRecordOneTwice is what makes
// the union safe on a finalized declaration.
//
// Finalize fills the map from the same fields this reads, so every entry would
// otherwise be derived a second time and the planner would emit the constraint
// twice.
func TestGeneratedSelfReferencingForeignKeys_DoesNotRecordOneTwice(t *testing.T) {
	c := qt.New(t)
	both := schemamodel.SelfReferencingFK{
		FieldName: "parent_id", Foreign: "nodes(id)", ForeignKeyName: "fk_nodes_parent",
	}
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Node", Name: "nodes"}},
		Fields: []schemamodel.Field{
			{StructName: "Node", Name: "parent_id", Type: "BIGINT",
				Foreign: "nodes(id)", ForeignKeyName: "fk_nodes_parent"},
		},
		SelfReferencingForeignKeys: map[string][]schemamodel.SelfReferencingFK{
			"nodes": {both},
		},
	}

	derived := deporder.GeneratedSelfReferencingForeignKeys(schema)

	c.Assert(derived["nodes"], qt.HasLen, 1)
}

// TestGeneratedSelfReferencingForeignKeys_LeavesATableLevelConstraintAlone is
// the boundary of the derivation above, and it is a boundary rather than a gap.
//
// [schemamodel.BuildDependencyGraph] refuses the same projection at its own
// constraint branch, in its own words: a table-level constraint keeps its
// structured local and referenced column lists, and SelfReferencingFK is
// intentionally single-column and lossy. The derivation claimed it anyway, so
// the object reached the plan through both pools.
//
// Measured before the fix: the single-column form emitted two identical ALTERs
// under one constraint name, and the composite form emitted a second statement
// naming the column `"owner_a, owner_b"`, which no table has
// (stokaro/ptah#2583).
func TestGeneratedSelfReferencingForeignKeys_LeavesATableLevelConstraintAlone(t *testing.T) {
	tests := []struct {
		name        string
		columns     []string
		foreignCols []string
	}{
		{name: "single column", columns: []string{"parent_id"}, foreignCols: []string{"id"}},
		{name: "composite", columns: []string{"owner_a", "owner_b"}, foreignCols: []string{"a", "b"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			schema := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Node", Name: "nodes"}},
				Constraints: []schemamodel.Constraint{{
					StructName: "Node", Table: "nodes", Name: "fk_nodes_parent",
					Type: "FOREIGN KEY", Columns: test.columns,
					ForeignTable: "nodes", ForeignColumns: test.foreignCols,
				}},
			}

			c.Assert(deporder.GeneratedSelfReferencingForeignKeys(schema)["nodes"], qt.HasLen, 0)
		})
	}
}

// TestGeneratedSelfReferencingForeignKeys_StillDerivesAFieldReference is the
// control that keeps the removal above from reading as "the derivation was
// dropped".
//
// A field's `foreign=` is carried by no other pool, so it is the shape
// stokaro/ptah#2471 was about and the one that still has to be derived.
func TestGeneratedSelfReferencingForeignKeys_StillDerivesAFieldReference(t *testing.T) {
	c := qt.New(t)
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Node", Name: "nodes"}},
		Fields: []schemamodel.Field{{
			StructName: "Node", Name: "parent_id", Type: "INTEGER",
			Foreign: "nodes(id)", ForeignKeyName: "fk_nodes_parent",
		}},
	}

	derived := deporder.GeneratedSelfReferencingForeignKeys(schema)["nodes"]

	c.Assert(derived, qt.HasLen, 1)
	c.Assert(derived[0].FieldName, qt.Equals, "parent_id")
	c.Assert(derived[0].ForeignKeyName, qt.Equals, "fk_nodes_parent")
}
