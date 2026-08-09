package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestCompareWithDialect_PartitionAttachedIndexIsNeverPlanned pins the object a
// plan must leave alone: a partition's copy of an index on its partitioned
// parent.
//
// PostgreSQL builds one copy per partition when an index is created on a
// partitioned parent and names it itself, so a desired state written against
// the parent never mentions those names. Read as ordinary indexes they are
// "in the database, not in the desired state" and the plan drops them, which
// PostgreSQL 17.10 refuses with SQLSTATE 2BP01: `cannot drop index
// events_2026_tenant_idx because index idx_events_tenant requires it`. The copy
// exists only after the parent index has been applied, so the defect is
// invisible to a single generate/apply cycle and surfaces on the next generate,
// by which point the file has a checksum and a commit. See #997.
//
// Two rows are discriminating fixtures rather than cases of their own, and both
// were confirmed against PostgreSQL 17.10 before being written down:
//
//   - `events_2026_id_idx` is a standalone index on the partition that carries
//     the same name PostgreSQL would have given a copy. `DROP INDEX
//     "events_2026_id_idx"` succeeds, so an implementation keyed on the naming
//     convention -- or on "this table is a partition, leave its indexes alone"
//     -- silently stops managing an index the server drops on request.
//   - `my_local_created` is a copy that carries a name the convention would
//     never produce, built as CREATE INDEX on the partition and then
//     ALTER INDEX ... ATTACH PARTITION. `DROP INDEX "my_local_created"` is
//     refused with `cannot drop index my_local_created because index
//     idx_events_created requires it`, so the same naming implementation plans
//     a statement the server rejects.
//
// The attachment is the fact; the name is a coincidence in both directions.
func TestCompareWithDialect_PartitionAttachedIndexIsNeverPlanned(t *testing.T) {
	partitionTables := []types.DBTable{
		{Name: "events", Type: "TABLE", Partitioned: true},
		{Name: "events_2026", Type: "TABLE"},
	}
	parentDeclaration := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "events", StructName: "Event"},
			{Name: "events_2026", StructName: "Event2026"},
		},
		Fields: []goschema.Field{
			{StructName: "Event", Name: "tenant", Type: "TEXT"},
			{StructName: "Event2026", Name: "tenant", Type: "TEXT"},
		},
		Indexes: []goschema.Index{
			{Name: "idx_events_tenant", StructName: "Event", Fields: []string{"tenant"}},
		},
	}

	tests := []struct {
		name      string
		generated *goschema.Database
		database  *types.DBSchema
		assert    func(c *qt.C, diff *difftypes.SchemaDiff)
	}{
		{
			name:      "the partition copy of a declared parent index is not dropped",
			generated: parentDeclaration,
			database: &types.DBSchema{
				Tables: partitionTables,
				Indexes: []types.DBIndex{
					{
						Name:      "idx_events_tenant",
						TableName: "events",
						Columns:   []string{"tenant"},
					},
					{
						Name:              "events_2026_tenant_idx",
						TableName:         "events_2026",
						Columns:           []string{"tenant"},
						PartitionAttached: true,
					},
				},
			},
			assert: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
				c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
			},
		},
		{
			name:      "an index created on the partition itself is still dropped",
			generated: parentDeclaration,
			database: &types.DBSchema{
				Tables: partitionTables,
				Indexes: []types.DBIndex{
					{
						Name:      "idx_events_tenant",
						TableName: "events",
						Columns:   []string{"tenant"},
					},
					{
						Name:              "events_2026_tenant_idx",
						TableName:         "events_2026",
						Columns:           []string{"tenant"},
						PartitionAttached: true,
					},
					{
						Name:      "idx_events_2026_local",
						TableName: "events_2026",
						Columns:   []string{"id"},
					},
				},
			},
			assert: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
					{Name: "idx_events_2026_local", TableName: "events_2026"},
				})
			},
		},
		{
			name:      "a standalone partition index named like a copy is still dropped",
			generated: parentDeclaration,
			database: &types.DBSchema{
				Tables: partitionTables,
				Indexes: []types.DBIndex{
					{
						Name:      "idx_events_tenant",
						TableName: "events",
						Columns:   []string{"tenant"},
					},
					{
						Name:      "events_2026_id_idx",
						TableName: "events_2026",
						Columns:   []string{"id"},
					},
				},
			},
			assert: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
					{Name: "events_2026_id_idx", TableName: "events_2026"},
				})
			},
		},
		{
			name:      "a copy attached under a name of its own is not dropped",
			generated: parentDeclaration,
			database: &types.DBSchema{
				Tables: partitionTables,
				Indexes: []types.DBIndex{
					{
						Name:      "idx_events_tenant",
						TableName: "events",
						Columns:   []string{"tenant"},
					},
					{
						Name:              "my_local_created",
						TableName:         "events_2026",
						Columns:           []string{"created_at"},
						PartitionAttached: true,
					},
				},
			},
			assert: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
			},
		},
		{
			name: "a declared index that the database reports as a partition copy is not replaced",
			generated: &goschema.Database{
				Tables: []goschema.Table{
					{Name: "events", StructName: "Event"},
					{Name: "events_2026", StructName: "Event2026"},
				},
				Fields: []goschema.Field{
					{StructName: "Event", Name: "tenant", Type: "TEXT"},
					{StructName: "Event2026", Name: "tenant", Type: "TEXT"},
					{StructName: "Event2026", Name: "id", Type: "BIGINT"},
				},
				Indexes: []goschema.Index{
					{Name: "idx_events_tenant", StructName: "Event", Fields: []string{"tenant"}},
					{Name: "events_2026_tenant_idx", StructName: "Event2026", Fields: []string{"id"}},
				},
			},
			database: &types.DBSchema{
				Tables: partitionTables,
				Indexes: []types.DBIndex{
					{
						Name:      "idx_events_tenant",
						TableName: "events",
						Columns:   []string{"tenant"},
					},
					{
						Name:              "events_2026_tenant_idx",
						TableName:         "events_2026",
						Columns:           []string{"tenant"},
						PartitionAttached: true,
					},
				},
			},
			assert: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
				c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
			},
		},
		{
			name:      "control: an ordinary undeclared index on an ordinary table is dropped",
			generated: parentDeclaration,
			database: &types.DBSchema{
				Tables: append(
					[]types.DBTable{{Name: "members", Type: "TABLE"}},
					partitionTables...,
				),
				Indexes: []types.DBIndex{
					{
						Name:      "idx_events_tenant",
						TableName: "events",
						Columns:   []string{"tenant"},
					},
					{
						Name:      "idx_members_email",
						TableName: "members",
						Columns:   []string{"email"},
					},
				},
			},
			assert: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
					{Name: "idx_members_email", TableName: "members"},
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(tt.generated, tt.database, "postgres")

			tt.assert(c, diff)
		})
	}
}
