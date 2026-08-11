package types_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestSchemaDiff_HasChanges(t *testing.T) {
	tests := []struct {
		name     string
		diff     *types.SchemaDiff
		expected bool
	}{
		{
			name:     "no changes",
			diff:     &types.SchemaDiff{},
			expected: false,
		},
		{
			name: "supplemental foreign key removal metadata is not a change",
			diff: &types.SchemaDiff{ForeignKeysRemovedWithTables: []types.ForeignKeyRemovalInfo{{
				Name: "fk_parent", TableName: "children", Columns: []string{"parent_id"},
				ForeignTable: "parents", ForeignColumns: []string{"id"},
			}}},
			expected: false,
		},
		{
			name: "tables added",
			diff: &types.SchemaDiff{
				TablesAdded: []string{"users"},
			},
			expected: true,
		},
		{
			name: "tables removed",
			diff: &types.SchemaDiff{
				TablesRemoved: []string{"old_table"},
			},
			expected: true,
		},
		{
			name: "tables modified",
			diff: &types.SchemaDiff{
				TablesModified: []types.TableDiff{
					{TableName: "users", ColumnsAdded: []string{"email"}},
				},
			},
			expected: true,
		},
		{
			name: "enums added",
			diff: &types.SchemaDiff{
				EnumsAdded: []string{"status_enum"},
			},
			expected: true,
		},
		{
			name: "enums removed",
			diff: &types.SchemaDiff{
				EnumsRemoved: []string{"old_enum"},
			},
			expected: true,
		},
		{
			name: "enums modified",
			diff: &types.SchemaDiff{
				EnumsModified: []types.EnumDiff{
					{EnumName: "status", ValuesAdded: []string{"pending"}},
				},
			},
			expected: true,
		},
		{
			name: "indexes added",
			diff: &types.SchemaDiff{
				IndexesAdded: []types.IndexRef{
					{Name: "idx_user_email", TableName: "users"},
				},
			},
			expected: true,
		},
		{
			name: "indexes removed",
			diff: &types.SchemaDiff{
				IndexesRemoved: []types.IndexRef{
					{Name: "old_index", TableName: "users"},
				},
			},
			expected: true,
		},
		{
			name: "extensions added",
			diff: &types.SchemaDiff{
				ExtensionsAdded: []string{"pg_trgm"},
			},
			expected: true,
		},
		{
			name: "extensions removed",
			diff: &types.SchemaDiff{
				ExtensionsRemoved: []string{"btree_gin"},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			result := tt.diff.HasChanges()
			c.Assert(result, qt.Equals, tt.expected)
		})
	}
}
