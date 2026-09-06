package difftypes_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestSchemaDiff_HasChanges(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		expected bool
	}{
		{
			name:     "no changes",
			diff:     &difftypes.SchemaDiff{},
			expected: false,
		},
		{
			name: "supplemental foreign key removal metadata is not a change",
			diff: &difftypes.SchemaDiff{ForeignKeysRemovedWithTables: []difftypes.ForeignKeyRemovalInfo{{
				Name: "fk_parent", TableName: "children", Columns: []string{"parent_id"},
				ForeignTable: "parents", ForeignColumns: []string{"id"},
			}}},
			expected: false,
		},
		{
			name: "tables added",
			diff: &difftypes.SchemaDiff{
				TablesAdded: difftypes.TableChanges{{Name: "users"}},
			},
			expected: true,
		},
		{
			name: "tables removed",
			diff: &difftypes.SchemaDiff{
				TablesRemoved: []string{"old_table"},
			},
			expected: true,
		},
		{
			name: "tables modified",
			diff: &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{
					{TableName: "users", ColumnsAdded: difftypes.ColumnChanges{{Name: "email"}}},
				},
			},
			expected: true,
		},
		{
			name: "enums added",
			diff: &difftypes.SchemaDiff{
				EnumsAdded: difftypes.EnumChanges{{Name: "status_enum"}},
			},
			expected: true,
		},
		{
			name: "enums removed",
			diff: &difftypes.SchemaDiff{
				EnumsRemoved: difftypes.EnumChanges{{Name: "old_enum"}},
			},
			expected: true,
		},
		{
			name: "enums modified",
			diff: &difftypes.SchemaDiff{
				EnumsModified: []difftypes.EnumDiff{
					{EnumName: "status", ValuesAdded: []string{"pending"}},
				},
			},
			expected: true,
		},
		{
			name: "indexes added",
			diff: &difftypes.SchemaDiff{
				IndexesAdded: difftypes.IndexChanges{
					{Index: schemamodel.Index{Name: "idx_user_email", Fields: []string{"email"}}, TableName: "users"},
				},
			},
			expected: true,
		},
		{
			name: "indexes removed",
			diff: &difftypes.SchemaDiff{
				IndexesRemoved: []difftypes.IndexRef{
					{Name: "old_index", TableName: "users"},
				},
			},
			expected: true,
		},
		{
			name: "extensions added",
			diff: &difftypes.SchemaDiff{
				ExtensionsAdded: difftypes.ExtensionChanges{{Name: "pg_trgm"}},
			},
			expected: true,
		},
		{
			name: "extensions removed",
			diff: &difftypes.SchemaDiff{
				ExtensionsRemoved: difftypes.ExtensionChanges{{Name: "btree_gin"}},
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
