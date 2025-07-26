package generator

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestReverseSchemaDiff_Extensions(t *testing.T) {
	tests := []struct {
		name     string
		input    *types.SchemaDiff
		expected *types.SchemaDiff
	}{
		{
			name: "reverse extensions added and removed",
			input: &types.SchemaDiff{
				ExtensionsAdded:   []string{"pg_trgm", "btree_gin"},
				ExtensionsRemoved: []string{"postgis", "uuid-ossp"},
			},
			expected: &types.SchemaDiff{
				ExtensionsAdded:   []string{"postgis", "uuid-ossp"},
				ExtensionsRemoved: []string{"pg_trgm", "btree_gin"},
			},
		},
		{
			name: "reverse only extensions added",
			input: &types.SchemaDiff{
				ExtensionsAdded:   []string{"pg_trgm"},
				ExtensionsRemoved: []string{},
			},
			expected: &types.SchemaDiff{
				ExtensionsAdded:   []string{},
				ExtensionsRemoved: []string{"pg_trgm"},
			},
		},
		{
			name: "reverse only extensions removed",
			input: &types.SchemaDiff{
				ExtensionsAdded:   []string{},
				ExtensionsRemoved: []string{"postgis"},
			},
			expected: &types.SchemaDiff{
				ExtensionsAdded:   []string{"postgis"},
				ExtensionsRemoved: []string{},
			},
		},
		{
			name: "no extensions to reverse",
			input: &types.SchemaDiff{
				ExtensionsAdded:   []string{},
				ExtensionsRemoved: []string{},
			},
			expected: &types.SchemaDiff{
				ExtensionsAdded:   []string{},
				ExtensionsRemoved: []string{},
			},
		},
		{
			name: "nil extension slices",
			input: &types.SchemaDiff{
				ExtensionsAdded:   nil,
				ExtensionsRemoved: nil,
			},
			expected: &types.SchemaDiff{
				ExtensionsAdded:   nil,
				ExtensionsRemoved: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := reverseSchemaDiff(tt.input)

			c.Assert(result.ExtensionsAdded, qt.DeepEquals, tt.expected.ExtensionsAdded)
			c.Assert(result.ExtensionsRemoved, qt.DeepEquals, tt.expected.ExtensionsRemoved)
		})
	}
}

func TestReverseSchemaDiff_CompleteReversal(t *testing.T) {
	c := qt.New(t)

	// Test that all fields are properly reversed
	input := &types.SchemaDiff{
		TablesAdded:       []string{"users", "posts"},
		TablesRemoved:     []string{"old_table"},
		EnumsAdded:        []string{"status_type"},
		EnumsRemoved:      []string{"old_enum"},
		IndexesAdded:      []string{"idx_users_email"},
		IndexesRemoved:    []string{"idx_old"},
		ExtensionsAdded:   []string{"pg_trgm", "btree_gin"},
		ExtensionsRemoved: []string{"postgis"},
	}

	result := reverseSchemaDiff(input)

	// Verify all reversals
	c.Assert(result.TablesAdded, qt.DeepEquals, input.TablesRemoved)
	c.Assert(result.TablesRemoved, qt.DeepEquals, input.TablesAdded)
	c.Assert(result.EnumsAdded, qt.DeepEquals, input.EnumsRemoved)
	c.Assert(result.EnumsRemoved, qt.DeepEquals, input.EnumsAdded)
	c.Assert(result.IndexesAdded, qt.DeepEquals, input.IndexesRemoved)
	c.Assert(result.IndexesRemoved, qt.DeepEquals, input.IndexesAdded)
	c.Assert(result.ExtensionsAdded, qt.DeepEquals, input.ExtensionsRemoved)
	c.Assert(result.ExtensionsRemoved, qt.DeepEquals, input.ExtensionsAdded)
}

func TestReverseSchemaDiff_TableModifications(t *testing.T) {
	c := qt.New(t)

	// Test table modifications reversal
	input := &types.SchemaDiff{
		TablesModified: []types.TableDiff{
			{
				TableName:      "users",
				ColumnsAdded:   []string{"email", "created_at"},
				ColumnsRemoved: []string{"legacy_field"},
				ColumnsModified: []types.ColumnDiff{
					{
						ColumnName: "name",
						Changes:    map[string]string{"type": "VARCHAR(100) -> VARCHAR(255)"},
					},
				},
			},
		},
	}

	result := reverseSchemaDiff(input)

	c.Assert(len(result.TablesModified), qt.Equals, 1)

	reversedTable := result.TablesModified[0]
	c.Assert(reversedTable.TableName, qt.Equals, "users")
	c.Assert(reversedTable.ColumnsAdded, qt.DeepEquals, []string{"legacy_field"})
	c.Assert(reversedTable.ColumnsRemoved, qt.DeepEquals, []string{"email", "created_at"})

	c.Assert(len(reversedTable.ColumnsModified), qt.Equals, 1)
	reversedColumn := reversedTable.ColumnsModified[0]
	c.Assert(reversedColumn.ColumnName, qt.Equals, "name")
	c.Assert(reversedColumn.Changes["type"], qt.Equals, "VARCHAR(255) -> VARCHAR(100)")
}

func TestReverseSchemaDiff_EnumModifications(t *testing.T) {
	c := qt.New(t)

	// Test enum modifications reversal
	input := &types.SchemaDiff{
		EnumsModified: []types.EnumDiff{
			{
				EnumName:      "status_type",
				ValuesAdded:   []string{"pending", "archived"},
				ValuesRemoved: []string{"deprecated"},
			},
		},
	}

	result := reverseSchemaDiff(input)

	c.Assert(len(result.EnumsModified), qt.Equals, 1)

	reversedEnum := result.EnumsModified[0]
	c.Assert(reversedEnum.EnumName, qt.Equals, "status_type")
	c.Assert(reversedEnum.ValuesAdded, qt.DeepEquals, []string{"deprecated"})
	c.Assert(reversedEnum.ValuesRemoved, qt.DeepEquals, []string{"pending", "archived"})
}
