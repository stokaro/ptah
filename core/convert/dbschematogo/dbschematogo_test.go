package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/convert/dbschematogo"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
)

func TestConvertDBSchemaToGoSchema_Extensions(t *testing.T) {
	tests := []struct {
		name     string
		dbSchema *types.DBSchema
		expected []goschema.Extension
	}{
		{
			name: "single extension without comment",
			dbSchema: &types.DBSchema{
				Extensions: []types.DBExtension{
					{
						Name:    "pg_trgm",
						Version: "1.6",
						Schema:  "public",
					},
				},
			},
			expected: []goschema.Extension{
				{
					Name:        "pg_trgm",
					IfNotExists: true,
					Version:     "1.6",
					Comment:     "",
				},
			},
		},
		{
			name: "single extension with comment",
			dbSchema: &types.DBSchema{
				Extensions: []types.DBExtension{
					{
						Name:    "postgis",
						Version: "3.0",
						Schema:  "public",
						Comment: stringPtr("Geographic data support"),
					},
				},
			},
			expected: []goschema.Extension{
				{
					Name:        "postgis",
					IfNotExists: true,
					Version:     "3.0",
					Comment:     "Geographic data support",
				},
			},
		},
		{
			name: "multiple extensions",
			dbSchema: &types.DBSchema{
				Extensions: []types.DBExtension{
					{
						Name:    "pg_trgm",
						Version: "1.6",
						Schema:  "public",
					},
					{
						Name:    "btree_gin",
						Version: "1.3",
						Schema:  "public",
						Comment: stringPtr("Enable GIN indexes on btree types"),
					},
				},
			},
			expected: []goschema.Extension{
				{
					Name:        "pg_trgm",
					IfNotExists: true,
					Version:     "1.6",
					Comment:     "",
				},
				{
					Name:        "btree_gin",
					IfNotExists: true,
					Version:     "1.3",
					Comment:     "Enable GIN indexes on btree types",
				},
			},
		},
		{
			name: "no extensions",
			dbSchema: &types.DBSchema{
				Extensions: []types.DBExtension{},
			},
			expected: []goschema.Extension{},
		},
		{
			name: "nil extensions",
			dbSchema: &types.DBSchema{
				Extensions: nil,
			},
			expected: []goschema.Extension{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := dbschematogo.ConvertDBSchemaToGoSchema(tt.dbSchema)

			c.Assert(len(result.Extensions), qt.Equals, len(tt.expected))
			for i, expectedExt := range tt.expected {
				actualExt := result.Extensions[i]
				c.Assert(actualExt.Name, qt.Equals, expectedExt.Name)
				c.Assert(actualExt.IfNotExists, qt.Equals, expectedExt.IfNotExists)
				c.Assert(actualExt.Version, qt.Equals, expectedExt.Version)
				c.Assert(actualExt.Comment, qt.Equals, expectedExt.Comment)
			}
		})
	}
}

func TestConvertDBSchemaToGoSchema_ExtensionsWithOtherElements(t *testing.T) {
	c := qt.New(t)

	// Test that extensions are properly converted alongside other schema elements
	dbSchema := &types.DBSchema{
		Tables: []types.DBTable{
			{
				Name: "users",
				Columns: []types.DBColumn{
					{
						Name:     "id",
						DataType: "integer",
					},
				},
			},
		},
		Extensions: []types.DBExtension{
			{
				Name:    "pg_trgm",
				Version: "1.6",
				Schema:  "public",
			},
		},
		Enums: []types.DBEnum{
			{
				Name:   "status_type",
				Values: []string{"active", "inactive"},
			},
		},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	// Verify extensions are converted
	c.Assert(len(result.Extensions), qt.Equals, 1)
	c.Assert(result.Extensions[0].Name, qt.Equals, "pg_trgm")
	c.Assert(result.Extensions[0].IfNotExists, qt.Equals, true)
	c.Assert(result.Extensions[0].Version, qt.Equals, "1.6")

	// Verify other elements are also converted
	c.Assert(len(result.Tables), qt.Equals, 1)
	c.Assert(result.Tables[0].Name, qt.Equals, "users")
	c.Assert(len(result.Enums), qt.Equals, 1)
	c.Assert(result.Enums[0].Name, qt.Equals, "status_type")
}

func TestConvertDBSchemaToGoSchema_ExtensionDefaultValues(t *testing.T) {
	c := qt.New(t)

	// Test that extensions get proper default values
	dbSchema := &types.DBSchema{
		Extensions: []types.DBExtension{
			{
				Name:    "test_extension",
				Version: "1.0",
				Schema:  "public",
				// Comment is nil
			},
		},
	}

	result := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	c.Assert(len(result.Extensions), qt.Equals, 1)
	ext := result.Extensions[0]

	// Verify default values
	c.Assert(ext.Name, qt.Equals, "test_extension")
	c.Assert(ext.IfNotExists, qt.Equals, true) // Should default to true for safety
	c.Assert(ext.Version, qt.Equals, "1.0")
	c.Assert(ext.Comment, qt.Equals, "") // Should be empty string when nil
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}
