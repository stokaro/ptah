package generator

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestMigrationFileGeneration_ExtensionSQL(t *testing.T) {
	tests := []struct {
		name              string
		generatedSchema   *goschema.Database
		databaseSchema    *types.DBSchema
		expectedUpSQL     []string
		expectedDownSQL   []string
		unexpectedUpSQL   []string
		unexpectedDownSQL []string
	}{
		{
			name: "extension addition generates correct up and down SQL",
			generatedSchema: &goschema.Database{
				Extensions: []goschema.Extension{
					{Name: "pg_trgm", IfNotExists: true, Comment: "Enable trigram similarity search"},
					{Name: "btree_gin", IfNotExists: true, Comment: "Enable GIN indexes on btree types"},
				},
			},
			databaseSchema: &types.DBSchema{
				Extensions: []types.DBExtension{},
			},
			expectedUpSQL: []string{
				"-- Direction: UP",
				"-- Enable trigram similarity search",
				"CREATE EXTENSION IF NOT EXISTS pg_trgm;",
				"-- Enable GIN indexes on btree types",
				"CREATE EXTENSION IF NOT EXISTS btree_gin;",
			},
			expectedDownSQL: []string{
				"-- Direction: DOWN",
				"WARNING: Removing extension 'pg_trgm' may break existing functionality",
				"DROP EXTENSION IF EXISTS pg_trgm;",
				"WARNING: Removing extension 'btree_gin' may break existing functionality",
				"DROP EXTENSION IF EXISTS btree_gin;",
			},
			unexpectedUpSQL: []string{
				"DROP EXTENSION",
			},
			unexpectedDownSQL: []string{
				"CREATE EXTENSION",
			},
		},
		{
			name: "extension removal generates correct up and down SQL",
			generatedSchema: &goschema.Database{
				Extensions: []goschema.Extension{},
			},
			databaseSchema: &types.DBSchema{
				Extensions: []types.DBExtension{
					{Name: "pg_trgm", Version: "1.6", Schema: "public"},
				},
			},
			expectedUpSQL: []string{
				"-- Direction: UP",
				"WARNING: Removing extension 'pg_trgm' may break existing functionality",
				"DROP EXTENSION IF EXISTS pg_trgm;",
			},
			expectedDownSQL: []string{
				"-- Direction: DOWN",
				"CREATE EXTENSION IF NOT EXISTS pg_trgm",
			},
			unexpectedUpSQL: []string{
				"CREATE EXTENSION IF NOT EXISTS pg_trgm;",
			},
			unexpectedDownSQL: []string{
				"DROP EXTENSION",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// 1. Calculate schema diff
			diff := schemadiff.Compare(tt.generatedSchema, tt.databaseSchema)
			c.Assert(diff.HasChanges(), qt.IsTrue)

			// 2. Generate up migration SQL
			upSQL, err := generateUpMigrationSQL(diff, tt.generatedSchema, "postgres")
			c.Assert(err, qt.IsNil)

			// 3. Generate down migration SQL using the fixed reverseSchemaDiff function
			downSQL, err := generateDownMigrationSQL(diff, tt.generatedSchema, tt.databaseSchema, "postgres")
			c.Assert(err, qt.IsNil)

			// 4. Verify up migration SQL contains expected patterns
			for _, expected := range tt.expectedUpSQL {
				c.Assert(strings.Contains(upSQL, expected), qt.IsTrue,
					qt.Commentf("Expected up SQL to contain: %s\nActual up SQL:\n%s", expected, upSQL))
			}

			for _, unexpected := range tt.unexpectedUpSQL {
				c.Assert(strings.Contains(upSQL, unexpected), qt.IsFalse,
					qt.Commentf("Expected up SQL to NOT contain: %s\nActual up SQL:\n%s", unexpected, upSQL))
			}

			// 5. Verify down migration SQL contains expected patterns
			for _, expected := range tt.expectedDownSQL {
				c.Assert(strings.Contains(downSQL, expected), qt.IsTrue,
					qt.Commentf("Expected down SQL to contain: %s\nActual down SQL:\n%s", expected, downSQL))
			}

			for _, unexpected := range tt.unexpectedDownSQL {
				c.Assert(strings.Contains(downSQL, unexpected), qt.IsFalse,
					qt.Commentf("Expected down SQL to NOT contain: %s\nActual down SQL:\n%s", unexpected, downSQL))
			}
		})
	}
}

func TestReverseSchemaDiff_ExtensionFieldsPresent(t *testing.T) {
	c := qt.New(t)

	// Test that the reverseSchemaDiff function properly handles extension fields
	originalDiff := &difftypes.SchemaDiff{
		ExtensionsAdded:   []string{"pg_trgm", "btree_gin"},
		ExtensionsRemoved: []string{"postgis"},
	}

	reversedDiff := reverseSchemaDiff(originalDiff)

	// Verify that extension fields are properly reversed
	c.Assert(reversedDiff.ExtensionsAdded, qt.DeepEquals, originalDiff.ExtensionsRemoved)
	c.Assert(reversedDiff.ExtensionsRemoved, qt.DeepEquals, originalDiff.ExtensionsAdded)

	// Verify the specific values
	c.Assert(reversedDiff.ExtensionsAdded, qt.DeepEquals, []string{"postgis"})
	c.Assert(reversedDiff.ExtensionsRemoved, qt.DeepEquals, []string{"pg_trgm", "btree_gin"})
}

func TestExtensionMigrationSQL_CompleteFlow(t *testing.T) {
	c := qt.New(t)

	// Test the complete flow: schema with extensions -> empty database -> up migration -> down migration
	generatedSchema := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "pg_trgm", IfNotExists: true, Comment: "Enable trigram similarity search"},
		},
	}

	emptyDatabase := &types.DBSchema{
		Extensions: []types.DBExtension{},
	}

	// 1. Generate up migration (should create extension)
	upDiff := schemadiff.Compare(generatedSchema, emptyDatabase)
	upSQL, err := generateUpMigrationSQL(upDiff, generatedSchema, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Contains(upSQL, "CREATE EXTENSION IF NOT EXISTS pg_trgm;"), qt.IsTrue)

	// 2. Simulate database state after up migration
	databaseAfterUp := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "pg_trgm", Version: "1.6", Schema: "public"},
		},
	}

	// 3. Generate down migration (should drop extension)
	// For down migration, we use the original upDiff and reverse it
	downSQL, err := generateDownMigrationSQL(upDiff, generatedSchema, databaseAfterUp, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Contains(downSQL, "DROP EXTENSION IF EXISTS pg_trgm;"), qt.IsTrue)

	// 4. Verify the cycle is complete
	c.Assert(upDiff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(len(upDiff.ExtensionsRemoved), qt.Equals, 0)
}

// TestMigrationFileGeneration_EmptyDiffPrevention tests the fix for issue #36
// where empty UP migrations with dangerous DOWN migrations were generated.
// Updated to verify that empty diffs are treated as successful no-op operations.
func TestMigrationFileGeneration_EmptyDiffPrevention(t *testing.T) {
	tests := []struct {
		name            string
		generatedSchema *goschema.Database
		databaseSchema  *types.DBSchema
		diff            *difftypes.SchemaDiff
		description     string
	}{
		{
			name: "table modification with missing field definitions should not generate migration",
			generatedSchema: &goschema.Database{
				Tables: []goschema.Table{
					{Name: "users", StructName: "User"},
				},
				Fields: []goschema.Field{
					// Note: Missing the field that the diff claims to add/modify
					// This simulates the scenario where the planner can't find field definitions
				},
			},
			databaseSchema: &types.DBSchema{
				Tables: []types.DBTable{
					{
						Name: "users",
						Columns: []types.DBColumn{
							{Name: "id", DataType: "integer", IsPrimaryKey: true},
						},
					},
				},
			},
			diff: &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{
					{
						TableName:    "users",
						ColumnsAdded: []string{"email"}, // This field doesn't exist in generated schema
					},
				},
			},
			description: "When field definitions are missing, no migration should be generated",
		},
		{
			name: "completely empty diff should not generate migration",
			generatedSchema: &goschema.Database{
				Tables: []goschema.Table{},
				Fields: []goschema.Field{},
			},
			databaseSchema: &types.DBSchema{
				Tables: []types.DBTable{},
			},
			diff: &difftypes.SchemaDiff{
				// Completely empty diff
			},
			description: "Empty diffs should be treated as successful no-op operations",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Generate up migration SQL - should return success with empty SQL for no changes
			upSQL, err := generateUpMigrationSQL(tt.diff, tt.generatedSchema, "postgres")
			c.Assert(err, qt.IsNil, qt.Commentf("Expected success for empty migration, but got error: %v", err))
			c.Assert(upSQL, qt.Equals, "", qt.Commentf("Expected empty SQL for no changes, but got: %s", upSQL))
		})
	}
}

// TestGenerateUpMigrationSQL_NoChangesSuccess tests that generateUpMigrationSQL returns success
// with empty SQL when no actual SQL statements are generated, treating it as a successful no-op operation
func TestGenerateUpMigrationSQL_NoChangesSuccess(t *testing.T) {
	c := qt.New(t)

	// Test case: empty diff should return success with empty SQL
	emptyDiff := &difftypes.SchemaDiff{
		// Completely empty diff
	}

	emptySchema := &goschema.Database{
		Tables: []goschema.Table{},
		Fields: []goschema.Field{},
	}

	// Generate up migration SQL - should return success with empty SQL
	upSQL, err := generateUpMigrationSQL(emptyDiff, emptySchema, "postgres")
	c.Assert(err, qt.IsNil, qt.Commentf("Expected success for empty diff, but got error: %v", err))
	c.Assert(upSQL, qt.Equals, "", qt.Commentf("Expected empty SQL for no changes, but got: %s", upSQL))
}

// TestHasActualSQLStatements tests the helper function that detects comment-only statements
func TestHasActualSQLStatements(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
		expected   bool
	}{
		{
			name:       "empty statements",
			statements: []string{},
			expected:   false,
		},
		{
			name:       "only comments",
			statements: []string{"-- Add/modify columns for table: users --"},
			expected:   false,
		},
		{
			name:       "comments with whitespace",
			statements: []string{"  -- Add/modify columns for table: users --  "},
			expected:   false,
		},
		{
			name:       "multiple comments only",
			statements: []string{"-- Comment 1", "-- Comment 2", "/* Block comment */"},
			expected:   false,
		},
		{
			name:       "actual SQL statement",
			statements: []string{"ALTER TABLE users ADD COLUMN email VARCHAR(255)"},
			expected:   true,
		},
		{
			name:       "mix of comments and SQL",
			statements: []string{"-- Add column", "ALTER TABLE users ADD COLUMN email VARCHAR(255)"},
			expected:   true,
		},
		{
			name:       "SQL with inline comments",
			statements: []string{"ALTER TABLE users ADD COLUMN email VARCHAR(255) -- Add email column"},
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			result := hasActualSQLStatements(tt.statements)
			c.Assert(result, qt.Equals, tt.expected)
		})
	}
}
