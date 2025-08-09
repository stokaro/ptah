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
					{Name: "uuid-ossp", IfNotExists: true, Comment: "Enable UUID generation"},
					{Name: "hstore", IfNotExists: true, Comment: "Enable key-value store"},
				},
			},
			databaseSchema: &types.DBSchema{
				Extensions: []types.DBExtension{
					{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},    // ignored by default
					{Name: "btree_gin", Version: "1.3", Schema: "public"},     // ignored by default
					{Name: "pg_trgm", Version: "1.6", Schema: "public"},       // ignored by default
				},
			},
			expectedUpSQL: []string{
				"-- Direction: UP",
				"-- Enable UUID generation",
				"CREATE EXTENSION IF NOT EXISTS uuid-ossp;",
				"-- Enable key-value store",
				"CREATE EXTENSION IF NOT EXISTS hstore;",
			},
			expectedDownSQL: []string{
				"-- Direction: DOWN",
				"WARNING: Removing extension 'uuid-ossp' may break existing functionality",
				"DROP EXTENSION IF EXISTS uuid-ossp;",
				"WARNING: Removing extension 'hstore' may break existing functionality",
				"DROP EXTENSION IF EXISTS hstore;",
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
					{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},    // ignored by default
					{Name: "btree_gin", Version: "1.3", Schema: "public"},     // ignored by default
					{Name: "pg_trgm", Version: "1.6", Schema: "public"},       // ignored by default
					{Name: "uuid-ossp", Version: "1.1", Schema: "public"},
				},
			},
			expectedUpSQL: []string{
				"-- Direction: UP",
				"WARNING: Removing extension 'uuid-ossp' may break existing functionality",
				"DROP EXTENSION IF EXISTS uuid-ossp;",
			},
			expectedDownSQL: []string{
				"-- Direction: DOWN",
				"CREATE EXTENSION IF NOT EXISTS uuid-ossp",
			},
			unexpectedUpSQL: []string{
				"CREATE EXTENSION IF NOT EXISTS uuid-ossp;",
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
			downSQL, err := generateDownMigrationSQL(diff, tt.databaseSchema, "postgres")
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

	// Test the complete flow: schema with extensions -> database with ignored extensions -> up migration -> down migration
	generatedSchema := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "uuid-ossp", IfNotExists: true, Comment: "Enable UUID generation"},
		},
	}

	databaseWithIgnoredExtensions := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},    // ignored by default
			{Name: "btree_gin", Version: "1.3", Schema: "public"},     // ignored by default
			{Name: "pg_trgm", Version: "1.6", Schema: "public"},       // ignored by default
		},
	}

	// 1. Generate up migration (should create extension)
	upDiff := schemadiff.Compare(generatedSchema, databaseWithIgnoredExtensions)
	upSQL, err := generateUpMigrationSQL(upDiff, generatedSchema, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Contains(upSQL, "CREATE EXTENSION IF NOT EXISTS uuid-ossp;"), qt.IsTrue)

	// 2. Simulate database state after up migration
	databaseAfterUp := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},    // ignored by default
			{Name: "btree_gin", Version: "1.3", Schema: "public"},     // ignored by default
			{Name: "pg_trgm", Version: "1.6", Schema: "public"},       // ignored by default
			{Name: "uuid-ossp", Version: "1.1", Schema: "public"},
		},
	}

	// 3. Generate down migration (should drop extension)
	// For down migration, we use the original upDiff and reverse it
	downSQL, err := generateDownMigrationSQL(upDiff, databaseAfterUp, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Contains(downSQL, "DROP EXTENSION IF EXISTS uuid-ossp;"), qt.IsTrue)

	// 4. Verify the cycle is complete
	c.Assert(upDiff.ExtensionsAdded, qt.DeepEquals, []string{"uuid-ossp"})
	c.Assert(len(upDiff.ExtensionsRemoved), qt.Equals, 0)
}
