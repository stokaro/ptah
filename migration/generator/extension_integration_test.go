package generator_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestExtensionMigration_EndToEnd(t *testing.T) {
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
			name: "add single extension",
			generatedSchema: &goschema.Database{
				Extensions: []goschema.Extension{
					{Name: "uuid-ossp", IfNotExists: true, Comment: "Enable UUID generation"},
				},
			},
			databaseSchema: &types.DBSchema{
				Extensions: []types.DBExtension{
					{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"}, // ignored by default
					{Name: "btree_gin", Version: "1.3", Schema: "public"},   // ignored by default
					{Name: "pg_trgm", Version: "1.6", Schema: "public"},     // ignored by default
				},
			},
			expectedUpSQL: []string{
				"-- Enable UUID generation",
				"CREATE EXTENSION IF NOT EXISTS uuid-ossp;",
			},
			expectedDownSQL: []string{
				"WARNING: Removing extension 'uuid-ossp' may break existing functionality",
				"DROP EXTENSION IF EXISTS uuid-ossp;",
			},
			unexpectedUpSQL: []string{
				"DROP EXTENSION",
			},
			unexpectedDownSQL: []string{
				"CREATE EXTENSION",
			},
		},
		{
			name: "add multiple extensions",
			generatedSchema: &goschema.Database{
				Extensions: []goschema.Extension{
					{Name: "uuid-ossp", IfNotExists: true, Comment: "Enable UUID generation"},
					{Name: "hstore", IfNotExists: true, Comment: "Enable key-value store"},
				},
			},
			databaseSchema: &types.DBSchema{
				Extensions: []types.DBExtension{
					{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"}, // ignored by default
					{Name: "btree_gin", Version: "1.3", Schema: "public"},   // ignored by default
					{Name: "pg_trgm", Version: "1.6", Schema: "public"},     // ignored by default
				},
			},
			expectedUpSQL: []string{
				"CREATE EXTENSION IF NOT EXISTS uuid-ossp;",
				"CREATE EXTENSION IF NOT EXISTS hstore;",
			},
			expectedDownSQL: []string{
				"DROP EXTENSION IF EXISTS uuid-ossp;",
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
			name: "remove extension",
			generatedSchema: &goschema.Database{
				Extensions: []goschema.Extension{},
			},
			databaseSchema: &types.DBSchema{
				Extensions: []types.DBExtension{
					{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"}, // ignored by default
					{Name: "btree_gin", Version: "1.3", Schema: "public"},   // ignored by default
					{Name: "pg_trgm", Version: "1.6", Schema: "public"},     // ignored by default
					{Name: "uuid-ossp", Version: "1.1", Schema: "public"},
				},
			},
			expectedUpSQL: []string{
				"DROP EXTENSION IF EXISTS uuid-ossp;",
			},
			expectedDownSQL: []string{
				"CREATE EXTENSION IF NOT EXISTS uuid-ossp;",
			},
			unexpectedUpSQL: []string{
				"CREATE EXTENSION IF NOT EXISTS uuid-ossp;",
			},
			unexpectedDownSQL: []string{
				"DROP EXTENSION",
			},
		},
		{
			name: "extension with version",
			generatedSchema: &goschema.Database{
				Extensions: []goschema.Extension{
					{Name: "postgis", Version: "3.0", IfNotExists: true, Comment: "Geographic data support"},
				},
			},
			databaseSchema: &types.DBSchema{
				Extensions: []types.DBExtension{},
			},
			expectedUpSQL: []string{
				"-- Geographic data support",
				"CREATE EXTENSION IF NOT EXISTS postgis VERSION '3.0';",
			},
			expectedDownSQL: []string{
				"DROP EXTENSION IF EXISTS postgis;",
			},
			unexpectedUpSQL: []string{
				"DROP EXTENSION",
			},
			unexpectedDownSQL: []string{
				"CREATE EXTENSION",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// 1. Calculate schema diff
			diff := schemadiff.Compare(tt.generatedSchema, tt.databaseSchema)
			c.Assert(diff.HasChanges(), qt.IsTrue, qt.Commentf("Expected schema changes to be detected"))

			// 2. Generate up migration SQL
			upSQL := planner.GenerateSchemaDiffSQL(diff, tt.generatedSchema, "postgres")

			// 3. Generate down migration SQL by reversing the diff
			reverseDiff := &difftypes.SchemaDiff{
				ExtensionsAdded:   diff.ExtensionsRemoved,
				ExtensionsRemoved: diff.ExtensionsAdded,
			}

			// For down migrations, we need to convert database schema to go schema format
			dbAsGoSchema := &goschema.Database{
				Extensions: make([]goschema.Extension, len(tt.databaseSchema.Extensions)),
			}
			for i, ext := range tt.databaseSchema.Extensions {
				dbAsGoSchema.Extensions[i] = goschema.Extension{
					Name:        ext.Name,
					IfNotExists: true, // Default for down migrations
				}
			}

			downSQL := planner.GenerateSchemaDiffSQL(reverseDiff, dbAsGoSchema, "postgres")

			// 4. Verify up migration SQL
			for _, expected := range tt.expectedUpSQL {
				c.Assert(strings.Contains(upSQL, expected), qt.IsTrue,
					qt.Commentf("Expected up SQL to contain: %s\nActual up SQL:\n%s", expected, upSQL))
			}

			for _, unexpected := range tt.unexpectedUpSQL {
				c.Assert(strings.Contains(upSQL, unexpected), qt.IsFalse,
					qt.Commentf("Expected up SQL to NOT contain: %s\nActual up SQL:\n%s", unexpected, upSQL))
			}

			// 5. Verify down migration SQL
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

func TestExtensionMigration_UpDownCycle(t *testing.T) {
	c := qt.New(t)

	// Test a complete up/down migration cycle
	generatedSchema := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "uuid-ossp", IfNotExists: true, Comment: "Enable UUID generation"},
			{Name: "hstore", IfNotExists: true, Comment: "Enable key-value store"},
		},
	}

	databaseSchema := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"}, // ignored by default
			{Name: "btree_gin", Version: "1.3", Schema: "public"},   // ignored by default
			{Name: "pg_trgm", Version: "1.6", Schema: "public"},     // ignored by default
		},
	}

	// 1. Calculate initial diff (should add extensions)
	upDiff := schemadiff.Compare(generatedSchema, databaseSchema)
	c.Assert(len(upDiff.ExtensionsAdded), qt.Equals, 2)
	c.Assert(len(upDiff.ExtensionsRemoved), qt.Equals, 0)

	// 2. Simulate applying the up migration (database now has extensions)
	simulatedDatabaseAfterUp := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"}, // ignored by default
			{Name: "btree_gin", Version: "1.3", Schema: "public"},   // ignored by default
			{Name: "pg_trgm", Version: "1.6", Schema: "public"},     // ignored by default
			{Name: "uuid-ossp", Version: "1.1", Schema: "public"},
			{Name: "hstore", Version: "1.8", Schema: "public"},
		},
	}

	// 3. Calculate down diff (should remove extensions)
	downDiff := schemadiff.Compare(&goschema.Database{Extensions: []goschema.Extension{}}, simulatedDatabaseAfterUp)
	c.Assert(len(downDiff.ExtensionsAdded), qt.Equals, 0)
	c.Assert(len(downDiff.ExtensionsRemoved), qt.Equals, 2)

	// 4. Verify the cycle is complete
	c.Assert(upDiff.ExtensionsAdded, qt.DeepEquals, downDiff.ExtensionsRemoved)
	c.Assert(upDiff.ExtensionsRemoved, qt.DeepEquals, downDiff.ExtensionsAdded)
}
