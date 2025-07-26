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
					{Name: "pg_trgm", IfNotExists: true, Comment: "Enable trigram similarity search"},
				},
			},
			databaseSchema: &types.DBSchema{
				Extensions: []types.DBExtension{},
			},
			expectedUpSQL: []string{
				"-- Enable trigram similarity search",
				"CREATE EXTENSION IF NOT EXISTS pg_trgm;",
			},
			expectedDownSQL: []string{
				"WARNING: Removing extension 'pg_trgm' may break existing functionality",
				"DROP EXTENSION IF EXISTS pg_trgm;",
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
					{Name: "pg_trgm", IfNotExists: true, Comment: "Enable trigram similarity search"},
					{Name: "btree_gin", IfNotExists: true, Comment: "Enable GIN indexes on btree types"},
				},
			},
			databaseSchema: &types.DBSchema{
				Extensions: []types.DBExtension{},
			},
			expectedUpSQL: []string{
				"CREATE EXTENSION IF NOT EXISTS pg_trgm;",
				"CREATE EXTENSION IF NOT EXISTS btree_gin;",
			},
			expectedDownSQL: []string{
				"DROP EXTENSION IF EXISTS pg_trgm;",
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
			name: "remove extension",
			generatedSchema: &goschema.Database{
				Extensions: []goschema.Extension{},
			},
			databaseSchema: &types.DBSchema{
				Extensions: []types.DBExtension{
					{Name: "pg_trgm", Version: "1.6", Schema: "public"},
				},
			},
			expectedUpSQL: []string{
				"DROP EXTENSION IF EXISTS pg_trgm;",
			},
			expectedDownSQL: []string{
				"CREATE EXTENSION IF NOT EXISTS pg_trgm;",
			},
			unexpectedUpSQL: []string{
				"CREATE EXTENSION IF NOT EXISTS pg_trgm;",
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
			{Name: "pg_trgm", IfNotExists: true, Comment: "Enable trigram similarity search"},
			{Name: "btree_gin", IfNotExists: true, Comment: "Enable GIN indexes on btree types"},
		},
	}

	databaseSchema := &types.DBSchema{
		Extensions: []types.DBExtension{},
	}

	// 1. Calculate initial diff (should add extensions)
	upDiff := schemadiff.Compare(generatedSchema, databaseSchema)
	c.Assert(len(upDiff.ExtensionsAdded), qt.Equals, 2)
	c.Assert(len(upDiff.ExtensionsRemoved), qt.Equals, 0)

	// 2. Simulate applying the up migration (database now has extensions)
	simulatedDatabaseAfterUp := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "pg_trgm", Version: "1.6", Schema: "public"},
			{Name: "btree_gin", Version: "1.3", Schema: "public"},
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
