package generator_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgextension"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// declaringExtension is a desired schema that declares one extension to the
// extension owner and claims that it declares every extension.
func declaringExtension(name string, extension pgextension.DesiredExtension) *schemamodel.Database {
	return &schemamodel.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(must.Must(pgextension.DesiredObject(pgextension.Ref(name), extension)))),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Desired)),
	}
}

// TestExtensionMigration_EndToEnd pins the up migration a comparison plans
// for the extension owner's changes, rendered as a migration file renders it.
// The down migrations are pinned with the generator's own reversal in
// TestMigrationFileGeneration_ExtensionSQL.
func TestExtensionMigration_EndToEnd(t *testing.T) {
	tests := []struct {
		name            string
		generatedSchema *schemamodel.Database
		databaseSchema  *catalog.Database
		expectedUpSQL   []string
		unexpectedUpSQL []string
	}{
		{
			name:            "add single extension",
			generatedSchema: declaringExtension("pg_trgm", pgextension.DesiredExtension{IfNotExists: true, Comment: "Enable trigram similarity search"}),
			databaseSchema:  holdingExtensions(),
			expectedUpSQL: []string{
				"CREATE EXTENSION IF NOT EXISTS pg_trgm;",
				"COMMENT ON EXTENSION pg_trgm IS 'Enable trigram similarity search';",
			},
			unexpectedUpSQL: []string{"DROP EXTENSION"},
		},
		{
			name:            "add multiple extensions",
			generatedSchema: declaringExtensions("pg_trgm", "btree_gin"),
			databaseSchema:  holdingExtensions(),
			expectedUpSQL: []string{
				"CREATE EXTENSION IF NOT EXISTS pg_trgm;",
				"CREATE EXTENSION IF NOT EXISTS btree_gin;",
			},
			unexpectedUpSQL: []string{"DROP EXTENSION"},
		},
		{
			name:            "remove extension",
			generatedSchema: declaringExtensions(),
			databaseSchema:  holdingExtensions(installedExtension{"pg_trgm", "public", "1.6"}),
			expectedUpSQL: []string{
				"WARNING: Removing extension 'pg_trgm' may break existing functionality",
				"DROP EXTENSION IF EXISTS pg_trgm;",
			},
			unexpectedUpSQL: []string{"CREATE EXTENSION"},
		},
		{
			name:            "extension with version",
			generatedSchema: declaringExtension("postgis", pgextension.DesiredExtension{Version: "3.0", IfNotExists: true, Comment: "Geographic data support"}),
			databaseSchema:  holdingExtensions(),
			expectedUpSQL: []string{
				"CREATE EXTENSION IF NOT EXISTS postgis VERSION '3.0';",
				"COMMENT ON EXTENSION postgis IS 'Geographic data support';",
			},
			unexpectedUpSQL: []string{"DROP EXTENSION"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := must.Must(schemadiff.CompareWithDialect(t.Context(),
				tt.generatedSchema, tt.databaseSchema, "postgres", must.Must(builtin.New()),
			))
			c.Assert(diff.HasChanges(), qt.IsTrue, qt.Commentf("Expected schema changes to be detected"))

			upSQL, err := planner.GenerateSchemaDiffSQL(context.Background(), must.Must(builtin.New()), diff, "postgres")
			c.Assert(err, qt.IsNil)
			upSQL = legacyRenderedSQL(upSQL)

			for _, expected := range tt.expectedUpSQL {
				c.Assert(upSQL, qt.Contains, expected,
					qt.Commentf("Expected up SQL to contain: %s\nActual up SQL:\n%s", expected, upSQL))
			}
			for _, unexpected := range tt.unexpectedUpSQL {
				c.Assert(upSQL, qt.Not(qt.Contains), unexpected,
					qt.Commentf("Expected up SQL to NOT contain: %s\nActual up SQL:\n%s", unexpected, upSQL))
			}
		})
	}
}

func TestExtensionMigration_UpDownCycle(t *testing.T) {
	c := qt.New(t)

	// 1. The up comparison installs both extensions.
	upDiff := must.Must(schemadiff.CompareWithDialect(t.Context(),
		declaringExtensions("pg_trgm", "btree_gin"), holdingExtensions(), "postgres", must.Must(builtin.New()),
	))
	upAdded, upRemoved := extensionChanges(upDiff)
	c.Assert(upAdded, qt.DeepEquals, []string{"btree_gin", "pg_trgm"})
	c.Assert(upRemoved, qt.HasLen, 0)

	// 2. After the up migration the database holds them, and a schema that
	// declares none drops both.
	afterUp := holdingExtensions(installedExtension{"pg_trgm", "public", "1.6"}, installedExtension{"btree_gin", "public", "1.3"})
	downDiff := must.Must(schemadiff.CompareWithDialect(t.Context(),
		declaringExtensions(), afterUp, "postgres", must.Must(builtin.New()),
	))
	downAdded, downRemoved := extensionChanges(downDiff)
	c.Assert(downAdded, qt.HasLen, 0)
	c.Assert(downRemoved, qt.DeepEquals, upAdded)
}
