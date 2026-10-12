package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// TestOwnerStatementsReportTheirChanges pins that a statement an owner reads
// counts as the change its operation reports: a PostgreSQL CREATE EXTENSION
// adds the extension, and COMMENT ON EXTENSION modifies it. Each statement
// is read alone, so the comment counts though nothing before it in its own
// read declares the extension.
func TestOwnerStatementsReportTheirChanges(t *testing.T) {
	c := qt.New(t)
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"1_extension.sql": "CREATE EXTENSION IF NOT EXISTS pg_trgm;\nCOMMENT ON EXTENSION pg_trgm IS 'trigrams';",
	}), lint.Options{DirFormat: migrationfile.DirFormatAtlas, Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	c.Assert(projectChanges(fileByName(c, analysis, "1_extension.sql").Changes), qt.DeepEquals, []changeProjection{
		{Kind: lint.SchemaChangeAdd, Object: "pg_trgm"},
		{Kind: lint.SchemaChangeModify, Object: "pg_trgm"},
	})
}

// TestExtensionChangesReportTheirChanges pins that DROP EXTENSION and ALTER
// EXTENSION, read through the extension owner, count as the drop and the
// change their operations report, in each spelling the owner reads. A
// statement the parse cannot model contributes no change, so these
// statements reach the count and the review scope only through the owner.
func TestExtensionChangesReportTheirChanges(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []changeProjection
	}{
		{name: "a drop", sql: "DROP EXTENSION pg_trgm;", want: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "pg_trgm"}}},
		{name: "a guarded drop with its dependents", sql: "DROP EXTENSION IF EXISTS pg_trgm CASCADE;",
			want: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "pg_trgm"}}},
		{name: "an update", sql: "ALTER EXTENSION hstore UPDATE TO '1.8';", want: []changeProjection{{Kind: lint.SchemaChangeModify, Object: "hstore"}}},
		{name: "a move", sql: "ALTER EXTENSION citext SET SCHEMA ext;", want: []changeProjection{{Kind: lint.SchemaChangeModify, Object: "citext"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{"1_change.sql": test.sql}),
				lint.Options{DirFormat: migrationfile.DirFormatAtlas, Dialect: "postgres"})

			c.Assert(err, qt.IsNil)
			c.Assert(projectChanges(fileByName(c, analysis, "1_change.sql").Changes), qt.DeepEquals, test.want)
		})
	}
}

// TestExtensionDropsAreDestructive pins that a drop the owner reads still
// meets DS107, CASCADE included, with and without a named target.
func TestExtensionDropsAreDestructive(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
	}{
		{name: "a drop", dialect: "postgres", sql: "DROP EXTENSION pg_trgm;"},
		{name: "a drop with its dependents", dialect: "postgres", sql: "DROP EXTENSION IF EXISTS pg_trgm CASCADE;"},
		{name: "a drop in a run that names no target", sql: "DROP EXTENSION pg_trgm CASCADE;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(lintOneDialect(c, test.dialect, test.sql), qt.DeepEquals, []string{"DS107"})
		})
	}
}
