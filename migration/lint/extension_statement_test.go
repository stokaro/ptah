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
