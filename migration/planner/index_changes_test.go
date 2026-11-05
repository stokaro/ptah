package planner_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/planner"
)

// TestIndexCommentsArePlannedInPlace writes a kept index's changed comment
// with COMMENT ON INDEX on every target with index_comments, in the spelling
// that target names an index with, and removes a cleared one with NULL.
func TestIndexCommentsArePlannedInPlace(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		desired string
		want    string
	}{
		{name: "postgres", dialect: platform.Postgres, desired: "x", want: `COMMENT ON INDEX "app"."a" IS 'x'`},
		{name: "yugabytedb", dialect: platform.YugabyteDB, desired: "x", want: `COMMENT ON INDEX "app"."a" IS 'x'`},
		{name: "cockroachdb", dialect: platform.CockroachDB, desired: "x", want: `COMMENT ON INDEX "app"."users"@"a" IS 'x'`},
		{name: "a cleared comment", dialect: platform.Postgres, desired: "", want: `COMMENT ON INDEX "app"."a" IS NULL`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{IndexCommentsChanged: []difftypes.IndexCommentChange{{
				TableName: "app.users", Name: "a", Current: "old", Desired: test.desired,
			}}}

			statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), builtintest.Runtime(), diff, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{test.want})
		})
	}
}
