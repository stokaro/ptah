package planner_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// unloggedDiff changes the table app.cache to unlogged, or back.
func unloggedDiff(unlogged bool) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "app.cache", UnloggedChange: &difftypes.UnloggedChange{Current: !unlogged, Desired: unlogged},
	}}}
}

// TestUnloggedChange_HappyPath writes a table's change between logged and
// unlogged as one ALTER TABLE on PostgreSQL.
func TestUnloggedChange_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		unlogged bool
		want     string
	}{
		{name: "made unlogged", unlogged: true, want: "-- ALTER statements: --\nALTER TABLE \"app\".\"cache\" SET UNLOGGED"},
		{name: "made logged", unlogged: false, want: "-- ALTER statements: --\nALTER TABLE \"app\".\"cache\" SET LOGGED"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), builtintest.Runtime(),
				unloggedDiff(test.unlogged), platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{test.want})
		})
	}
}

// TestUnloggedChange_FailurePath refuses the change on a family member that
// creates no unlogged table. The comparison records none there, so only a diff
// built by hand reaches it, and writing nothing would report the change made.
func TestUnloggedChange_FailurePath(t *testing.T) {
	for _, dialect := range []string{platform.YugabyteDB, platform.CockroachDB, platform.Spanner} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), builtintest.Runtime(),
				unloggedDiff(true), dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*`+dialect+`: ALTER TABLE \.\.\. SET UNLOGGED cannot be rendered: this target creates no unlogged table.*`)
			c.Assert(statements, qt.IsNil)
		})
	}
}
