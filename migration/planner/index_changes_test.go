package planner_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/planner"
)

// TestEveryPlannerButYDBRefusesIndexChangesInPlace drives a hand-built diff
// that renames an index, one that writes an index comment, and one that
// changes a YDB index's partitioning through every registered planner but
// YDB's. The comparison records none of them on those targets, so only such a
// diff reaches them, and a planner that read none of them would plan nothing
// and report the database synced. A target with index_comments, the
// PostgreSQL family's, plans the comment; TestIndexCommentsArePlannedInPlace
// covers it.
func TestEveryPlannerButYDBRefusesIndexChangesInPlace(t *testing.T) {
	c := qt.New(t)
	dialects := builtintest.Runtime().Targets()
	dialects = slices.DeleteFunc(dialects, func(dialect string) bool { return dialect == platform.YDB })
	c.Assert(len(dialects) >= 8, qt.IsTrue, qt.Commentf("registered dialects: %v", dialects))
	withoutIndexComments := slices.DeleteFunc(slices.Clone(dialects), func(dialect string) bool {
		return capability.ForDialect(dialect).Has(capability.IndexComments)
	})
	c.Assert(withoutIndexComments, qt.Contains, platform.Spanner)

	type refused struct {
		dialects []string
		diff     *difftypes.SchemaDiff
	}
	cases := map[string]refused{
		"rename": {dialects: dialects, diff: &difftypes.SchemaDiff{
			IndexesRenamed: []difftypes.IndexRename{{TableName: "users", From: "a", To: "b"}},
		}},
		"partitioning": {dialects: dialects, diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{
			Subject: objectidentity.NewBuilder(identifier.ForDialect(platform.YDB)).IndexParts("", "users", "a"),
			Value: &ydbdiff.IndexPartitioning{After: &ydbschema.DesiredIndexPartitioning{
				IndexPartitioning: ydbschema.IndexPartitioning{MinPartitions: 2},
			}},
		}}}},
		"comment": {dialects: withoutIndexComments, diff: &difftypes.SchemaDiff{
			IndexCommentsChanged: []difftypes.IndexCommentChange{{TableName: "users", Name: "a", Desired: "x"}},
		}},
	}
	for name, refusal := range cases {
		for _, dialect := range refusal.dialects {
			t.Run(dialect+"/"+name, func(t *testing.T) {
				c := qt.New(t)
				nodes, err := planner.GenerateSchemaDiffAST(
					context.Background(), builtintest.Runtime(),
					refusal.diff, dialect,
				)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(nodes, qt.IsNil)
			})
		}
	}
}

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
