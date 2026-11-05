package planner_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// skipTargets are bundled targets with planners of their own, among them
// families whose planner has no notion of a skip policy: the policy has to be
// applied before any of them is reached.
var skipTargets = []string{platform.Postgres, platform.MySQL, platform.SQLite, platform.SQLServer}

func droppedLegacyTable() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "legacy"}}}
}

// TestGenerateSchemaDiffSQLStatementsWithOptions_SkipChangeKinds_HappyPath
// pins that every target omits a skipped table drop and writes the comment
// naming it in its place, the words a generated migration carries.
func TestGenerateSchemaDiffSQLStatementsWithOptions_SkipChangeKinds_HappyPath(t *testing.T) {
	for _, dialect := range skipTargets {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			got, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
				c.Context(), must.Must(builtin.New()), droppedLegacyTable(), dialect,
				planext.Options{SkipChangeKinds: []planext.ChangeKind{planext.DropTable}},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, []string{
				"-- SKIP: DROP TABLE of legacy omitted by diff policy (skip: drop_table)",
			})
		})
	}
}

// TestGenerateSchemaDiffSQLStatementsWithOptions_NoSkipChangeKinds_HappyPath
// is the control: the same diff with no kinds listed drops the table on every
// target, so the row above measures the policy and not the fixture.
func TestGenerateSchemaDiffSQLStatementsWithOptions_NoSkipChangeKinds_HappyPath(t *testing.T) {
	for _, dialect := range skipTargets {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			got, err := planner.GenerateSchemaDiffSQL(c.Context(), must.Must(builtin.New()), droppedLegacyTable(), dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Contains, "DROP TABLE")
			c.Assert(got, qt.Not(qt.Contains), "SKIP:")
		})
	}
}

// recordingPlanner is a target's planner, registered the way a provider
// registers one, that keeps the diff it was called with.
type recordingPlanner struct {
	diff **difftypes.SchemaDiff
}

func (p recordingPlanner) GenerateMigrationAST(_ context.Context, _ featureplan.Runtime, diff *difftypes.SchemaDiff) ([]ast.Node, error) {
	*p.diff = diff
	return []ast.Node{ast.NewComment("planned by the external target")}, nil
}

// TestGenerateSchemaDiffASTWithOptions_SkipChangeKindsBeforeAnExternalPlanner
// pins that a target's own planner -- a provider's, for one -- is handed the
// diff without the skipped change and options that list no kinds, and that
// the plan still opens with the comment naming what was left out.
func TestGenerateSchemaDiffASTWithOptions_SkipChangeKindsBeforeAnExternalPlanner(t *testing.T) {
	c := qt.New(t)
	var built []planext.Options
	var received *difftypes.SchemaDiff
	runtime, err := engine.New(engine.Provider{ID: "example.org/external", Targets: []engine.Target{{
		Name: "external",
		Planner: func(opts planext.Options) planext.Planner {
			built = append(built, opts)
			return recordingPlanner{diff: &received}
		},
	}}})
	c.Assert(err, qt.IsNil)
	diff := &difftypes.SchemaDiff{}
	diff.SetIndexRemovals([]difftypes.IndexRef{{Name: "items_note", TableName: "items"}})

	nodes, err := planner.GenerateSchemaDiffASTWithOptions(c.Context(), runtime, diff, "external",
		planext.Options{SkipChangeKinds: []planext.ChangeKind{planext.DropIndex}})

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.DeepEquals, []ast.Node{
		ast.NewComment("SKIP: DROP INDEX of items.items_note omitted by diff policy (skip: drop_index)"),
		ast.NewComment("planned by the external target"),
	})
	c.Assert(built, qt.HasLen, 1)
	c.Assert(built[0].SkipChangeKinds, qt.IsNil)
	c.Assert(received.IndexRemovals(), qt.HasLen, 0)
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{{Name: "items_note", TableName: "items"}})
}
