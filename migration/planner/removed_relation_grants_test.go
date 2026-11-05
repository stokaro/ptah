package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// readerOn is the reader role's SELECT on a table or a view.
func readerOn(object string) []difftypes.GrantRef {
	return []difftypes.GrantRef{{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: object}}
}

// A grant the comparison adds on an object the plan removes names something the
// source no longer declares, so the plan is refused before any statement exists.
func TestGenerateSchemaDiffAST_RefusesAGrantOnARemovedRelation_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name string
		diff difftypes.SchemaDiff
		want string
	}{
		{name: "a removed view", diff: difftypes.SchemaDiff{
			ViewsRemoved: difftypes.ViewChanges{{Name: "recent", Body: "SELECT id FROM items"}},
			GrantsAdded:  readerOn("recent")},
			want: "granting SELECT on view recent to reader: the plan drops the view; declare it, or leave the grant out"},
		{name: "a removed materialized view", diff: difftypes.SchemaDiff{
			MaterializedViewsRemoved: difftypes.MaterializedViewChanges{{Name: "totals", Body: "SELECT id FROM items"}},
			GrantsAdded:              readerOn("totals")},
			want: "granting SELECT on materialized view totals to reader: the plan drops the materialized view; declare it, or leave the grant out"},
		{name: "a removed table", diff: difftypes.SchemaDiff{
			TablesRemoved: difftypes.TableRemovals{{Name: "items"}},
			GrantsAdded:   readerOn("items")},
			want: "granting SELECT on table items to reader: the plan drops the table; declare it, or leave the grant out"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := planner.GenerateSchemaDiffAST(t.Context(), must.Must(builtin.New()), &test.diff, platform.Postgres)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, ".*"+test.want)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// Skipping the drop keeps the table, so the grant has an object to name. A grant
// on another object, and a grant that names no relation, are not this refusal's.
func TestGenerateSchemaDiffAST_RefusesAGrantOnARemovedRelation_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		diff difftypes.SchemaDiff
		opts planext.Options
	}{
		{name: "a removal diff.skip keeps", diff: difftypes.SchemaDiff{
			TablesRemoved: difftypes.TableRemovals{{Name: "items"}},
			GrantsAdded:   readerOn("items")},
			opts: planext.Options{SkipChangeKinds: []planext.ChangeKind{planext.DropTable}}},
		{name: "a grant on another table", diff: difftypes.SchemaDiff{
			TablesRemoved: difftypes.TableRemovals{{Name: "items"}},
			GrantsAdded:   readerOn("orders")}},
		{name: "a schema named as the view is", diff: difftypes.SchemaDiff{
			ViewsRemoved: difftypes.ViewChanges{{Name: "recent", Body: "SELECT id FROM items"}},
			GrantsAdded:  []difftypes.GrantRef{{Role: "reader", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "recent"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := planner.GenerateSchemaDiffASTWithOptions(t.Context(), must.Must(builtin.New()), &test.diff, platform.Postgres, test.opts)

			c.Assert(err, qt.IsNil)
		})
	}
}
