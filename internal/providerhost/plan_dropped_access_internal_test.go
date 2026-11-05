package providerhost

// White-box testing required: which revocations the common planner leaves to
// a drop is decided in its steps before scheduling, which only a provider
// process sees. integration/providercli covers the plan a provider renders.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/modelast"
)

// readerOn is the reader role's SELECT on object, a table or a view.
func readerOn(object string) difftypes.GrantRef {
	return difftypes.GrantRef{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: object}
}

// recentQueryChange changes the query of the view main.recent.
var recentQueryChange = []difftypes.ViewDiff{{ViewName: "main.recent", PreviousBody: "SELECT id FROM items",
	Desired: schemamodel.View{Name: "main.recent", Body: "SELECT id FROM items WHERE id > 0"}}}

// totalsQueryChange changes the query of the materialized view main.totals,
// which the planner drops and creates again.
var totalsQueryChange = []difftypes.MaterializedViewDiff{{ViewName: "main.totals", Changes: map[string]string{"body": "changed"},
	Desired: schemamodel.MaterializedView{Name: "main.totals", Body: "SELECT count(*) FROM items WHERE id > 0"}}}

// removedTotals removes the materialized view main.totals.
var removedTotals = difftypes.MaterializedViewChanges{{Name: "main.totals", Body: "SELECT count(*) FROM items"}}

// commonPlannerFor is the common planner of the fixture target, with or
// without a view replaced in place.
func commonPlannerFor(inPlace bool) commonPlanner {
	return commonPlanner{target: "fixture", commonObjects: true,
		options: planext.Options{Capabilities: capability.Capabilities{}.With(capability.CreateOrReplaceView, inPlace)}}
}

// fixtureSemantics are the identifier rules the tests compare names by.
func fixtureSemantics() *identifier.Semantics {
	return &identifier.Semantics{DefaultSchema: "main", TableNames: identifier.ComparisonExact}
}

// An entry on a table, a view or a materialized view the plan drops goes with
// the object, so the plan revokes none: a table it removes, a view or a
// materialized view it removes, a materialized view it changes, and a view it
// drops to create again on a target that cannot replace one in place. A view
// replaced in place, and one the plan keeps, keep their entries, so a removed
// one is revoked there. A grant option follows the entry it belongs to.
func TestPlanCommonObjects_LeavesTheEntriesOfADroppedObjectToItsDrop(t *testing.T) {
	recentRevoke := ast.NewRevokePrivilege("reader", "TABLE", "main.recent", []string{"SELECT"})
	for _, test := range []struct {
		name    string
		inPlace bool
		diff    difftypes.SchemaDiff
		before  []ast.Node
	}{
		{name: "a removed view", diff: difftypes.SchemaDiff{ViewsRemoved: difftypes.ViewChanges{{Name: "main.recent", Body: "SELECT id FROM items"}},
			GrantsRemoved: []difftypes.GrantRef{readerOn("main.recent")}}, before: []ast.Node{ast.NewDropView("main.recent")}},
		{name: "a view dropped to create again", diff: difftypes.SchemaDiff{ViewsModified: recentQueryChange,
			GrantsRemoved: []difftypes.GrantRef{readerOn("main.recent")}}, before: []ast.Node{ast.NewDropView("main.recent")}},
		{name: "a removed table", diff: difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "main.archive"}},
			GrantsRemoved: []difftypes.GrantRef{readerOn("main.archive")}}},
		{name: "a removed materialized view", diff: difftypes.SchemaDiff{MaterializedViewsRemoved: removedTotals,
			GrantsRemoved: []difftypes.GrantRef{readerOn("main.totals")}}, before: []ast.Node{ast.NewDropMaterializedView("main.totals")}},
		{name: "a materialized view dropped to create again", diff: difftypes.SchemaDiff{MaterializedViewsModified: totalsQueryChange,
			GrantsRemoved: []difftypes.GrantRef{readerOn("main.totals")}}, before: []ast.Node{ast.NewDropMaterializedView("main.totals")}},
		{name: "a grant option on a removed view", diff: difftypes.SchemaDiff{ViewsRemoved: difftypes.ViewChanges{{Name: "main.recent", Body: "SELECT id FROM items"}},
			GrantOptionsRevoked: []difftypes.GrantRef{readerOn("main.recent")}}, before: []ast.Node{ast.NewDropView("main.recent")}},
		{name: "a view replaced in place", inPlace: true, diff: difftypes.SchemaDiff{ViewsModified: recentQueryChange,
			GrantsRemoved: []difftypes.GrantRef{readerOn("main.recent")}}, before: []ast.Node{recentRevoke}},
		{name: "a kept view", diff: difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "main.archive"}},
			GrantsRemoved: []difftypes.GrantRef{readerOn("main.recent")}}, before: []ast.Node{recentRevoke}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.diff.IdentifierSemantics = fixtureSemantics()

			plan, err := commonPlannerFor(test.inPlace).planCommonObjects(&test.diff)

			c.Assert(err, qt.IsNil)
			c.Assert(plan.before, qt.DeepEquals, test.before)
		})
	}
}

// A materialized view the plan drops to create again holds no entry once it
// is created, so the plan grants each entry the old one held again after the
// creation, as it does for a view. An entry the comparison removes is not
// granted again.
func TestPlanCommonObjects_GrantsARecreatedMaterializedViewItsEntriesAgain(t *testing.T) {
	c := qt.New(t)
	diff := difftypes.SchemaDiff{IdentifierSemantics: fixtureSemantics(), MaterializedViewsModified: totalsQueryChange,
		CurrentGrants: []difftypes.GrantRef{readerOn("main.totals"), {Role: "writer", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "main.totals"}},
		GrantsRemoved: []difftypes.GrantRef{{Role: "writer", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "main.totals"}}}

	plan, err := commonPlannerFor(false).planCommonObjects(&diff)

	c.Assert(err, qt.IsNil)
	c.Assert(plan.before, qt.DeepEquals, []ast.Node{ast.NewDropMaterializedView("main.totals")})
	c.Assert(plan.after, qt.DeepEquals, []ast.Node{
		modelast.FromMaterializedView(totalsQueryChange[0].Desired),
		grantNode(readerOn("main.totals"), false),
	})
}

// A grant the comparison adds on a view or a materialized view the plan
// removes would run after the drop and name an object that is gone, so the
// plan is refused before any step is returned.
func TestPlanCommonObjects_RefusesAGrantOnARemovedView_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name string
		diff difftypes.SchemaDiff
		want string
	}{
		{name: "a removed view", diff: difftypes.SchemaDiff{ViewsRemoved: difftypes.ViewChanges{{Name: "main.recent", Body: "SELECT id FROM items"}},
			GrantsAdded: []difftypes.GrantRef{readerOn("main.recent")}},
			want: "granting SELECT on view main.recent to reader: the plan drops the view; declare it, or leave the grant out"},
		{name: "a removed materialized view", diff: difftypes.SchemaDiff{MaterializedViewsRemoved: removedTotals,
			GrantsAdded: []difftypes.GrantRef{readerOn("main.totals")}},
			want: "granting SELECT on materialized view main.totals to reader: the plan drops the materialized view; declare it, or leave the grant out"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.diff.IdentifierSemantics = fixtureSemantics()

			plan, err := commonPlannerFor(false).planCommonObjects(&test.diff)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert([][]ast.Node{plan.before, plan.prepared, plan.after, plan.last}, qt.DeepEquals, make([][]ast.Node, 4))
		})
	}
}

// A grant the comparison adds on a view the plan drops to create again runs
// after the creation, so it is planned there rather than refused.
func TestPlanCommonObjects_GrantsOnAViewDroppedToCreateAgain(t *testing.T) {
	c := qt.New(t)
	diff := difftypes.SchemaDiff{IdentifierSemantics: fixtureSemantics(), ViewsModified: recentQueryChange,
		GrantsAdded: []difftypes.GrantRef{readerOn("main.recent")}}

	plan, err := commonPlannerFor(false).planCommonObjects(&diff)

	c.Assert(err, qt.IsNil)
	c.Assert(plan.before, qt.DeepEquals, []ast.Node{ast.NewDropView("main.recent")})
	c.Assert(plan.after, qt.DeepEquals, []ast.Node{modelast.FromView(recentQueryChange[0].Desired), grantNode(readerOn("main.recent"), false)})
}
