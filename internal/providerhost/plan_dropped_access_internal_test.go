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
	"ptah.run/core/schemamodel"
)

// readerOn is the reader role's SELECT on object, a table or a view.
func readerOn(object string) difftypes.GrantRef {
	return difftypes.GrantRef{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: object}
}

// recentQueryChange changes the query of the view main.recent.
var recentQueryChange = []difftypes.ViewDiff{{ViewName: "main.recent", PreviousBody: "SELECT id FROM items",
	Desired: schemamodel.View{Name: "main.recent", Body: "SELECT id FROM items WHERE id > 0"}}}

// An entry on a table or a view the plan drops goes with the object, so the
// plan revokes none: a table it removes, a view it removes, and a view it
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
		{name: "a grant option on a removed view", diff: difftypes.SchemaDiff{ViewsRemoved: difftypes.ViewChanges{{Name: "main.recent", Body: "SELECT id FROM items"}},
			GrantOptionsRevoked: []difftypes.GrantRef{readerOn("main.recent")}}, before: []ast.Node{ast.NewDropView("main.recent")}},
		{name: "a view replaced in place", inPlace: true, diff: difftypes.SchemaDiff{ViewsModified: recentQueryChange,
			GrantsRemoved: []difftypes.GrantRef{readerOn("main.recent")}}, before: []ast.Node{recentRevoke}},
		{name: "a kept view", diff: difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "main.archive"}},
			GrantsRemoved: []difftypes.GrantRef{readerOn("main.recent")}}, before: []ast.Node{recentRevoke}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.diff.IdentifierSemantics = &identifier.Semantics{DefaultSchema: "main", TableNames: identifier.ComparisonExact}
			planner := commonPlanner{target: "fixture", commonObjects: true,
				options: planext.Options{Capabilities: capability.Capabilities{}.With(capability.CreateOrReplaceView, test.inPlace)}}

			plan, err := planner.planCommonObjects(&test.diff)

			c.Assert(err, qt.IsNil)
			c.Assert(plan.before, qt.DeepEquals, test.before)
		})
	}
}
