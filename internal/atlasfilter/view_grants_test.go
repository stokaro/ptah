package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasfilter"
)

// A privilege names a view as it names a table, and a reader reports a grant
// on a view as a grant on a table. A selection that keeps the view keeps the
// grant on it, on the declared side and on the read one, so a declared grant
// is planned and a held one the declaration leaves out is revoked. A view the
// selection leaves out takes its grants with it.
func TestScopeKeepsTheGrantsOfAKeptView(t *testing.T) {
	for _, test := range []struct {
		name     string
		scope    atlasfilter.Scope
		desired  *schemamodel.Database
		live     *catalog.Database
		declared int
		held     int
	}{
		{name: "a view in a selected schema", scope: atlasfilter.Scope{Selections: ownerSelections(), Schemas: []string{"shop"}},
			desired: &schemamodel.Database{Views: []schemamodel.View{{Name: "shop.recent", Body: "SELECT 1"}},
				Grants: []schemamodel.Grant{{Role: "app", OnTable: "shop.recent", Privileges: []string{"SELECT"}}}},
			live: &catalog.Database{Views: []catalog.View{{Schema: "shop", Name: "recent"}},
				Grants: []catalog.Grant{{Role: "app", ObjectType: "TABLE", Schema: "shop", ObjectName: "recent", Privilege: "SELECT"}}},
			declared: 1, held: 1},
		{name: "a materialized view in a selected schema", scope: atlasfilter.Scope{Selections: ownerSelections(), Schemas: []string{"shop"}},
			desired: &schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{Name: "shop.totals", Body: "SELECT 1"}},
				Grants: []schemamodel.Grant{{Role: "app", OnTable: "shop.totals", Privileges: []string{"SELECT"}}}},
			live: &catalog.Database{MatViews: []catalog.MaterializedView{{Schema: "shop", Name: "totals"}},
				Grants: []catalog.Grant{{Role: "app", ObjectType: "TABLE", Schema: "shop", ObjectName: "totals", Privilege: "SELECT"}}},
			declared: 1, held: 1},
		{name: "a view named without its schema", scope: atlasfilter.Scope{Selections: ownerSelections(), Schemas: []string{"main"}, DefaultSchema: "main"},
			desired: &schemamodel.Database{Views: []schemamodel.View{{Name: "recent", Body: "SELECT 1"}},
				Grants: []schemamodel.Grant{{Role: "app", OnTable: "recent", Privileges: []string{"SELECT"}}}},
			live: &catalog.Database{Views: []catalog.View{{Schema: "main", Name: "recent"}},
				Grants: []catalog.Grant{{Role: "app", ObjectType: "TABLE", Schema: "main", ObjectName: "recent", Privilege: "SELECT"}}},
			declared: 1, held: 1},
		{name: "a view in a schema the selection leaves out", scope: atlasfilter.Scope{Selections: ownerSelections(), Schemas: []string{"other"}},
			desired: &schemamodel.Database{Views: []schemamodel.View{{Name: "shop.recent", Body: "SELECT 1"}},
				Grants: []schemamodel.Grant{{Role: "app", OnTable: "shop.recent", Privileges: []string{"SELECT"}}}},
			live: &catalog.Database{Views: []catalog.View{{Schema: "shop", Name: "recent"}},
				Grants: []catalog.Grant{{Role: "app", ObjectType: "TABLE", Schema: "shop", ObjectName: "recent", Privilege: "SELECT"}}},
			declared: 0, held: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := atlasfilter.ScopeGenerated(test.desired, test.scope)
			c.Assert(err, qt.IsNil)
			read, err := atlasfilter.ScopeDatabase(test.live, test.scope)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Grants, qt.HasLen, test.declared)
			c.Assert(read.Grants, qt.HasLen, test.held)
		})
	}
}
