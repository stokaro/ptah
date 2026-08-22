package mysql

// White-box testing required: the refusal and the planning helpers beside it
// are package-local and have no exported API.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestRejectMaterializedViews_AsksTheCapabilityNotThePlanner holds the decision
// stokaro/ptah#1883 is about.
//
// This planner serves four engines and only one of them owns materialized
// views. Keying the refusal on the planner rather than on the capability made
// Ptah answer the same question three ways at once: the preset published a
// check mark, the renderer emitted the DDL, and this refusal told an Oracle
// user that MySQL does not support the object.
//
// The rows are one per engine on this planner rather than one per outcome,
// because what has to hold is that the refusal moved for exactly one of them.
func TestRejectMaterializedViews_AsksTheCapabilityNotThePlanner(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		wantRefusal bool
	}{
		{name: "mysql has no such object", dialect: platform.MySQL, wantRefusal: true},
		{name: "mariadb has no such object", dialect: platform.MariaDB, wantRefusal: true},
		{name: "sql server has indexed views, not these", dialect: platform.SQLServer, wantRefusal: true},
		{name: "oracle owns them", dialect: platform.Oracle},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			planner := NewForDialect(test.dialect, capability.ForDialect(test.dialect))
			err := planner.rejectMaterializedViews(&types.SchemaDiff{
				MaterializedViewsAdded: []string{"mv_sales"},
			})

			c.Assert(err != nil, qt.Equals, test.wantRefusal)
		})
	}
}

// TestRejectMaterializedViews_SaysNothingWhenTheDiffCarriesNone is the control
// the rows above would pass without: a planner that refused every call would
// satisfy three of them.
func TestRejectMaterializedViews_SaysNothingWhenTheDiffCarriesNone(t *testing.T) {
	c := qt.New(t)

	for _, dialect := range []string{platform.MySQL, platform.MariaDB, platform.SQLServer, platform.Oracle} {
		planner := NewForDialect(dialect, capability.ForDialect(dialect))
		c.Assert(planner.rejectMaterializedViews(&types.SchemaDiff{}), qt.IsNil,
			qt.Commentf("dialect %s", dialect))
	}
}

// TestMaterializedViewPlanning_EmitsTheThreeStatementsAChangeNeeds pins what
// the planner produces once the refusal is gone.
//
// A change is a drop and a create rather than a replacement, because Oracle has
// no CREATE OR REPLACE for a materialized view -- the statement is CREATE
// MATERIALIZED VIEW and nothing else.
func TestMaterializedViewPlanning_EmitsTheThreeStatementsAChangeNeeds(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		MaterializedViews: []goschema.MaterializedView{
			{Name: "order_totals", Body: "SELECT id, amount FROM orders"},
		},
	}
	planner := NewForDialect(platform.Oracle, capability.ForDialect(platform.Oracle))

	added := planner.addNewMaterializedViews(nil, &types.SchemaDiff{
		MaterializedViewsAdded: []string{"order_totals"},
	}, generated)
	c.Assert(added, qt.HasLen, 1)

	modified := planner.modifyExistingMaterializedViews(nil, &types.SchemaDiff{
		MaterializedViewsModified: []types.MaterializedViewDiff{{ViewName: "order_totals"}},
	}, generated)
	c.Assert(modified, qt.HasLen, 2)

	removed := planner.removeMaterializedViews(nil, &types.SchemaDiff{
		MaterializedViewsRemoved: []string{"order_totals"},
	})
	c.Assert(removed, qt.HasLen, 1)

	// A name the declaration does not carry produces nothing rather than a nil
	// node the renderer would have to survive.
	c.Assert(planner.addNewMaterializedViews(nil, &types.SchemaDiff{
		MaterializedViewsAdded: []string{"absent"},
	}, generated), qt.HasLen, 0)
}
