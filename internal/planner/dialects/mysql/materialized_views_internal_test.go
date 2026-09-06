package mysql

// White-box testing required: the refusal and the planning helpers beside it
// are package-local and have no exported API.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
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
			err := planner.rejectMaterializedViews(&difftypes.SchemaDiff{
				MaterializedViewsAdded: difftypes.MaterializedViewChanges{{Name: "mv_sales"}},
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
		c.Assert(planner.rejectMaterializedViews(&difftypes.SchemaDiff{}), qt.IsNil,
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

	desired := &schemamodel.Database{
		MaterializedViews: []schemamodel.MaterializedView{
			{Name: "order_totals", Body: "SELECT id, amount FROM orders"},
		},
	}
	planner := NewForDialect(platform.Oracle, capability.ForDialect(platform.Oracle))

	added := planner.addNewMaterializedViews(nil, &difftypes.SchemaDiff{
		MaterializedViewsAdded: difftypes.MaterializedViewChanges{
			{Name: "order_totals", Body: "SELECT id, amount FROM orders"},
		},
	})
	c.Assert(added, qt.HasLen, 1)

	modified := planner.modifyExistingMaterializedViews(nil, &difftypes.SchemaDiff{
		MaterializedViewsModified: []difftypes.MaterializedViewDiff{{
			ViewName: "order_totals",
			Desired:  desired.MaterializedViews[0],
		}},
	})
	c.Assert(modified, qt.HasLen, 2)

	removed := planner.removeMaterializedViews(nil, &difftypes.SchemaDiff{
		MaterializedViewsRemoved: difftypes.MaterializedViewChanges{{Name: "order_totals"}},
	})
	c.Assert(removed, qt.HasLen, 1)

	// An addition no longer depends on the desired schema at all: the view
	// travels with the change, so what used to be a name the declaration did
	// not carry -- and produced nothing -- is now a view that renders in full
	// (stokaro/ptah#2315). The empty desired schema is what makes that
	// measurable rather than incidental.
	fromChangeAlone := planner.addNewMaterializedViews(nil, &difftypes.SchemaDiff{
		MaterializedViewsAdded: difftypes.MaterializedViewChanges{
			{Name: "absent_from_desired", Body: "SELECT 1"},
		},
	})
	c.Assert(fromChangeAlone, qt.HasLen, 1)
	created, ok := fromChangeAlone[0].(*ast.CreateMaterializedViewNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("node is %T", fromChangeAlone[0]))
	c.Assert(created.Name, qt.Equals, "absent_from_desired")
	c.Assert(created.Body, qt.Equals, "SELECT 1")
}
