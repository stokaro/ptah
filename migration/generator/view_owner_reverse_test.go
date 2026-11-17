package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// TestPlanBidirectionalSchemaDiff_ChangesViewOwnersInPlaceBothWays changes the
// declared owner of a view and of a materialized view, and nothing else. Both
// directions give the objects away in place, and the rollback gives each back
// to the owner the read reported; planned as a replacement, the rollback would
// drop the materialized view and its rows.
func TestPlanBidirectionalSchemaDiff_ChangesViewOwnersInPlaceBothWays(t *testing.T) {
	c := qt.New(t)
	tables, fields, table := matViewOrderTable()
	desired := &schemamodel.Database{Tables: tables, Fields: fields,
		Views:             []schemamodel.View{{Name: "v", Body: "SELECT id FROM t", Owner: "reporting"}},
		MaterializedViews: []schemamodel.MaterializedView{{Name: "m", Body: "SELECT id, a FROM t", Owner: "Report Owner"}},
	}
	current := &catalog.Database{Tables: []catalog.Table{table},
		Views:    []catalog.View{{Name: "v", Schema: "public", Body: "SELECT id FROM t"}},
		MatViews: []catalog.MaterializedView{{Name: "m", Schema: "public", Body: "SELECT id, a FROM t"}},
		ObjectOwners: []catalog.ObjectOwner{
			{Kind: "view", Schema: "public", Name: "v", Owner: "app"},
			{Kind: "materialized view", Schema: "public", Name: "m", Owner: "app"},
		},
	}
	runtime := must.Must(builtin.New())
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, platform.Postgres, runtime))

	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: platform.Postgres,
	})
	c.Assert(err, qt.IsNil)
	caps := capability.ForDialect(platform.Postgres)
	up, err := builtin.RenderSQLWithCapabilities(platform.Postgres, caps, plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	down, err := builtin.RenderSQLWithCapabilities(platform.Postgres, caps, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(sqlutil.SplitSQLStatements(up), qt.DeepEquals, []string{
		`ALTER VIEW "v" OWNER TO "reporting"`,
		`ALTER MATERIALIZED VIEW "m" OWNER TO "Report Owner"`,
	}, qt.Commentf("%s", up))
	c.Assert(sqlutil.SplitSQLStatements(down), qt.DeepEquals, []string{
		`ALTER VIEW "public"."v" OWNER TO "app"`,
		`ALTER MATERIALIZED VIEW "public"."m" OWNER TO "app"`,
	}, qt.Commentf("%s", down))
}

// TestPlanBidirectionalSchemaDiff_RebuildsAViewWithItsOwnerBothWays changes
// the body of a materialized view that declares no owner, so the plan drops
// and creates it again in both directions. Each direction gives the view back
// to the owner the read reported, rather than leaving it with the role that
// runs the plan.
func TestPlanBidirectionalSchemaDiff_RebuildsAViewWithItsOwnerBothWays(t *testing.T) {
	c := qt.New(t)
	tables, fields, table := matViewOrderTable()
	desired := &schemamodel.Database{Tables: tables, Fields: fields,
		MaterializedViews: []schemamodel.MaterializedView{{Name: "m", Body: "SELECT id FROM t"}},
	}
	current := &catalog.Database{Tables: []catalog.Table{table},
		MatViews:      []catalog.MaterializedView{{Name: "m", Schema: "public", Body: "SELECT id, a FROM t"}},
		ObjectOwners:  []catalog.ObjectOwner{{Kind: "materialized view", Schema: "public", Name: "m", Owner: "app"}},
		ConnectedRole: "migrator",
	}
	runtime := must.Must(builtin.New())
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, platform.Postgres, runtime))

	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: platform.Postgres,
	})
	c.Assert(err, qt.IsNil)
	caps := capability.ForDialect(platform.Postgres)
	up, err := builtin.RenderSQLWithCapabilities(platform.Postgres, caps, plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	down, err := builtin.RenderSQLWithCapabilities(platform.Postgres, caps, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(sqlutil.SplitSQLStatements(up), qt.Contains, `ALTER MATERIALIZED VIEW "m" OWNER TO "app"`, qt.Commentf("%s", up))
	c.Assert(sqlutil.SplitSQLStatements(down), qt.Contains, `ALTER MATERIALIZED VIEW "public"."m" OWNER TO "app"`, qt.Commentf("%s", down))
}

// TestPlanBidirectionalSchemaDiff_RebuildsARollbackWithTheOwnerItGivesBack
// gives a view to another owner as the forward change appends a column, which
// CREATE OR REPLACE VIEW does in place. The rollback removes the column, so it
// drops the view and creates it from the pre-change database, whose read
// declares no owner: it still gives the view back to the owner the read
// reported, rather than leaving it with the one the forward change gave it.
func TestPlanBidirectionalSchemaDiff_RebuildsARollbackWithTheOwnerItGivesBack(t *testing.T) {
	c := qt.New(t)
	tables, fields, table := matViewOrderTable()
	desired := &schemamodel.Database{Tables: tables, Fields: fields,
		Views: []schemamodel.View{{Name: "v", Body: "SELECT id, a FROM t", Owner: "reporting"}},
	}
	current := &catalog.Database{Tables: []catalog.Table{table},
		Views:         []catalog.View{{Name: "v", Schema: "public", Body: "SELECT id FROM t"}},
		ObjectOwners:  []catalog.ObjectOwner{{Kind: "view", Schema: "public", Name: "v", Owner: "app"}},
		ConnectedRole: "migrator",
	}
	runtime := must.Must(builtin.New())
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, platform.Postgres, runtime))

	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: platform.Postgres,
	})
	c.Assert(err, qt.IsNil)
	caps := capability.ForDialect(platform.Postgres)
	down, err := builtin.RenderSQLWithCapabilities(platform.Postgres, caps, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)

	statements := sqlutil.SplitSQLStatements(down)
	c.Assert(statements, qt.Contains, `DROP VIEW IF EXISTS "public"."v" CASCADE`, qt.Commentf("%s", down))
	c.Assert(statements, qt.Contains, `ALTER VIEW "public"."v" OWNER TO "app"`, qt.Commentf("%s", down))
	c.Assert(statements, qt.Not(qt.Contains), `ALTER VIEW "public"."v" OWNER TO "reporting"`, qt.Commentf("%s", down))
}
