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

// TestPlanBidirectionalSchemaDiff_ChangesViewOptionsInPlaceBothWays changes the
// options of a view and the storage parameters of a materialized view, and
// nothing else. Both directions change them in place, and the rollback gives
// back exactly what the database held: it sets the options the up plan reset
// or changed, and resets the ones it added. Planned as a replacement, the
// rollback would drop the materialized view and its rows; planned from the
// declaration alone, it could not know what to give back.
func TestPlanBidirectionalSchemaDiff_ChangesViewOptionsInPlaceBothWays(t *testing.T) {
	c := qt.New(t)
	tables, fields, table := matViewOrderTable()
	desired := &schemamodel.Database{Tables: tables, Fields: fields,
		Views: []schemamodel.View{{Name: "v", Body: "SELECT id FROM t", Options: []string{"security_barrier=true"}}},
		MaterializedViews: []schemamodel.MaterializedView{
			{Name: "m", Body: "SELECT id, a FROM t", Options: []string{"fillfactor=80", "autovacuum_enabled=false"}},
		},
	}
	current := &catalog.Database{Tables: []catalog.Table{table},
		Views: []catalog.View{{Name: "v", Schema: "public", Body: "SELECT id FROM t", Options: []string{"security_invoker=on"}}},
		MatViews: []catalog.MaterializedView{
			{Name: "m", Schema: "public", Body: "SELECT id, a FROM t", Options: []string{"fillfactor=70"}},
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
		`ALTER VIEW "v" SET (security_barrier=true)`,
		`ALTER VIEW "v" RESET (security_invoker)`,
		`ALTER MATERIALIZED VIEW "m" SET (fillfactor=80, autovacuum_enabled=false)`,
	}, qt.Commentf("%s", up))
	c.Assert(sqlutil.SplitSQLStatements(down), qt.DeepEquals, []string{
		`ALTER VIEW "public"."v" SET (security_invoker=on)`,
		`ALTER VIEW "public"."v" RESET (security_barrier)`,
		`ALTER MATERIALIZED VIEW "public"."m" SET (fillfactor=70)`,
		`ALTER MATERIALIZED VIEW "public"."m" RESET (autovacuum_enabled)`,
	}, qt.Commentf("%s", down))
}
