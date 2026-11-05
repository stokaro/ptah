package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// TestPlanBidirectionalSchemaDiff_ChangesViewOptionsInPlaceBothWays changes the
// options of a view and the storage parameters of a materialized view, and
// nothing else. Both directions change them in place, and the rollback gives
// back exactly what the database held: it sets the options the up plan changed,
// and resets the ones it added. Planned as a replacement, the rollback would
// drop the materialized view and its rows; planned from the declaration alone,
// it could not know what to give back.
func TestPlanBidirectionalSchemaDiff_ChangesViewOptionsInPlaceBothWays(t *testing.T) {
	c := qt.New(t)
	tables, fields, table := matViewOrderTable()
	desired := &schemamodel.Database{Tables: tables, Fields: fields,
		Views: []schemamodel.View{{Name: "v", Body: "SELECT id FROM t", Options: []string{"security_barrier=true", "security_invoker=false"}}},
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
		`ALTER VIEW "v" SET (security_barrier=true, security_invoker=false)`,
		`ALTER MATERIALIZED VIEW "m" SET (fillfactor=80, autovacuum_enabled=false)`,
	}, qt.Commentf("%s", up))
	c.Assert(sqlutil.SplitSQLStatements(down), qt.DeepEquals, []string{
		`ALTER VIEW "public"."v" SET (security_invoker=on)`,
		`ALTER VIEW "public"."v" RESET (security_barrier)`,
		`ALTER MATERIALIZED VIEW "public"."m" SET (fillfactor=70)`,
		`ALTER MATERIALIZED VIEW "public"."m" RESET (autovacuum_enabled)`,
	}, qt.Commentf("%s", down))
}

// viewSecurityPlan plans the change from a view holding heldOptions and heldBody
// to a declaration stating declaredOptions and declaredBody, in both directions.
func viewSecurityPlan(
	c *qt.C,
	heldBody string,
	heldOptions []string,
	declaredBody string,
	declaredOptions []string,
) (*generator.BidirectionalSchemaPlan, error) {
	c.Helper()
	tables, fields, table := matViewOrderTable()
	desired := &schemamodel.Database{Tables: tables, Fields: fields,
		Views: []schemamodel.View{{Name: "v", Body: declaredBody, Options: declaredOptions}},
	}
	current := &catalog.Database{Tables: []catalog.Table{table},
		Views: []catalog.View{{Name: "v", Schema: "public", Body: heldBody, Options: heldOptions}},
	}
	runtime := must.Must(builtin.New())
	diff := must.Must(schemadiff.CompareWithDialect(c.Context(), desired, current, platform.Postgres, runtime))
	return generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: platform.Postgres,
	})
}

// TestPlanBidirectionalSchemaDiff_RollbackRemovesTheSecurityOptionItAdded pins
// that the rollback of a change that adds a security option plans. It leaves
// the option out because the view had none before the change, which is the
// earlier state rather than an omission, so the forward refusal must not read
// it. The last row gives an option back rather than removing one.
func TestPlanBidirectionalSchemaDiff_RollbackRemovesTheSecurityOptionItAdded(t *testing.T) {
	const narrow, wide = "SELECT id FROM t", "SELECT id, a FROM t"
	tests := []struct {
		name             string
		heldBody         string
		held             []string
		declaredBody     string
		declared         []string
		wantUp, wantDown []string
	}{
		{
			name: "the option added alone", heldBody: narrow, declaredBody: narrow,
			declared: []string{"security_invoker=true"},
			wantUp:   []string{`ALTER VIEW "v" SET (security_invoker=true)`},
			wantDown: []string{`ALTER VIEW "public"."v" RESET (security_invoker)`},
		},
		{
			name: "the option added with a column", heldBody: narrow, declaredBody: wide,
			declared: []string{"security_barrier=true"},
			wantUp:   []string{"CREATE OR REPLACE VIEW \"v\" WITH (security_barrier=true) AS\nSELECT id, a FROM t"},
			wantDown: []string{`DROP VIEW IF EXISTS "public"."v" CASCADE`, "CREATE VIEW \"public\".\"v\" AS\nSELECT id FROM t"},
		},
		{
			name: "an explicit false, which the rollback sets back", heldBody: narrow, declaredBody: narrow,
			held:     []string{"security_invoker=true"},
			declared: []string{"security_invoker=false"},
			wantUp:   []string{`ALTER VIEW "v" SET (security_invoker=false)`},
			wantDown: []string{`ALTER VIEW "public"."v" SET (security_invoker=true)`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			plan, err := viewSecurityPlan(c, test.heldBody, test.held, test.declaredBody, test.declared)

			c.Assert(err, qt.IsNil)
			caps := capability.ForDialect(platform.Postgres)
			up, err := builtin.RenderSQLWithCapabilities(platform.Postgres, caps, plan.Forward.Nodes...)
			c.Assert(err, qt.IsNil)
			down, err := builtin.RenderSQLWithCapabilities(platform.Postgres, caps, plan.Reverse.Nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(sqlutil.SplitSQLStatements(up), qt.DeepEquals, test.wantUp, qt.Commentf("%s", up))
			c.Assert(sqlutil.SplitSQLStatements(down), qt.DeepEquals, test.wantDown, qt.Commentf("%s", down))
		})
	}
}

// TestPlanBidirectionalSchemaDiff_RefusesToLoseAViewSecurityOption pins that the
// forward direction of a generated migration refuses as `schema apply` does, so
// `migrations generate` and ptah-compat `migrate diff` write no files.
func TestPlanBidirectionalSchemaDiff_RefusesToLoseAViewSecurityOption(t *testing.T) {
	c := qt.New(t)

	plan, err := viewSecurityPlan(c, "SELECT id FROM t", []string{"security_invoker=true"}, "SELECT id FROM t", nil)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `error planning forward migration: unsupported feature: view v holds security_invoker=true .*`)
	c.Assert(plan, qt.IsNil)
}
