package generator_test

import (
	"slices"
	"strings"
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

// declaredOverT is the declaration of the table t with views and
// materialized views over it.
func declaredOverT(views []schemamodel.View, matViews []schemamodel.MaterializedView) *schemamodel.Database {
	tables, fields, _ := matViewOrderTable()
	return &schemamodel.Database{Tables: tables, Fields: fields, Views: views, MaterializedViews: matViews}
}

// heldOverT is a database holding the table t with views and materialized
// views over it.
func heldOverT(views []catalog.View, matViews []catalog.MaterializedView) *catalog.Database {
	_, _, table := matViewOrderTable()
	return &catalog.Database{Tables: []catalog.Table{table}, Views: views, MatViews: matViews}
}

// viewLikeDrops is the view and materialized view drops of sql, in order.
func viewLikeDrops(sql string) []string {
	return slices.DeleteFunc(sqlutil.SplitSQLStatements(sql), func(statement string) bool {
		return !strings.HasPrefix(statement, "DROP VIEW") && !strings.HasPrefix(statement, "DROP MATERIALIZED VIEW")
	})
}

// TestPlanBidirectionalSchemaDiff_DropsSwappedViewLikesByTheBodiesTheDatabaseHolds
// changes two views, or two materialized views, that swap which one reads the
// other: the database holds 2 over 1 and the declaration 1 over 2. Every
// column of a view moves, so CREATE OR REPLACE VIEW cannot make either change,
// and both directions drop and create both objects.
//
// The up drops run against 2 over 1 and drop 2 first; the down drops run
// against 1 over 2, which the up plan created, and drop 1 first. Ordered by
// both bodies of each object, the two read each other, and the drops keep the
// order the comparison reports the objects in, 1 before 2: PostgreSQL refuses
// the up plan's first drop with 2BP01.
func TestPlanBidirectionalSchemaDiff_DropsSwappedViewLikesByTheBodiesTheDatabaseHolds(t *testing.T) {
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		current  *catalog.Database
		up, down []string
	}{
		{
			name: "materialized views",
			desired: declaredOverT(nil, []schemamodel.MaterializedView{
				{Name: "m1", Body: "SELECT id, a FROM m2"},
				{Name: "m2", Body: "SELECT id, a FROM t"},
			}),
			current: heldOverT(nil, []catalog.MaterializedView{
				{Name: "m1", Schema: "public", Body: "SELECT id, a FROM t"},
				{Name: "m2", Schema: "public", Body: "SELECT id, a FROM m1"},
			}),
			up:   []string{`DROP MATERIALIZED VIEW IF EXISTS "m2" CASCADE`, `DROP MATERIALIZED VIEW IF EXISTS "m1" CASCADE`},
			down: []string{`DROP MATERIALIZED VIEW IF EXISTS "public"."m1" CASCADE`, `DROP MATERIALIZED VIEW IF EXISTS "public"."m2" CASCADE`},
		},
		{
			name: "views",
			desired: declaredOverT([]schemamodel.View{
				{Name: "v1", Body: "SELECT a, id FROM v2"},
				{Name: "v2", Body: "SELECT a, id FROM t"},
			}, nil),
			current: heldOverT([]catalog.View{
				{Name: "v1", Schema: "public", Body: "SELECT id, a FROM t"},
				{Name: "v2", Schema: "public", Body: "SELECT id, a FROM v1"},
			}, nil),
			up:   []string{`DROP VIEW IF EXISTS "v2" CASCADE`, `DROP VIEW IF EXISTS "v1" CASCADE`},
			down: []string{`DROP VIEW IF EXISTS "public"."v1" CASCADE`, `DROP VIEW IF EXISTS "public"."v2" CASCADE`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), test.desired, test.current, platform.Postgres, runtime))

			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: diff, DesiredSchema: test.desired, CurrentSchema: test.current, Dialect: platform.Postgres,
			})
			c.Assert(err, qt.IsNil)
			caps := capability.ForDialect(platform.Postgres)
			up, err := builtin.RenderSQLWithCapabilities(platform.Postgres, caps, plan.Forward.Nodes...)
			c.Assert(err, qt.IsNil)
			down, err := builtin.RenderSQLWithCapabilities(platform.Postgres, caps, plan.Reverse.Nodes...)
			c.Assert(err, qt.IsNil)

			c.Assert(viewLikeDrops(up), qt.DeepEquals, test.up, qt.Commentf("%s", up))
			c.Assert(viewLikeDrops(down), qt.DeepEquals, test.down, qt.Commentf("%s", down))
		})
	}
}
