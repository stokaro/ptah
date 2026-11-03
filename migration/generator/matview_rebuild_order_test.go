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

// matViewOrderTable is the table both materialized views read, with the
// columns the database and the declaration give it alike.
func matViewOrderTable() ([]schemamodel.Table, []schemamodel.Field, catalog.Table) {
	tables := []schemamodel.Table{{StructName: "T", Name: "t"}}
	fields := []schemamodel.Field{
		{StructName: "T", Name: "id", Type: "integer", Primary: true},
		{StructName: "T", Name: "a", Type: "integer"},
	}
	held := catalog.Table{Name: "t", Schema: "public", Type: "BASE TABLE", Columns: []catalog.Column{
		{Name: "id", DataType: "integer", UDTName: "int4", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
		{Name: "a", DataType: "integer", UDTName: "int4", IsNullable: "NO", OrdinalPosition: 2},
	}}
	return tables, fields, held
}

// materializedViewDrops is the materialized view drops of sql, in order.
func materializedViewDrops(sql string) []string {
	return slices.DeleteFunc(sqlutil.SplitSQLStatements(sql), func(statement string) bool {
		return !strings.HasPrefix(statement, "DROP MATERIALIZED VIEW")
	})
}

// TestPlanBidirectionalSchemaDiff_DropsAMaterializedViewBeforeTheOneItsHeldBodyReads
// changes m1 and m2 together, where m2 reads m1 on one side of the change and
// t on the other. Both directions drop and create both views, and each drop
// runs while the database still holds the body m2 has before that plan: the
// one the database held for the up plan, and the one the up plan created for
// the down plan. Wherever either body reads m1, m2 is dropped first. Ordered
// by the body a plan creates alone, m1 is dropped first, and PostgreSQL
// refuses that drop with 2BP01 while m2 reads it (stokaro/ptah#5013). The
// down drops name the schema the database reported the views in.
func TestPlanBidirectionalSchemaDiff_DropsAMaterializedViewBeforeTheOneItsHeldBodyReads(t *testing.T) {
	const (
		m1Held     = "SELECT id, a FROM t"
		m1Declared = "SELECT id, a, a * 2 AS b FROM t"
		m2OfM1     = "SELECT id FROM m1"
		m2OfT      = "SELECT id FROM t"
	)
	tests := []struct {
		name               string
		m2Held, m2Declared string
		up, down           []string
	}{
		{
			name:   "the held body reads m1 and the declared one does not",
			m2Held: m2OfM1, m2Declared: m2OfT,
			up:   []string{`DROP MATERIALIZED VIEW IF EXISTS "m2" CASCADE`, `DROP MATERIALIZED VIEW IF EXISTS "m1" CASCADE`},
			down: []string{`DROP MATERIALIZED VIEW IF EXISTS "public"."m2" CASCADE`, `DROP MATERIALIZED VIEW IF EXISTS "public"."m1" CASCADE`},
		},
		{
			name:   "the declared body reads m1 and the held one does not",
			m2Held: m2OfT, m2Declared: m2OfM1,
			up:   []string{`DROP MATERIALIZED VIEW IF EXISTS "m2" CASCADE`, `DROP MATERIALIZED VIEW IF EXISTS "m1" CASCADE`},
			down: []string{`DROP MATERIALIZED VIEW IF EXISTS "public"."m2" CASCADE`, `DROP MATERIALIZED VIEW IF EXISTS "public"."m1" CASCADE`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			tables, fields, table := matViewOrderTable()
			desired := &schemamodel.Database{Tables: tables, Fields: fields, MaterializedViews: []schemamodel.MaterializedView{
				{Name: "m1", Body: m1Declared},
				{Name: "m2", Body: test.m2Declared},
			}}
			current := &catalog.Database{Tables: []catalog.Table{table}, MatViews: []catalog.MaterializedView{
				{Name: "m1", Schema: "public", Body: m1Held},
				{Name: "m2", Schema: "public", Body: test.m2Held},
			}}
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

			c.Assert(materializedViewDrops(up), qt.DeepEquals, test.up, qt.Commentf("%s", up))
			c.Assert(materializedViewDrops(down), qt.DeepEquals, test.down, qt.Commentf("%s", down))
		})
	}
}
