package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/sqlutil"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashcl"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// TestPlanBidirectionalSchemaDiff_HCLKeepsHeldStorageParameters drives an HCL
// schema, which has no attribute for a materialized view's storage
// parameters, against a database whose materialized view holds fillfactor=70.
// An unchanged view plans nothing in either direction, rather than a RESET of
// a parameter the document could not state; a changed one is dropped and
// created again with the parameter it held, in both directions. A view's
// security block is compared as any other declaration of its options is.
func TestPlanBidirectionalSchemaDiff_HCLKeepsHeldStorageParameters(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		up, down []string
	}{
		{name: "an unchanged view", body: "SELECT id, a FROM t", up: make([]string, 0), down: make([]string, 0)},
		{
			name: "a changed view",
			body: "SELECT id FROM t",
			up: []string{
				`DROP MATERIALIZED VIEW IF EXISTS "public"."m" CASCADE`,
				"CREATE MATERIALIZED VIEW \"public\".\"m\" WITH (fillfactor=70) AS\nSELECT id FROM t",
			},
			down: []string{
				`DROP MATERIALIZED VIEW IF EXISTS "public"."m" CASCADE`,
				"CREATE MATERIALIZED VIEW \"public\".\"m\" WITH (fillfactor=70) AS\nSELECT id, a FROM t",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, err := atlashcl.Parse(t.Context(), []byte(`
schema "public" {}

table "t" {
  schema = schema.public
  column "id" {
    type = integer
  }
  column "a" {
    type = integer
  }
  primary_key {
    columns = [column.id]
  }
}

materialized "m" {
  schema = schema.public
  as     = "`+test.body+`"
}

view "v" {
  schema = schema.public
  as     = "SELECT id FROM t"
  security {
    invoker = true
  }
}
`), "schema.hcl")
			c.Assert(err, qt.IsNil)
			_, _, table := matViewOrderTable()
			current := &catalog.Database{Tables: []catalog.Table{table},
				Views:    []catalog.View{{Name: "v", Schema: "public", Body: "SELECT id FROM t", Options: []string{"security_invoker=on"}}},
				MatViews: []catalog.MaterializedView{{Name: "m", Schema: "public", Body: "SELECT id, a FROM t", Options: []string{"fillfactor=70"}}},
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

			c.Assert(sqlutil.SplitSQLStatements(up), qt.DeepEquals, test.up, qt.Commentf("%s", up))
			c.Assert(sqlutil.SplitSQLStatements(down), qt.DeepEquals, test.down, qt.Commentf("%s", down))
		})
	}
}
