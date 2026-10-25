package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgroutine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// TestPlanBidirectionalSchemaDiff_RemovedRoutineComesBackWithItsProperties
// pins the rollback of a routine the forward plan removes: it creates the
// routine again as the database held it, with its pinned settings, STRICT,
// LEAKPROOF, PARALLEL and SECURITY, and with a SQL-standard body as SQL. A
// SECURITY DEFINER function that came back without its search_path would run
// with the caller's.
func TestPlanBidirectionalSchemaDiff_RemovedRoutineComesBackWithItsProperties(t *testing.T) {
	tests := []struct {
		name    string
		routine catalog.Function
		want    string
	}{
		{
			name: "a function with settings and properties",
			routine: catalog.Function{Name: "f", Schema: "public", Parameters: "x integer", Returns: "integer",
				Language: "sql", Security: "DEFINER", Volatility: "IMMUTABLE", Body: "SELECT x",
				Settings: []string{"search_path=pg_catalog"}, IdentityArguments: new("x integer"),
				Facets: must.Must(pgroutine.WithObservedProperties(schemaext.Facets{}, pgroutine.Properties{Strict: true, Leakproof: true, Parallel: "SAFE"}))},
			want: "CREATE FUNCTION \"public\".\"f\"(x integer) RETURNS integer AS $$\nSELECT x\n$$\n" +
				"LANGUAGE sql SECURITY DEFINER IMMUTABLE LEAKPROOF STRICT PARALLEL SAFE SET search_path = pg_catalog;",
		},
		{
			name: "a function with a SQL-standard BEGIN ATOMIC body",
			routine: catalog.Function{Name: "c", Schema: "public", Returns: "bigint", Language: "sql", Security: "INVOKER",
				Volatility: "STABLE", Body: "BEGIN ATOMIC\n SELECT count(t.a) AS count\n    FROM t;\nEND", IdentityArguments: new("")},
			want: "CREATE FUNCTION \"public\".\"c\"() RETURNS bigint LANGUAGE sql SECURITY INVOKER STABLE BEGIN ATOMIC\n" +
				" SELECT count(t.a) AS count\n    FROM t;\nEND;",
		},
		{
			name: "a function with a SQL-standard RETURN body",
			routine: catalog.Function{Name: "n", Schema: "public", Returns: "bigint", Language: "sql", Security: "INVOKER",
				Volatility: "STABLE", Body: "RETURN (SELECT count(*) AS count FROM t)", IdentityArguments: new("")},
			want: `CREATE FUNCTION "public"."n"() RETURNS bigint LANGUAGE sql SECURITY INVOKER STABLE RETURN (SELECT count(*) AS count FROM t);`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			desired := &schemamodel.Database{}
			current := &catalog.Database{Functions: []catalog.Function{test.routine}}
			diff := must.Must(schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current,
				catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public"}, nil, runtime))

			plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current,
				Dialect: platform.Postgres, Capabilities: capability.Postgres18(),
			})
			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQL(platform.Postgres, plan.Reverse.Nodes...)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}
