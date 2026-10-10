package builtin_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgpolicy"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// The row-security owner is registered, but nothing in this repository's
// sources or readers produces its models yet, so these tests build them
// themselves and drive them through the bundled runtime the commands use.

// ordersWithPolicies declares table orders with its switches on, a function
// that reads the table, which the render therefore creates after it, a view
// over the table, and two policies: one calls the function and carries a
// comment, the other is restrictive.
func ordersWithPolicies() *schemamodel.Database {
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders",
			Facets: must.Must(schemaext.NewFacets(&pgpolicy.DesiredTableState{Enabled: true, Forced: true}))}},
		Fields: []schemamodel.Field{
			{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Order", Name: "tenant_id", Type: "INTEGER"},
		},
		Functions: []schemamodel.Function{{Name: "newest_tenant", Returns: "INTEGER", Language: "sql", Volatility: "STABLE",
			Body: "SELECT max(tenant_id) FROM orders"}},
		Views: []schemamodel.View{{StructName: "RecentOrder", Name: "recent_orders", Body: "SELECT id FROM orders"}},
		FeatureObjects: must.Must(schemaext.NewObjects(
			must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("", "orders", "newest"), pgpolicy.DesiredPolicy{
				Command: pgpolicy.CommandSelect, Roles: []pgpolicy.RoleSelector{{Name: "reader"}}, Using: new("tenant_id = newest_tenant()"),
				Comment: "the newest tenant's rows"})),
			must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("", "orders", "positive"), pgpolicy.DesiredPolicy{
				Composition: pgpolicy.Restrictive, Using: new("tenant_id > 0")})),
		)),
		FeatureCoverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Desired)),
	}
	schemamodel.Finalize(database)
	return database
}

// TestPostgresCreatedTableCreatesItsPolicies pins a table declared with
// policies, on the render surface and on the plan surface: its CREATE TABLE
// carries no policy, its switches follow it, and each policy is created after
// the function and the view the render creates, with its comment after it.
func TestPostgresCreatedTableCreatesItsPolicies(t *testing.T) {
	c := qt.New(t)
	declared := ordersWithPolicies()
	runtime := must.Must(builtin.New())

	rendered, err := builtin.GetOrderedCreateStatements(declared, platform.Postgres)
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, &catalog.Database{}, catalog.ServerInfo{Dialect: platform.Postgres}, nil, runtime)
	c.Assert(err, qt.IsNil)
	planned, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, platform.Postgres, planner.Options{})
	c.Assert(err, qt.IsNil)

	for _, statements := range [][]string{rendered, planned} {
		script := strings.Join(statements, "\n")
		position := func(text string) int {
			c.Helper()
			index := strings.Index(script, text)
			c.Assert(index >= 0, qt.IsTrue, qt.Commentf("%q is missing from\n%s", text, script))
			return index
		}
		view := position(`CREATE VIEW "recent_orders"`)
		order := []int{
			position(`CREATE TABLE "orders" (`),
			position(`ALTER TABLE "orders" ENABLE ROW LEVEL SECURITY`),
			position(`ALTER TABLE "orders" FORCE ROW LEVEL SECURITY`),
			position(`CREATE FUNCTION "newest_tenant"()`),
			view,
			position("CREATE POLICY \"newest\" ON \"orders\" FOR SELECT TO \"reader\"\n    USING (tenant_id = newest_tenant())"),
			position(`COMMENT ON POLICY "newest" ON "orders" IS 'the newest tenant''s rows'`),
		}
		c.Assert(slices.IsSorted(order), qt.IsTrue, qt.Commentf("%v\n%s", order, script))
		c.Assert(position("CREATE POLICY \"positive\" ON \"orders\" AS RESTRICTIVE\n    USING (tenant_id > 0)") > view, qt.IsTrue, qt.Commentf("%s", script))
		c.Assert(strings.Count(script, "POLICY"), qt.Equals, 3, qt.Commentf("the CREATE TABLE carries no policy:\n%s", script))
	}
}

// TestPostgresFamilyRendersRowSecurity pins every target the row-security owner
// is registered for: a declared policy and the table's switches render after
// the table.
func TestPostgresFamilyRendersRowSecurity(t *testing.T) {
	for _, target := range []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(ordersWithPolicies(), target)

			c.Assert(err, qt.IsNil)
			script := strings.Join(statements, "\n")
			c.Assert(script, qt.Contains, `ALTER TABLE "orders" ENABLE ROW LEVEL SECURITY`)
			c.Assert(script, qt.Contains, `CREATE POLICY "positive" ON "orders" AS RESTRICTIVE`)
		})
	}
}

// TestSpannerRefusesRowSecurity pins Spanner, which speaks the PostgreSQL
// dialect without row security: a declared policy and declared switches are
// refused by name rather than rendered or compared.
func TestSpannerRefusesRowSecurity(t *testing.T) {
	policyOnly := ordersWithPolicies()
	policyOnly.Tables[0].Facets = schemaext.Facets{}
	switchesOnly := ordersWithPolicies()
	switchesOnly.FeatureObjects = schemaext.Objects{}
	tests := []struct {
		name     string
		database *schemamodel.Database
		want     string
	}{
		{name: "a policy", database: policyOnly, want: `unsupported feature: feature objects are not registered for target "spanner"`},
		{name: "the switches", database: switchesOnly, want: `.*Spanner table facet "ptah.run/pgpolicy/table-state" is not supported.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())

			statements, err := builtin.GetOrderedCreateStatements(test.database, platform.Spanner)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(statements, qt.IsNil)
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), test.database, &catalog.Database{}, catalog.ServerInfo{Dialect: platform.Spanner}, nil, runtime)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestPostgresRefusesAPolicyWithoutATable pins that a declared policy whose
// reference names no table is the author's error, refused while the schema is
// validated, rather than a failure of the owner asked to plan it.
func TestPostgresRefusesAPolicyWithoutATable(t *testing.T) {
	c := qt.New(t)
	database := ordersWithPolicies()
	ref := pgpolicy.PolicyRef("", "orders", "tableless")
	ref.Parent = objectidentity.Part{}
	database.FeatureObjects = must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &pgpolicy.DesiredPolicy{}}))

	statements, err := builtin.GetOrderedCreateStatements(database, platform.Postgres)

	c.Assert(err, qt.ErrorAs, new(*renderer.SchemaRefusalError))
	c.Assert(err, qt.ErrorMatches, `invalid feature value: a row-security policy requires a schema, a table and a name`)
	c.Assert(statements, qt.IsNil)
}
