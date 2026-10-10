package atlasfilter_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/featureselect"
)

// policyPredicates binds one filter predicate to each of app.orders and
// app.invoices.
func policyPredicates() []mssqlschema.Predicate {
	predicate := func(table string) mssqlschema.Predicate {
		return mssqlschema.Predicate{Type: mssqlschema.Filter, Function: mssqlschema.ObjectName{Schema: "rls", Name: "fn"},
			Arguments: []string{"tenant_id"}, Table: mssqlschema.ObjectName{Schema: "app", Name: table}}
	}
	return []mssqlschema.Predicate{predicate("orders"), predicate("invoices")}
}

func policyCoverage(representation schemaext.Representation) schemaext.Coverage {
	return must.Must(mssqlschema.Coverage(representation, schemaext.Knowledge{State: schemaext.Complete}, nil))
}

// policyDatabase is a read holding app.orders, app.invoices and app.audit, and
// one security policy binding the first two.
func policyDatabase() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Schema: "app", Name: "orders"}, {Schema: "app", Name: "invoices"}, {Schema: "app", Name: "audit"}},
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(mssqlschema.ObservedSecurityPolicyObject(mssqlschema.SecurityPolicyRef("rls", "tenancy"),
			mssqlschema.ObservedSecurityPolicy{Predicates: policyPredicates(), Enabled: true, SchemaBinding: true})))),
		FeatureCoverage: policyCoverage(schemaext.Observed),
	}
}

// policyDeclaration is the same schema as a declaration.
func policyDeclaration() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Schema: "app", Name: "orders", StructName: "O"}, {Schema: "app", Name: "invoices", StructName: "I"},
			{Schema: "app", Name: "audit", StructName: "A"}},
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(mssqlschema.DesiredSecurityPolicyObject(mssqlschema.SecurityPolicyRef("rls", "tenancy"),
			mssqlschema.DesiredSecurityPolicy{Predicates: policyPredicates()})))),
		FeatureCoverage: policyCoverage(schemaext.Desired),
	}
}

// policyBindings captures the tables the policy binds on both sides, as a
// comparison does before it scopes them.
func policyBindings(c *qt.C) featureselect.Bindings {
	c.Helper()
	bindings, err := featureselect.CaptureBindings(context.Background(), must.Must(builtin.New()), "sqlserver", identifier.ForDialect("sqlserver"),
		featureselect.Side{Representation: schemaext.Observed, Objects: policyDatabase().FeatureObjects, Coverage: policyDatabase().FeatureCoverage},
		featureselect.Side{Representation: schemaext.Desired, Objects: policyDeclaration().FeatureObjects, Coverage: policyDeclaration().FeatureCoverage})
	c.Assert(err, qt.IsNil)
	return bindings
}

// TestScopeKeepsAPolicyByTheTablesItBinds pins how a scope decides a
// security policy, which binds tables without belonging to one: kept whole
// when every table it binds is in scope, left out when none is, on the read
// and on the declaration alike, through an include, an exclusion and a schema
// selection.
func TestScopeKeepsAPolicyByTheTablesItBinds(t *testing.T) {
	tests := []struct {
		name         string
		scope        atlasfilter.Scope
		wantPolicies int
	}{
		{name: "an include holding every bound table", scope: atlasfilter.Scope{Include: []string{"app.orders", "app.invoices"}}, wantPolicies: 1},
		{name: "an include holding no bound table", scope: atlasfilter.Scope{Include: []string{"app.audit"}}},
		{name: "an exclusion of an unbound table", scope: atlasfilter.Scope{Exclude: []string{"app.audit"}}, wantPolicies: 1},
		{name: "an exclusion of every bound table", scope: atlasfilter.Scope{Exclude: []string{"app.orders", "app.invoices"}}},
		{name: "the schema of the bound tables", scope: atlasfilter.Scope{Schemas: []string{"app"}}, wantPolicies: 1},
		{name: "another schema", scope: atlasfilter.Scope{Schemas: []string{"billing"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope := test.scope
			scope.Bindings = policyBindings(c)

			read, readErr := atlasfilter.ScopeDatabase(policyDatabase(), scope)
			declaration, declarationErr := atlasfilter.ScopeGenerated(policyDeclaration(), scope)

			c.Assert(readErr, qt.IsNil)
			c.Assert(declarationErr, qt.IsNil)
			c.Assert(read.FeatureObjects.Len(), qt.Equals, test.wantPolicies)
			c.Assert(declaration.FeatureObjects.Len(), qt.Equals, test.wantPolicies)
		})
	}
}

// TestScopeRefusesToSplitAPolicy pins the refusal of a scope that selects one
// of the tables a security policy binds, on both sides and through both
// selector kinds, naming the tables on each side of the scope.
func TestScopeRefusesToSplitAPolicy(t *testing.T) {
	const wantErr = `.*ptah.run/mssql/security-policy rls.tenancy binds app.orders, which the scope selects, and app.invoices, which it does not.*`
	tests := []struct {
		name  string
		scope atlasfilter.Scope
	}{
		{name: "an include of one bound table", scope: atlasfilter.Scope{Include: []string{"app.orders"}}},
		{name: "an exclusion of one bound table", scope: atlasfilter.Scope{Exclude: []string{"app.invoices"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope := test.scope
			scope.Bindings = policyBindings(c)

			read, readErr := atlasfilter.ScopeDatabase(policyDatabase(), scope)
			declaration, declarationErr := atlasfilter.ScopeGenerated(policyDeclaration(), scope)

			c.Assert(readErr, qt.ErrorIs, featureselect.ErrPartialScope)
			c.Assert(readErr, qt.ErrorMatches, wantErr)
			c.Assert(declarationErr, qt.ErrorIs, featureselect.ErrPartialScope)
			c.Assert(declarationErr, qt.ErrorMatches, wantErr)
			c.Assert(read, qt.IsNil)
			c.Assert(declaration, qt.IsNil)
		})
	}
}

// TestScopeLeavesANamedAggregateToItsSelector pins the control for kinds a
// scope selects by name: a TimescaleDB continuous aggregate an include names
// stays in scope though the hypertable it reads does not, as it did before
// bindings were captured, and one the include does not name is left out.
func TestScopeLeavesANamedAggregateToItsSelector(t *testing.T) {
	tests := []struct {
		name           string
		include        []string
		wantAggregates int
	}{
		{name: "the aggregate named", include: []string{"app.hourly"}, wantAggregates: 1},
		{name: "another object named", include: []string{"app.audit"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			read := &catalog.Database{
				Tables: []catalog.Table{{Schema: "app", Name: "readings"}, {Schema: "app", Name: "audit"}},
				FeatureObjects: must.Must(schemaext.NewObjects(tsschema.ObservedContinuousAggregateObject("app", "hourly",
					tsschema.ObservedContinuousAggregate{Definition: "SELECT 1", HypertableSchema: "app", HypertableName: "readings", MaterializedOnly: new(true)}))),
				FeatureCoverage: must.Must(tsschema.CompleteCoverage(schemaext.Observed)),
			}
			bindings, err := featureselect.CaptureBindings(context.Background(), must.Must(builtin.New()), "postgres", identifier.ForDialect("postgres"),
				featureselect.Side{Representation: schemaext.Observed, Objects: read.FeatureObjects, Coverage: read.FeatureCoverage})
			c.Assert(err, qt.IsNil)

			got, err := atlasfilter.ScopeDatabase(read, atlasfilter.Scope{Include: test.include, Bindings: bindings})

			c.Assert(err, qt.IsNil)
			c.Assert(got.FeatureObjects.Len(), qt.Equals, test.wantAggregates)
		})
	}
}
