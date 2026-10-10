package atlasschema

// White-box testing required: no source declares or reads a SQL Server
// security policy yet, so a policy reaches these scopes only through a state
// a test builds. The exported commands take URLs, which cannot carry one.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/atlassource"
	"ptah.run/internal/featureselect"
)

const splitPolicyErr = `(?s).*ptah.run/mssql/security-policy rls.tenancy binds dbo.orders, which the scope selects, and dbo.invoices, which it does not.*`

func boundPredicates() []mssqlschema.Predicate {
	predicate := func(table string) mssqlschema.Predicate {
		return mssqlschema.Predicate{Type: mssqlschema.Filter, Function: mssqlschema.ObjectName{Schema: "rls", Name: "fn"},
			Arguments: []string{"tenant_id"}, Table: mssqlschema.ObjectName{Schema: "dbo", Name: table}}
	}
	return []mssqlschema.Predicate{predicate("orders"), predicate("invoices")}
}

// boundRead is a SQL Server read of dbo.orders, dbo.invoices and dbo.audit
// with one enabled security policy binding the first two.
func boundRead() *catalog.Database {
	tables := make([]catalog.Table, 0, 3)
	for _, name := range []string{"orders", "invoices", "audit"} {
		tables = append(tables, catalog.Table{Schema: "dbo", Name: name, Columns: []catalog.Column{{Name: "tenant_id", DataType: "int", IsNullable: "YES"}}})
	}
	return &catalog.Database{
		Tables: tables,
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(mssqlschema.ObservedSecurityPolicyObject(mssqlschema.SecurityPolicyRef("rls", "tenancy"),
			mssqlschema.ObservedSecurityPolicy{Predicates: boundPredicates(), Enabled: true, SchemaBinding: true})))),
		FeatureCoverage: must.Must(mssqlschema.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
}

// boundDeclaration declares the same tables and policy.
func boundDeclaration() *schemamodel.Database {
	declared := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(mssqlschema.DesiredSecurityPolicyObject(mssqlschema.SecurityPolicyRef("rls", "tenancy"),
			mssqlschema.DesiredSecurityPolicy{Predicates: boundPredicates()})))),
		FeatureCoverage: must.Must(mssqlschema.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	for _, name := range []string{"orders", "invoices", "audit"} {
		declared.Tables = append(declared.Tables, schemamodel.Table{Schema: "dbo", Name: name, StructName: name})
		declared.Fields = append(declared.Fields, schemamodel.Field{StructName: name, FieldName: "TenantID", Name: "tenant_id", Type: "INT", Nullable: true})
	}
	return declared
}

// TestDiffResolvedStates_ScopesAPolicyWhole pins that a scoped diff keeps a
// security policy only with every table it binds and refuses a scope that
// would split it, through an include and an exclusion.
func TestDiffResolvedStates_ScopesAPolicyWhole(t *testing.T) {
	read := atlassource.State{DB: boundRead(), DefaultSchema: "dbo"}
	declared := atlassource.State{Schema: boundDeclaration(), DefaultSchema: "dbo"}
	tests := []struct {
		name    string
		options DiffOptions
	}{
		{name: "an include of one bound table", options: DiffOptions{Include: []string{"orders"}}},
		{name: "an exclusion of one bound table", options: DiffOptions{Exclude: []string{"invoices"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			options := test.options
			options.Runtime = must.Must(builtin.New())

			report, diff, err := diffResolvedStates(t.Context(), nil, read, declared, platform.SQLServer, nil, devServerSides{}, options)

			c.Assert(err, qt.ErrorIs, featureselect.ErrPartialScope)
			c.Assert(err, qt.ErrorMatches, splitPolicyErr)
			c.Assert(report.Changes, qt.IsNil)
			c.Assert(diff, qt.IsNil)
		})
	}
	t.Run("an exclusion of an unbound table", func(t *testing.T) {
		c := qt.New(t)
		_, _, err := diffResolvedStates(t.Context(), nil, read, declared, platform.SQLServer, nil, devServerSides{},
			DiffOptions{Exclude: []string{"audit"}, Runtime: must.Must(builtin.New())})
		c.Assert(err, qt.IsNil)
	})
}

// TestScopeInspectSchema_ScopesAPolicyWhole pins the same rule for an
// inspection, which exports the read: a policy is never written with part of
// its bindings.
func TestScopeInspectSchema_ScopesAPolicyWhole(t *testing.T) {
	c := qt.New(t)
	info := catalog.ServerInfo{Dialect: platform.SQLServer, Schema: "dbo"}

	split, _, splitErr := scopeInspectSchema(t.Context(), boundRead(), info,
		InspectOptions{Exclude: []string{"invoices"}, Runtime: must.Must(builtin.New())})
	left, _, err := scopeInspectSchema(t.Context(), boundRead(), info,
		InspectOptions{Exclude: []string{"orders", "invoices"}, Runtime: must.Must(builtin.New())})

	c.Assert(splitErr, qt.ErrorIs, featureselect.ErrPartialScope)
	c.Assert(split, qt.IsNil)
	c.Assert(err, qt.IsNil)
	c.Assert(left.FeatureObjects.Len(), qt.Equals, 0)
}

// TestExcludeCurrentWithBindings_ScopesAPolicyWhole pins that the reread a
// saved plan is checked against subtracts its exclusions the way the plan was
// computed, so a policy the plan left out is left out of the reread too.
func TestExcludeCurrentWithBindings_ScopesAPolicyWhole(t *testing.T) {
	c := qt.New(t)
	info := catalog.ServerInfo{Dialect: platform.SQLServer, Schema: "dbo"}

	left, err := excludeCurrentWithBindings(t.Context(), boundRead(), []string{"orders", "invoices"}, info, must.Must(builtin.New()))
	split, splitErr := excludeCurrentWithBindings(t.Context(), boundRead(), []string{"orders"}, info, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(left.FeatureObjects.Len(), qt.Equals, 0)
	c.Assert(splitErr, qt.ErrorIs, featureselect.ErrPartialScope)
	c.Assert(split, qt.IsNil)
}

// TestSelectApplyStates_ScopesAPolicyWhole pins the rule for schema apply,
// which carries a plan out: a split policy is refused before planning, and an
// exclusion of every table it binds leaves it out of both sides.
func TestSelectApplyStates_ScopesAPolicyWhole(t *testing.T) {
	c := qt.New(t)
	options := ApplyOptions{Runtime: must.Must(builtin.New())}

	_, _, splitErr := selectApplyStates(t.Context(), platform.SQLServer, boundRead(), boundDeclaration(),
		atlasfilter.Scope{Exclude: []string{"invoices"}, DefaultSchema: "dbo"}, options)
	current, desired, err := selectApplyStates(t.Context(), platform.SQLServer, boundRead(), boundDeclaration(),
		atlasfilter.Scope{Exclude: []string{"orders", "invoices"}, DefaultSchema: "dbo"}, options)

	c.Assert(splitErr, qt.ErrorIs, featureselect.ErrPartialScope)
	c.Assert(splitErr, qt.ErrorMatches, splitPolicyErr)
	c.Assert(err, qt.IsNil)
	c.Assert(current.FeatureObjects.Len(), qt.Equals, 0)
	c.Assert(desired.FeatureObjects.Len(), qt.Equals, 0)
}
