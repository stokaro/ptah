package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

func boundFilter(table string) mssqlschema.Predicate {
	return mssqlschema.Predicate{Type: mssqlschema.Filter, Function: mssqlschema.ObjectName{Schema: "rls", Name: "fn"},
		Arguments: []string{"tenant_id"}, Table: mssqlschema.ObjectName{Schema: "dbo", Name: table}}
}

// boundLive is a SQL Server database holding dbo.orders and dbo.invoices,
// each with tenant_id and note, and one security policy binding both.
func boundLive() *catalog.Database {
	db := &catalog.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(mssqlschema.ObservedSecurityPolicyObject(mssqlschema.SecurityPolicyRef("rls", "tenancy"),
			mssqlschema.ObservedSecurityPolicy{Predicates: []mssqlschema.Predicate{boundFilter("orders"), boundFilter("invoices")}, Enabled: true, SchemaBinding: true})))),
		FeatureCoverage: must.Must(mssqlschema.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	for _, name := range []string{"orders", "invoices"} {
		db.Tables = append(db.Tables, catalog.Table{Schema: "dbo", Name: name, Columns: []catalog.Column{
			{Name: "tenant_id", DataType: "int", IsNullable: "YES", OrdinalPosition: 1},
			{Name: "note", DataType: "int", IsNullable: "YES", OrdinalPosition: 2},
		}})
	}
	return db
}

// boundDesired declares the tables named, each with the columns named, and
// the policy binding the predicates given, or no policy at all when the
// description cannot express one.
func boundDesired(tables, columns []string, policy *mssqlschema.DesiredSecurityPolicy) *schemamodel.Database {
	db := &schemamodel.Database{}
	for _, name := range tables {
		db.Tables = append(db.Tables, schemamodel.Table{Schema: "dbo", Name: name, StructName: name})
		for _, column := range columns {
			db.Fields = append(db.Fields, schemamodel.Field{StructName: name, FieldName: column, Name: column, Type: "INT", Nullable: true})
		}
	}
	if policy == nil {
		db.FeatureCoverage = must.Must(mssqlschema.Coverage(schemaext.Desired,
			schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the description cannot express security policies"}, nil))
		return db
	}
	db.FeatureObjects = must.Must(schemaext.NewObjects(must.Must(mssqlschema.DesiredSecurityPolicyObject(mssqlschema.SecurityPolicyRef("rls", "tenancy"), *policy))))
	db.FeatureCoverage = must.Must(mssqlschema.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	return db
}

// TestCompare_RefusesToDropWhatAPolicyBinds pins that a plan never drops a
// table or a column a security policy the desired state keeps still binds:
// a declared policy that still names it, and an observed one the description
// could not express, which the comparison keeps as it is. SQL Server would
// refuse the drop (Msg 3729, 5074), and dropping the policy's other bindings
// with it is not what the description asked for.
func TestCompare_RefusesToDropWhatAPolicyBinds(t *testing.T) {
	both := &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{boundFilter("orders"), boundFilter("invoices")}}
	tests := []struct {
		name    string
		desired *schemamodel.Database
		wantErr string
	}{
		{name: "a declared policy binding a dropped table", desired: boundDesired([]string{"orders"}, []string{"tenant_id", "note"}, both),
			wantErr: `(?s).*ptah.run/mssql/security-policy rls.tenancy binds table dbo.invoices, which the plan drops; change it so it no longer binds the table, or keep the table.*`},
		{name: "a kept policy binding a dropped table", desired: boundDesired([]string{"orders"}, []string{"tenant_id", "note"}, nil),
			wantErr: `(?s).*ptah.run/mssql/security-policy rls.tenancy binds table dbo.invoices, which the plan drops.*`},
		{name: "a declared policy reading a dropped column", desired: boundDesired([]string{"orders", "invoices"}, []string{"note"}, both),
			wantErr: `(?s).*ptah.run/mssql/security-policy rls.tenancy binds column dbo.(invoices|orders).tenant_id, which the plan drops; change it so it no longer binds the column, or keep the column.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), test.desired, boundLive(), catalog.ServerInfo{Dialect: "sqlserver", Schema: "dbo"}, nil, must.Must(builtin.New()))

			var refusal *schemadiff.RefusalError
			c.Assert(err, qt.ErrorAs, &refusal)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestCompare_DropsATableOnceThePolicyLetsItGo pins the supported path: the
// declaration removes the binding, so the comparison plans the policy change
// beside the table drop, and the planner runs the change first.
func TestCompare_DropsATableOnceThePolicyLetsItGo(t *testing.T) {
	c := qt.New(t)
	desired := boundDesired([]string{"orders"}, []string{"tenant_id", "note"},
		&mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{boundFilter("orders")}})

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, boundLive(), catalog.ServerInfo{Dialect: "sqlserver", Schema: "dbo"}, nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesRemoved.Names(), qt.DeepEquals, []string{"dbo.invoices"})
	c.Assert(diff.FeatureChanges, qt.HasLen, 1)
	change := diff.FeatureChanges[0].Value.(*mssqldiff.SecurityPolicy)
	c.Assert(change.After.Predicates, qt.DeepEquals, []mssqlschema.Predicate{boundFilter("orders")})
}

// TestCompare_RefusesToDropAFunctionAPolicyCalls pins the function half of
// the rule, on SQL Server, whose plans drop a function the description leaves
// out: a predicate function the kept policy calls, and any function at all
// when an argument expression may call functions the owner cannot list.
func TestCompare_RefusesToDropAFunctionAPolicyCalls(t *testing.T) {
	expression := &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
		{Type: mssqlschema.Filter, Function: mssqlschema.ObjectName{Schema: "rls", Name: "fn"}, Arguments: []string{"dbo.tenant_of(tenant_id)"},
			Table: mssqlschema.ObjectName{Schema: "dbo", Name: "orders"}},
		boundFilter("invoices"),
	}}
	tests := []struct {
		name     string
		policy   *mssqlschema.DesiredSecurityPolicy
		function string
		wantErr  string
	}{
		{name: "the predicate function", function: "fn",
			wantErr: `(?s).*ptah.run/mssql/security-policy rls.tenancy binds function rls.fn, which the plan drops; change it so it no longer binds the function, or keep the function.*`},
		{name: "a function an expression may call", policy: expression, function: "helper",
			wantErr: `(?s).*ptah.run/mssql/security-policy rls.tenancy may reference functions its owner cannot list \(the argument dbo.tenant_of\(tenant_id\) .*\), and the plan drops or replaces rls.helper.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			live := boundLive()
			live.Functions = []catalog.Function{{Schema: "rls", Name: test.function}}

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), boundDesired([]string{"orders", "invoices"}, []string{"tenant_id", "note"}, test.policy),
				live, catalog.ServerInfo{Dialect: "sqlserver", Schema: "dbo"}, nil, must.Must(builtin.New()))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestCompare_LeavesUnmanagedRoutinesAlone is the control for a server whose
// capabilities leave routines out: its plans drop no function, so a policy
// calling one the description leaves out is no reason to refuse.
func TestCompare_LeavesUnmanagedRoutinesAlone(t *testing.T) {
	c := qt.New(t)
	live := boundLive()
	live.Functions = []catalog.Function{{Schema: "rls", Name: "fn"}}
	withoutRoutines := capability.SQLServer2022().With(capability.Functions, false)

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), boundDesired([]string{"orders", "invoices"}, []string{"tenant_id", "note"}, nil),
		live, catalog.ServerInfo{Dialect: "sqlserver", Schema: "dbo", Capabilities: withoutRoutines}, nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.FunctionsRemoved.Names(), qt.DeepEquals, []string{"rls.fn"})
}

// TestCompare_RefusesToReplaceAFunctionAPolicyCalls pins that a replacement
// counts as a drop: SQL Server replaces a function by DROP FUNCTION and CREATE
// FUNCTION, and refuses the drop while a policy calls it.
func TestCompare_RefusesToReplaceAFunctionAPolicyCalls(t *testing.T) {
	c := qt.New(t)
	live := boundLive()
	live.Functions = []catalog.Function{{Schema: "rls", Name: "fn", Parameters: "@t int", Returns: "TABLE", Body: "RETURN SELECT 1 AS ok"}}
	desired := boundDesired([]string{"orders", "invoices"}, []string{"tenant_id", "note"}, nil)
	desired.Functions = []schemamodel.Function{{Name: "rls.fn", Parameters: "@t int", Returns: "TABLE", Body: "RETURN SELECT 2 AS ok"}}

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, live, catalog.ServerInfo{Dialect: "sqlserver", Schema: "dbo"}, nil, must.Must(builtin.New()))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `(?s).*security-policy rls.tenancy binds function rls.fn, which the plan replaces; change it so it no longer binds the function, or keep the function.*`)
	c.Assert(diff, qt.IsNil)
}
