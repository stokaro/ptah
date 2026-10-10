package mssqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
	"ptah.run/dialect/mssql/mssqlschema"
)

// TestCompareArgument pins the offline reading of a declared argument against
// the spelling SQL Server stores. The observed spellings in the agreeing rows
// are what sys.security_predicates reported for the declared ones on SQL
// Server 2025.
func TestCompareArgument(t *testing.T) {
	tests := []struct {
		name               string
		declared, observed string
		want               mssqlschema.Agreement
	}{
		{name: "a bracketed column", declared: "tenant_id", observed: "[tenant_id]", want: mssqlschema.Agree},
		{name: "a parenthesized literal", declared: "1", observed: "(1)", want: mssqlschema.Agree},
		{name: "a parenthesized column", declared: "(tenant_id)", observed: "[tenant_id]", want: mssqlschema.Agree},
		{name: "a negated column", declared: "-tenant_id", observed: " -[tenant_id]", want: mssqlschema.Agree},
		{name: "an expression the server only respaced", declared: "tenant_id + 0", observed: "[tenant_id]+(0)", want: mssqlschema.Agree},
		{name: "a national string", declared: "N'a b'", observed: "N'a b'", want: mssqlschema.Agree},
		{name: "a column with a space", declared: "[Owner Id]", observed: "[Owner Id]", want: mssqlschema.Agree},
		{name: "a quoted identifier", declared: `"tenant_id"`, observed: "[tenant_id]", want: mssqlschema.Agree},
		{name: "a doubled bracket", declared: "[a]]b]", observed: `"a]b"`, want: mssqlschema.Agree},
		{name: "equal text the reader cannot tokenize", declared: "x -- note", observed: "x -- note", want: mssqlschema.Agree},
		{name: "another column", declared: "tenant_id", observed: "[owner_id]", want: mssqlschema.Differ},
		{name: "a column in another case", declared: "TENANT_ID", observed: "[tenant_id]", want: mssqlschema.Differ},
		{name: "another string", declared: "'x'", observed: "'y'", want: mssqlschema.Differ},
		{name: "another integer", declared: "1", observed: "(2)", want: mssqlschema.Differ},
		{name: "a string of another type", declared: "'x'", observed: "N'x'", want: mssqlschema.Differ},
		{name: "a keyword and a column of its name", declared: "NULL", observed: "[NULL]", want: mssqlschema.Differ},
		{name: "a rewritten cast", declared: "CAST(tenant AS int) + 0", observed: "CONVERT([int],[tenant])+(0)", want: mssqlschema.Undecided},
		{name: "a float the server respells", declared: "1e3", observed: "(1.000000000000000e+003)", want: mssqlschema.Undecided},
		{name: "an integer with a leading zero", declared: "01", observed: "(1)", want: mssqlschema.Undecided},
		{name: "an expression in parentheses", declared: "(tenant_id + 1)", observed: "[tenant_id]+(1)", want: mssqlschema.Agree},
		{name: "a comment", declared: "tenant_id -- note", observed: "[tenant_id]", want: mssqlschema.Undecided},
		{name: "an unterminated string", declared: "'x", observed: "'x'", want: mssqlschema.Undecided},
		{name: "a call keeps its argument list", declared: "f(1)", observed: "[f] 1", want: mssqlschema.Undecided},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(mssqlschema.CompareArgument(test.declared, test.observed), qt.Equals, test.want)
		})
	}
}

// TestArgumentColumn pins which arguments name one column: an identifier in
// any quoting, and neither a literal nor an expression nor a keyword.
func TestArgumentColumn(t *testing.T) {
	tests := []struct {
		argument   string
		wantColumn string
		wantSimple bool
	}{
		{argument: "tenant_id", wantColumn: "tenant_id", wantSimple: true},
		{argument: "[Owner Id]", wantColumn: "Owner Id", wantSimple: true},
		{argument: "([tenant_id])", wantColumn: "tenant_id", wantSimple: true},
		{argument: "'x'", wantSimple: true},
		{argument: "(1)", wantSimple: true},
		{argument: "NULL"},
		{argument: "tenant_id + 1"},
		{argument: "dbo.f(tenant_id)"},
	}
	for _, test := range tests {
		t.Run(test.argument, func(t *testing.T) {
			c := qt.New(t)
			column, ok := mssqlschema.ArgumentColumn(test.argument)
			c.Assert(column, qt.Equals, test.wantColumn)
			c.Assert(ok, qt.Equals, test.wantColumn != "")
			c.Assert(mssqlschema.SimpleArgument(test.argument), qt.Equals, test.wantSimple)
		})
	}
}

// TestComparePolicy pins how a declaration and an observation compare: the
// resolved state, schema binding and replication, every slot paired under
// the identifier rules, and each invocation by its arguments.
func TestComparePolicy(t *testing.T) {
	semantics := identifier.ForDialect("sqlserver")
	observed := mssqlschema.ObservedSecurityPolicy{Predicates: []mssqlschema.Predicate{
		filter(orders, "[tenant_id]"), block(orders, mssqlschema.AfterInsert, "CONVERT([int],[tenant])+(0)"),
	}, Enabled: true, SchemaBinding: true}
	tests := []struct {
		name     string
		declared mssqlschema.DesiredSecurityPolicy
		want     mssqlschema.Agreement
	}{
		{name: "the defaults and the server's spelling", want: mssqlschema.Agree, declared: mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{block(orders, mssqlschema.AfterInsert, "CONVERT([int],[tenant])+(0)"), filter(orders, "tenant_id")}}},
		{name: "a rewritten argument", want: mssqlschema.Undecided, declared: mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id"), block(orders, mssqlschema.AfterInsert, "CAST(tenant AS int) + 0")}}},
		{name: "a rewritten argument beside another column", want: mssqlschema.Differ, declared: mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{filter(orders, "owner_id"), block(orders, mssqlschema.AfterInsert, "CAST(tenant AS int) + 0")}}},
		{name: "disabled", want: mssqlschema.Differ, declared: mssqlschema.DesiredSecurityPolicy{Enabled: &off,
			Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id"), block(orders, mssqlschema.AfterInsert, "CONVERT([int],[tenant])+(0)")}}},
		{name: "without schema binding", want: mssqlschema.Differ, declared: mssqlschema.DesiredSecurityPolicy{SchemaBinding: &off,
			Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id"), block(orders, mssqlschema.AfterInsert, "CONVERT([int],[tenant])+(0)")}}},
		{name: "left out of replication", want: mssqlschema.Differ, declared: mssqlschema.DesiredSecurityPolicy{NotForReplication: true,
			Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id"), block(orders, mssqlschema.AfterInsert, "CONVERT([int],[tenant])+(0)")}}},
		{name: "another operation", want: mssqlschema.Differ, declared: mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id"), block(orders, mssqlschema.BeforeDelete, "CONVERT([int],[tenant])+(0)")}}},
		{name: "a predicate fewer", want: mssqlschema.Differ, declared: mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id")}}},
		{name: "a rewritten argument the server spelled", want: mssqlschema.Agree, declared: mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id"), block(orders, mssqlschema.AfterInsert, "CAST(tenant AS int) + 0")},
			Normalized: []mssqlschema.Predicate{filter(orders, "[tenant_id]"), block(orders, mssqlschema.AfterInsert, "CONVERT([int],[tenant])+(0)")}}},
		{name: "a changed argument the server spelled", want: mssqlschema.Differ, declared: mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id"), block(orders, mssqlschema.AfterInsert, "CAST(tenant AS int) + 1")},
			Normalized: []mssqlschema.Predicate{filter(orders, "[tenant_id]"), block(orders, mssqlschema.AfterInsert, "CONVERT([int],[tenant])+(1)")}}},
		{name: "another function", want: mssqlschema.Differ, declared: mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{
				{Type: mssqlschema.Filter, Function: mssqlschema.ObjectName{Schema: "rls", Name: "fn_other"}, Arguments: []string{"tenant_id"}, Table: orders},
				block(orders, mssqlschema.AfterInsert, "CONVERT([int],[tenant])+(0)")}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, reason := mssqlschema.ComparePolicy(semantics, &test.declared, &observed)
			c.Assert(got, qt.Equals, test.want)
			c.Assert(reason != "", qt.Equals, test.want == mssqlschema.Undecided)
		})
	}
}

// TestComparePolicy_PairsTablesUnderTheCollation pins that a slot pairs by
// the identifier rules the comparison is given: offline the case of a table
// name counts, and under a resolved case-insensitive collation it does not.
func TestComparePolicy_PairsTablesUnderTheCollation(t *testing.T) {
	c := qt.New(t)
	declared := &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
		filter(mssqlschema.ObjectName{Schema: "APP", Name: "Orders"}, "tenant_id")}}
	observed := &mssqlschema.ObservedSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders, "[tenant_id]")}, Enabled: true, SchemaBinding: true}
	resolved := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").WithResolvedNames([]identifier.ResolvedName{
		{Name: "APP", Key: "app"}, {Name: "app", Key: "app"}, {Name: "Orders", Key: "orders"}, {Name: "orders", Key: "orders"},
	})

	offline, _ := mssqlschema.ComparePolicy(identifier.ForDialect("sqlserver"), declared, observed)
	folded, _ := mssqlschema.ComparePolicy(resolved, declared, observed)

	c.Assert(offline, qt.Equals, mssqlschema.Differ)
	c.Assert(folded, qt.Equals, mssqlschema.Agree)
}

// TestProjections pins the conversions between representations: a
// declaration predicts its defaults resolved, and an observation declares
// every value it holds, so converting back is the same policy.
func TestProjections(t *testing.T) {
	c := qt.New(t)
	declared := &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id")}, NotForReplication: true}

	observed, err := declared.Observed()
	c.Assert(err, qt.IsNil)
	back, err := observed.Desired()
	c.Assert(err, qt.IsNil)

	c.Assert(observed, qt.DeepEquals, &mssqlschema.ObservedSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id")},
		Enabled: true, SchemaBinding: true, NotForReplication: true})
	c.Assert(back, qt.DeepEquals, &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id")},
		Enabled: &on, SchemaBinding: &on, NotForReplication: true})
}

// TestEnabledTableConflicts pins the rule behind Msg 33264: a table that two
// enabled policies bind, matched across case and trailing spaces, and a
// disabled policy binding nothing for it.
func TestEnabledTableConflicts(t *testing.T) {
	c := qt.New(t)
	first, second, third := mssqlschema.ObjectName{Schema: "rls", Name: "a"}, mssqlschema.ObjectName{Schema: "rls", Name: "b"}, mssqlschema.ObjectName{Schema: "rls", Name: "c"}

	conflicts := mssqlschema.EnabledTableConflicts([]mssqlschema.NamedPolicy{
		{Name: first, Policy: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders), filter(invoices)}}},
		{Name: second, Policy: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
			block(mssqlschema.ObjectName{Schema: "APP", Name: "orders  "}, mssqlschema.AfterInsert)}}},
		{Name: third, Policy: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(invoices)}, Enabled: &off}},
	})

	c.Assert(conflicts, qt.DeepEquals, []mssqlschema.TableConflict{
		{Table: mssqlschema.ObjectName{Schema: "APP", Name: "orders  "}, First: first, Second: second},
	})
}
