package mssqldiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/engine/builtin"
)

var (
	tenant   = mssqlschema.ObjectName{Schema: "rls", Name: "fn_tenant"}
	orders   = mssqlschema.ObjectName{Schema: "app", Name: "orders"}
	invoices = mssqlschema.ObjectName{Schema: "app", Name: "invoices"}
	off      = false
)

func filter(table mssqlschema.ObjectName, arguments ...string) mssqlschema.Predicate {
	return mssqlschema.Predicate{Type: mssqlschema.Filter, Function: tenant, Arguments: arguments, Table: table}
}

func block(table mssqlschema.ObjectName, operation mssqlschema.BlockOperation, arguments ...string) mssqlschema.Predicate {
	return mssqlschema.Predicate{Type: mssqlschema.Block, Function: tenant, Arguments: arguments, Table: table, Operation: operation}
}

func observed(enabled bool, predicates ...mssqlschema.Predicate) *mssqlschema.ObservedSecurityPolicy {
	return &mssqlschema.ObservedSecurityPolicy{Predicates: predicates, Enabled: enabled, SchemaBinding: true}
}

func declared(predicates ...mssqlschema.Predicate) *mssqlschema.DesiredSecurityPolicy {
	return &mssqlschema.DesiredSecurityPolicy{Predicates: predicates}
}

// TestEdit pins the statements a change needs, measured on SQL Server 2025:
// schema binding and replication are changed by dropping and creating the
// policy, a slot spelled the same on both sides is altered, a drop that frees
// a slot an addition takes under another spelling goes first in a statement
// of its own, and the state is set before the predicates when the policy is
// disabled and after them when it is enabled.
func TestEdit(t *testing.T) {
	tests := []struct {
		name           string
		change         mssqldiff.SecurityPolicy
		want           mssqldiff.Edit
		wantStatements int
	}{
		{name: "a new policy", change: mssqldiff.SecurityPolicy{After: declared(filter(orders, "tenant_id"))},
			want: mssqldiff.Edit{Create: true}, wantStatements: 1},
		{name: "a dropped policy", change: mssqldiff.SecurityPolicy{Before: observed(true, filter(orders, "[tenant_id]"))},
			want: mssqldiff.Edit{Drop: true}, wantStatements: 1},
		{name: "schema binding turned off", wantStatements: 2, want: mssqldiff.Edit{Drop: true, Create: true}, change: mssqldiff.SecurityPolicy{
			Before: observed(true, filter(orders, "[tenant_id]")),
			After:  &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id")}, SchemaBinding: &off}}},
		{name: "replication left out", wantStatements: 2, want: mssqldiff.Edit{Drop: true, Create: true}, change: mssqldiff.SecurityPolicy{
			Before: observed(true, filter(orders, "[tenant_id]")),
			After:  &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id")}, NotForReplication: true}}},
		{name: "disabled", wantStatements: 1, want: mssqldiff.Edit{Disable: true}, change: mssqldiff.SecurityPolicy{
			Before: observed(true, filter(orders, "[tenant_id]")),
			After:  &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id")}, Enabled: &off}}},
		{name: "enabled with a predicate added", wantStatements: 2, change: mssqldiff.SecurityPolicy{
			Before: observed(false, filter(orders, "[tenant_id]")), After: declared(filter(orders, "tenant_id"), filter(invoices, "tenant_id"))},
			want: mssqldiff.Edit{Enable: true, Adds: []mssqlschema.Predicate{filter(invoices, "tenant_id")}}},
		{name: "a predicate of each kind", wantStatements: 1, change: mssqldiff.SecurityPolicy{
			Before: observed(true, filter(orders, "[tenant_id]"), block(orders, mssqlschema.AfterInsert, "[tenant_id]"), block(invoices, "", "[tenant_id]")),
			After:  declared(filter(orders, "owner_id"), block(orders, mssqlschema.BeforeUpdate, "tenant_id"), block(invoices, "", "tenant_id"))},
			want: mssqldiff.Edit{Drops: []mssqlschema.Predicate{block(orders, mssqlschema.AfterInsert, "[tenant_id]")},
				Alters: []mssqlschema.Predicate{filter(orders, "owner_id")}, Adds: []mssqlschema.Predicate{block(orders, mssqlschema.BeforeUpdate, "tenant_id")}}},
		{name: "a table spelled another way", wantStatements: 2, change: mssqldiff.SecurityPolicy{
			Before: observed(true, filter(orders, "[tenant_id]")), After: declared(filter(mssqlschema.ObjectName{Schema: "APP", Name: "Orders"}, "tenant_id"))},
			want: mssqldiff.Edit{Drops: []mssqlschema.Predicate{filter(orders, "[tenant_id]")},
				Adds: []mssqlschema.Predicate{filter(mssqlschema.ObjectName{Schema: "APP", Name: "Orders"}, "tenant_id")}, SeparateDrops: true}},
		{name: "a rewritten argument re-sent with another change", wantStatements: 1, change: mssqldiff.SecurityPolicy{
			Before: observed(true, filter(orders, "CONVERT([int],[tenant])+(0)")), After: declared(filter(orders, "CAST(tenant AS int) + 0"), filter(invoices))},
			want: mssqldiff.Edit{Alters: []mssqlschema.Predicate{filter(orders, "CAST(tenant AS int) + 0")}, Adds: []mssqlschema.Predicate{filter(invoices)}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			edit := test.change.Edit()
			c.Assert(edit, qt.DeepEquals, test.want)
			c.Assert(edit.Statements(), qt.Equals, test.wantStatements)
		})
	}
}

// TestAssess pins what a change does to access: a policy that starts
// enforcing narrows it and one that stops widens it; between two enforcing
// policies a lost slot widens, a new one narrows, another invocation is
// unknown, and replication left out widens.
func TestAssess(t *testing.T) {
	semantics := identifier.ForDialect("sqlserver")
	tests := []struct {
		name   string
		before *mssqlschema.ObservedSecurityPolicy
		after  *mssqlschema.DesiredSecurityPolicy
		want   schemaext.Access
	}{
		{name: "a new policy", after: declared(filter(orders)), want: schemaext.AccessNarrows},
		{name: "a new disabled policy", after: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders)}, Enabled: &off},
			want: schemaext.AccessUnchanged},
		{name: "a new policy without predicates", after: declared(), want: schemaext.AccessUnchanged},
		{name: "a dropped policy", before: observed(true, filter(orders)), want: schemaext.AccessWidens},
		{name: "a dropped disabled policy", before: observed(false, filter(orders)), want: schemaext.AccessUnchanged},
		{name: "enabled", before: observed(false, filter(orders)), after: declared(filter(orders)), want: schemaext.AccessNarrows},
		{name: "a predicate removed", before: observed(true, filter(orders), filter(invoices)), after: declared(filter(orders)), want: schemaext.AccessWidens},
		{name: "a predicate added", before: observed(true, filter(orders)), after: declared(filter(orders), filter(invoices)), want: schemaext.AccessNarrows},
		{name: "a block for every write narrowed to one", before: observed(true, block(orders, "")), after: declared(block(orders, mssqlschema.AfterInsert)),
			want: schemaext.AccessWidens},
		{name: "another argument", before: observed(true, filter(orders, "[tenant_id]")), after: declared(filter(orders, "owner_id")), want: schemaext.AccessUnknown},
		{name: "another argument beside a removal", before: observed(true, filter(orders, "[tenant_id]"), filter(invoices)),
			after: declared(filter(orders, "owner_id")), want: schemaext.AccessUnknown},
		{name: "the same predicates", before: observed(true, filter(orders, "[tenant_id]")), after: declared(filter(orders, "tenant_id")),
			want: schemaext.AccessUnchanged},
		{name: "replication left out", before: observed(true, filter(orders)),
			after: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders)}, NotForReplication: true}, want: schemaext.AccessWidens},
		{name: "replication checked again", before: &mssqlschema.ObservedSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders)},
			Enabled: true, SchemaBinding: true, NotForReplication: true}, after: declared(filter(orders)), want: schemaext.AccessNarrows},
		{name: "schema binding turned off", before: observed(true, filter(orders)),
			after: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders)}, SchemaBinding: &off}, want: schemaext.AccessUnchanged},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			effect := mssqldiff.Assess(semantics, test.before, test.after)
			c.Assert(effect.Access, qt.Equals, test.want)
			c.Assert(effect.Validate(), qt.IsNil)
		})
	}
}

// TestCodec_RoundTripsAChange pins that a change survives its codec through a
// registered runtime: both operands in their own wire forms and the access
// record.
func TestCodec_RoundTripsAChange(t *testing.T) {
	c := qt.New(t)
	codecs := must.Must(builtin.New()).Codecs()
	change := &mssqldiff.SecurityPolicy{Before: observed(true, filter(orders, "[tenant_id]")), After: declared(filter(orders, "owner_id")),
		Access: schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: "another argument"}}

	data, err := codecs.Marshal(t.Context(), schemaext.Change, []schemaext.Payload{change})
	c.Assert(err, qt.IsNil)
	decoded, err := codecs.Unmarshal(t.Context(), data)

	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{change})
}

// TestCodec_FailurePath pins the refusals of the change codec, each a typed
// model refusal: no operand, a lost access record, an operand the model's
// codec refuses, and an assessment that is not one.
func TestCodec_FailurePath(t *testing.T) {
	const operand = `{"predicates":[],"enabled":true,"schema_binding":true,"not_for_replication":false}`
	const access = `{"access":"widens","reason":"the policy stops filtering"}`
	tests := []struct {
		name  string
		input string
	}{
		{name: "no operand", input: `{"before":null,"after":null,"access":` + access + `}`},
		{name: "no access record", input: `{"before":` + operand + `,"after":null}`},
		{name: "an operand spelled another way", input: `{"before":` + operand + `,"after":{"predicates":[],"struct_name":""},"access":` + access + `}`},
		{name: "an access record without a reason", input: `{"before":` + operand + `,"after":null,"access":{"access":"widens","reason":""}}`},
		{name: "an unknown key", input: `{"before":` + operand + `,"after":null,"access":` + access + `,"note":"x"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			decoded, err := mssqldiff.Codec().Decode([]byte(test.input))
			var refusal *schemaext.InvalidModelError
			c.Assert(err, qt.ErrorAs, &refusal)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

// TestSecurityPolicy_CopySharesNothing pins that a copied change is
// independent of the original.
func TestSecurityPolicy_CopySharesNothing(t *testing.T) {
	c := qt.New(t)
	original := &mssqldiff.SecurityPolicy{Before: observed(true, filter(orders, "[tenant_id]")), After: declared(filter(orders, "tenant_id")),
		Access: schemaext.AccessEffect{Access: schemaext.AccessUnchanged, Reason: "same"}}

	copied := original.CloneChange().(*mssqldiff.SecurityPolicy)
	copied.Before.Predicates[0].Arguments[0] = "x"
	copied.After.Predicates[0].Table = invoices

	c.Assert(original.Before.Predicates[0].Arguments, qt.DeepEquals, []string{"[tenant_id]"})
	c.Assert(original.After.Predicates[0].Table, qt.Equals, orders)
}
