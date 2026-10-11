package mssqlpolicysource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/internal/mssqlpolicysource"
)

var (
	ordersTable = mssqlschema.ObjectName{Schema: "dbo", Name: "orders"}
	readFn      = mssqlschema.ObjectName{Schema: "rls", Name: "fn_read"}
)

// policyObject is policy under schema.tenancy with targets.
func policyObject(schema string, policy mssqlschema.DesiredSecurityPolicy, targets ...string) schemaext.Object {
	object := must.Must(mssqlschema.DesiredSecurityPolicyObject(mssqlschema.SecurityPolicyRef(schema, "tenancy"), policy))
	object.Targets = targets
	return object
}

func encodeSecurityPolicies(c *qt.C, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	c.Helper()
	return mssqlpolicysource.Annotations().Encoder.EncodeAnnotations(c.Context(), request)
}

func TestEncodeAnnotations_HappyPath(t *testing.T) {
	orders := annotation.Table{Name: "orders", Struct: "Order"}
	sales := annotation.Table{Schema: "sales", Name: "orders", Struct: "SalesOrder"}
	filter := mssqlschema.Predicate{Type: mssqlschema.Filter, Function: readFn, Arguments: []string{"tenant_id"}, Table: ordersTable}
	insert := mssqlschema.Predicate{Type: mssqlschema.Block, Function: readFn, Arguments: []string{"tenant_id"}, Table: ordersTable,
		Operation: mssqlschema.AfterInsert}
	update := mssqlschema.Predicate{Type: mssqlschema.Block, Function: readFn, Arguments: []string{"region"}, Table: ordersTable,
		Operation: mssqlschema.AfterUpdate}
	salesFilter := mssqlschema.Predicate{Type: mssqlschema.Filter, Function: readFn, Arguments: []string{"region_id"},
		Table: mssqlschema.ObjectName{Schema: "sales", Name: "orders"}}
	tests := []struct {
		name    string
		objects []schemaext.Object
		want    []annotation.EncodedDirective
	}{
		{name: "a filter alone, scoped to SQL Server by default",
			objects: []schemaext.Object{policyObject("dbo", mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter}})},
			want: []annotation.EncodedDirective{{Directive: "ptah:schema:rls:policy", Placement: annotation.AfterPolicies, Table: &orders,
				Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "tenancy"}, {Name: "table", Value: "orders"},
					{Name: "using", Value: "[rls].[fn_read](tenant_id)"}}, Targets: []string{"sqlserver"}}}},
		{name: "a filter beside two block predicates, the filter on the first",
			objects: []schemaext.Object{policyObject("rls", mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{update, filter, insert}},
				"sqlserver")},
			want: []annotation.EncodedDirective{
				{Directive: "ptah:schema:rls:policy", Placement: annotation.AfterPolicies, Table: &orders,
					Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "rls.tenancy"}, {Name: "table", Value: "orders"},
						{Name: "for", Value: "INSERT"}, {Name: "using", Value: "[rls].[fn_read](tenant_id)"},
						{Name: "with_check", Value: "[rls].[fn_read](tenant_id)"}}, Targets: []string{"sqlserver"}},
				{Directive: "ptah:schema:rls:policy", Placement: annotation.AfterPolicies, Table: &orders,
					Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "rls.tenancy"}, {Name: "table", Value: "orders"},
						{Name: "for", Value: "UPDATE"}, {Name: "with_check", Value: "[rls].[fn_read](region)"}}, Targets: []string{"sqlserver"}},
			}},
		{name: "a table in its own schema",
			objects: []schemaext.Object{policyObject("sales", mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{salesFilter}})},
			want: []annotation.EncodedDirective{{Directive: "ptah:schema:rls:policy", Placement: annotation.AfterPolicies, Table: &sales,
				Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "tenancy"}, {Name: "table", Value: "sales.orders"},
					{Name: "using", Value: "[rls].[fn_read](region_id)"}}, Targets: []string{"sqlserver"}}}},
		{name: "two tables, each with its own predicates",
			objects: []schemaext.Object{policyObject("rls", mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
				salesFilter, filter, {Type: mssqlschema.Block, Function: readFn, Arguments: []string{"region"}, Table: salesFilter.Table,
					Operation: mssqlschema.AfterInsert}}})},
			want: []annotation.EncodedDirective{
				{Directive: "ptah:schema:rls:policy", Placement: annotation.AfterPolicies, Table: &orders,
					Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "rls.tenancy"}, {Name: "table", Value: "orders"},
						{Name: "using", Value: "[rls].[fn_read](tenant_id)"}}, Targets: []string{"sqlserver"}},
				{Directive: "ptah:schema:rls:policy", Placement: annotation.AfterPolicies, Table: &sales,
					Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "rls.tenancy"}, {Name: "table", Value: "sales.orders"},
						{Name: "for", Value: "INSERT"}, {Name: "using", Value: "[rls].[fn_read](region_id)"},
						{Name: "with_check", Value: "[rls].[fn_read](region)"}}, Targets: []string{"sqlserver"}},
			}},
		{name: "nothing of the owner's"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := encodeSecurityPolicies(c, annotation.EncodeRequest{Objects: test.objects, Tables: annotation.Tables{orders, sales}})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Directives, qt.DeepEquals, test.want)
		})
	}
}

func TestEncodeAnnotations_FailurePath(t *testing.T) {
	orders := annotation.Table{Name: "orders", Struct: "Order"}
	filter := mssqlschema.Predicate{Type: mssqlschema.Filter, Function: readFn, Arguments: []string{"tenant_id"}, Table: ordersTable}
	tests := []struct {
		name    string
		object  schemaext.Object
		wantIs  error
		wantErr string
	}{
		{name: "a policy turned off",
			object: policyObject("rls", mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter}, Enabled: new(false)}),
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*turned off, without schema binding, or not for replication`},
		{name: "a policy without schema binding",
			object: policyObject("rls", mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter}, SchemaBinding: new(false)}),
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*turned off, without schema binding, or not for replication`},
		{name: "a policy not for replication",
			object: policyObject("rls", mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter}, NotForReplication: true}),
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*turned off, without schema binding, or not for replication`},
		{name: "a BEFORE UPDATE block predicate", object: policyObject("rls", mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
			{Type: mssqlschema.Block, Function: readFn, Table: ordersTable, Operation: mssqlschema.BeforeUpdate}}}),
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*cannot declare the BEFORE UPDATE block predicate of security policy .*`},
		{name: "a table the schema does not declare", object: policyObject("rls", mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
			{Type: mssqlschema.Filter, Function: readFn, Table: mssqlschema.ObjectName{Schema: "dbo", Name: "invoices"}}}}),
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*binds table \[dbo\]\.\[invoices\], which the export does not declare`},
		{name: "a value that is not a declaration", object: schemaext.Object{Ref: mssqlschema.SecurityPolicyRef("rls", "tenancy"),
			Value: &mssqlschema.ObservedSecurityPolicy{}},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot represent feature object .* with value \*mssqlschema.ObservedSecurityPolicy`},
		{name: "a predicate without a function", object: schemaext.Object{Ref: mssqlschema.SecurityPolicyRef("rls", "tenancy"),
			Value: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{{Type: mssqlschema.Filter, Table: ordersTable}}}},
			wantIs: schemaext.ErrInvalidValue, wantErr: `desired model "ptah.run/mssql/security-policy": .*`},
		{name: "a reference to a table", object: schemaext.Object{Ref: objectidentity.NewBuilder(identifier.ForDialect("sqlserver")).TableParts("rls", "tenancy"),
			Value: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter}}},
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*a security policy requires a schema and a name, and no table parent`},
		{name: "a declaration without predicates", object: schemaext.Object{Ref: mssqlschema.SecurityPolicyRef("rls", "tenancy"),
			Value: &mssqlschema.DesiredSecurityPolicy{}},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*cannot declare security policy .*tenancy.*, which binds no table`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := encodeSecurityPolicies(c, annotation.EncodeRequest{Objects: []schemaext.Object{test.object}, Tables: annotation.Tables{orders}})

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result.Directives, qt.IsNil)
		})
	}
}
