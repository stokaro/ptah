package goschematogo_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

var (
	ordersTable  = mssqlschema.ObjectName{Schema: "dbo", Name: "orders"}
	readFunction = mssqlschema.ObjectName{Schema: "rls", Name: "fn_read"}
)

// securityPolicySchema declares table orders and policy, scoped to SQL
// Server, under rls.tenancy.
func securityPolicySchema(policy mssqlschema.DesiredSecurityPolicy) *schemamodel.Database {
	object := must.Must(mssqlschema.DesiredSecurityPolicyObject(mssqlschema.SecurityPolicyRef("rls", "tenancy"), policy))
	object.Targets = []string{"sqlserver"}
	return &schemamodel.Database{
		Tables:          []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields:          []schemamodel.Field{{StructName: "Order", FieldName: "ID", Name: "id", Type: "INT", Primary: true}},
		FeatureObjects:  must.Must(schemaext.NewObjects(object)),
		FeatureCoverage: must.Must(mssqlschema.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
}

// renderSQLServerFile writes db as one Go file for SQL Server.
func renderSQLServerFile(c *qt.C, db *schemamodel.Database) (string, error) {
	c.Helper()
	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true, Dialect: "sqlserver", Runtime: must.Must(builtin.New())})
	var source strings.Builder
	for _, file := range files {
		source.Write(file.Data)
	}
	return source.String(), err
}

// A security policy is written as SQL Server-scoped row-level security
// annotations beside its table, one for each block operation, the first
// carrying the filter, and the Go source reads them back into the same
// policy.
func TestRender_WritesASecurityPolicyTheSourceReadsBack(t *testing.T) {
	c := qt.New(t)
	policy := mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
		{Type: mssqlschema.Filter, Function: readFunction, Arguments: []string{"tenant_id"}, Table: ordersTable},
		{Type: mssqlschema.Block, Function: readFunction, Arguments: []string{"CAST(tenant_id AS int) + 0"}, Table: ordersTable,
			Operation: mssqlschema.AfterInsert},
		{Type: mssqlschema.Block, Function: readFunction, Arguments: []string{"tenant_id"}, Table: ordersTable,
			Operation: mssqlschema.BeforeDelete},
	}}

	source, err := renderSQLServerFile(c, securityPolicySchema(policy))
	c.Assert(err, qt.IsNil)
	parsed := must.Must(goschema.ParseSource(builtintest.Annotations(), "models.go", source))

	c.Assert(source, qt.Contains, `//ptah:schema:rls:policy name="rls.tenancy" table="orders" for="INSERT" `+
		`using="[rls].[fn_read](tenant_id)" with_check="[rls].[fn_read](CAST(tenant_id AS int) + 0)" dialects="sqlserver"`)
	c.Assert(source, qt.Contains, `//ptah:schema:rls:policy name="rls.tenancy" table="orders" for="DELETE" `+
		`with_check="[rls].[fn_read](tenant_id)" dialects="sqlserver"`)
	read, found, err := parsed.FeatureObjects.Get(mssqlschema.SecurityPolicyRef("rls", "tenancy"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(read.Targets, qt.DeepEquals, []string{"sqlserver"})
	c.Assert(mssqlschema.SortedPredicates(read.Value.(*mssqlschema.DesiredSecurityPolicy).Predicates), qt.DeepEquals,
		mssqlschema.SortedPredicates(policy.Predicates))
}

// What a row-level security annotation cannot say is refused rather than left
// out.
func TestRender_SecurityPolicy_FailurePath(t *testing.T) {
	filter := mssqlschema.Predicate{Type: mssqlschema.Filter, Function: readFunction, Arguments: []string{"tenant_id"}, Table: ordersTable}
	off := false
	tests := []struct {
		name    string
		policy  mssqlschema.DesiredSecurityPolicy
		message string
	}{
		{name: "a policy turned off", policy: mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter}, Enabled: &off},
			message: `turned off, without schema binding, or not for replication`},
		{name: "a BEFORE UPDATE block predicate", policy: mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
			{Type: mssqlschema.Block, Function: readFunction, Table: ordersTable, Operation: mssqlschema.BeforeUpdate}}},
			message: `cannot declare the BEFORE UPDATE block predicate`},
		{name: "a table the export does not declare", policy: mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
			{Type: mssqlschema.Filter, Function: readFunction, Table: mssqlschema.ObjectName{Schema: "dbo", Name: "invoices"}}}},
			message: `binds table [dbo].[invoices], which the export does not declare`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := renderSQLServerFile(c, securityPolicySchema(test.policy))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err.Error(), qt.Contains, test.message)
		})
	}
}
