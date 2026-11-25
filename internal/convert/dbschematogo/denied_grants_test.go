package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvertDBSchemaToGoSchema_DescribesADenyAsDenied pins the SQL Server DENY
// rows of a read. A DENY row is a partial revoke, so it is no grant, and the
// description withholds it as a denied grant: left out, a description applied
// to another database gives the role what a grant on the schema or a
// membership carries there, the denied privilege included. The grantor is not
// part of a DENY a schema declares.
func TestConvertDBSchemaToGoSchema_DescribesADenyAsDenied(t *testing.T) {
	c := qt.New(t)

	converted := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), &catalog.Database{
		Grants: []catalog.Grant{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "t", GrantedBy: "dbo"},
			{
				Role: "reader", Privilege: "UPDATE", ObjectType: "TABLE", Schema: "dbo", ObjectName: "t",
				IsPartialRevoke: true, Denied: true, GrantedBy: "dbo",
			},
			{
				Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "t", Column: "secret",
				IsPartialRevoke: true, Denied: true, GrantedBy: "dbo",
			},
		},
	}, "sqlserver", must.Must(builtin.New())))

	c.Assert(privilegeTargets(converted.Grants), qt.DeepEquals, []string{"SELECT TABLE dbo.t"})
	c.Assert(privilegeTargets(converted.DeniedGrants), qt.DeepEquals, []string{"UPDATE TABLE dbo.t", "SELECT TABLE dbo.t (secret)"})
	c.Assert(converted.DeniedGrants[0].GrantedBy, qt.Equals, "")
}

// privilegeTargets is each grant's privileges and target, in order.
func privilegeTargets(grants []schemamodel.Grant) []string {
	out := make([]string, 0, len(grants))
	for _, grant := range grants {
		for _, privilege := range grant.Privileges {
			out = append(out, privilege+" "+grant.TargetKey())
		}
	}
	return out
}
