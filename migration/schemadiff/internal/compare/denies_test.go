package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// deniedRow is a SQL Server DENY row of reader on dbo.t, as the reader reports
// one: a partial revoke that is denied.
func deniedRow(privilege, column string) catalog.Grant {
	return catalog.Grant{
		Role: "reader", Privilege: privilege, ObjectType: "TABLE", Schema: "dbo", ObjectName: "t", Column: column,
		IsPartialRevoke: true, Denied: true, GrantedBy: "dbo",
	}
}

// denyOf is reader denied privilege on dbo.t in the desired state.
func denyOf(privilege string, columns ...string) schemamodel.Grant {
	return schemamodel.Grant{Role: "reader", Privileges: []string{privilege}, OnTable: "dbo.t", Columns: columns}
}

// tReader is the reference a plan names for privilege on dbo.t.
func tReader(privilege, column string) difftypes.GrantRef {
	return difftypes.GrantRef{Role: "reader", Privilege: privilege, ObjectType: "TABLE", ObjectName: "dbo.t", Column: column}
}

// heldDeny is tReader as the read reports it, with the grantor of the row.
func heldDeny(privilege, column string) difftypes.GrantRef {
	ref := tReader(privilege, column)
	ref.GrantedBy = "dbo"
	return ref
}

// TestGrantsWithOptions_ComparesDenies_HappyPath compares the denies a desired
// state declares with the DENY rows of a SQL Server database.
//
// A declared DENY the database lacks is added, for a role the schema manages or
// not. One the database holds is a match. One the database holds and the
// desired state leaves out is taken away only where the desired state holds
// every grant: a Go annotation or a YAML document has no DENY, so a removal
// planned from one would give a role what an administrator denied it.
func TestGrantsWithOptions_ComparesDenies_HappyPath(t *testing.T) {
	managed := []schemamodel.Role{{Name: "reader"}}
	tests := []struct {
		name        string
		desired     *schemamodel.Database
		held        []catalog.Grant
		complete    bool
		wantAdded   []difftypes.GrantRef
		wantRemoved []difftypes.GrantRef
		wantMatched int
	}{
		{
			name:      "a declared deny the database lacks",
			desired:   &schemamodel.Database{DeniedGrants: []schemamodel.Grant{denyOf("SELECT")}},
			wantAdded: []difftypes.GrantRef{tReader("SELECT", "")},
		},
		{
			name:      "a declared deny on a column",
			desired:   &schemamodel.Database{Roles: managed, DeniedGrants: []schemamodel.Grant{denyOf("SELECT", "secret")}},
			held:      []catalog.Grant{deniedRow("SELECT", "")},
			wantAdded: []difftypes.GrantRef{tReader("SELECT", "secret")},
		},
		{
			name:        "a declared deny the database holds",
			desired:     &schemamodel.Database{Roles: managed, DeniedGrants: []schemamodel.Grant{denyOf("SELECT")}},
			held:        []catalog.Grant{deniedRow("SELECT", "")},
			wantMatched: 1,
		},
		{
			name:    "a deny the schema leaves out from a role it manages",
			desired: &schemamodel.Database{Roles: managed},
			held:    []catalog.Grant{deniedRow("SELECT", "")},
		},
		{
			name:        "a deny a complete desired state leaves out",
			desired:     &schemamodel.Database{Roles: managed},
			held:        []catalog.Grant{deniedRow("SELECT", ""), deniedRow("UPDATE", "secret")},
			complete:    true,
			wantRemoved: []difftypes.GrantRef{heldDeny("SELECT", ""), heldDeny("UPDATE", "secret")},
		},
		{
			name:     "a deny a complete desired state leaves out of a role it does not name",
			desired:  &schemamodel.Database{},
			held:     []catalog.Grant{deniedRow("SELECT", "")},
			complete: true,
			// A complete desired state removes a grant from any role but the
			// PUBLIC defaults, and a DENY on the same rule.
			wantRemoved: []difftypes.GrantRef{heldDeny("SELECT", "")},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.GrantsWithOptions(test.desired, &catalog.Database{Grants: test.held}, diff, identifier.ForDialect("sqlserver"),
				compare.GrantOptions{DesiredComplete: test.complete})

			c.Assert(diff.DeniesAdded, qt.DeepEquals, test.wantAdded)
			c.Assert(diff.DeniesRemoved, qt.DeepEquals, test.wantRemoved)
			c.Assert(diff.Matched.Denies, qt.Equals, test.wantMatched)
			c.Assert(diff.GrantsAdded, qt.IsNil)
			c.Assert(diff.GrantsRemoved, qt.IsNil)
		})
	}
}
