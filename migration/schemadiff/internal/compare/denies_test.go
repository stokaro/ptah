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
// planned from one would give a role what an administrator denied it. The
// DENY it keeps is reported. A grantee matches by its name as written: the
// SQL Server read names the fixed role PUBLIC, as a schema file does, and a
// role a case-sensitive database holds as Public is another principal.
func TestGrantsWithOptions_ComparesDenies_HappyPath(t *testing.T) {
	managed := []schemamodel.Role{{Name: "reader"}}
	to := func(role string) catalog.Grant {
		row := deniedRow("DELETE", "")
		row.Role = role
		return row
	}
	toRef := func(role string) difftypes.GrantRef {
		ref := tReader("DELETE", "")
		ref.Role = role
		return ref
	}
	heldTo := func(role string) difftypes.GrantRef {
		ref := toRef(role)
		ref.GrantedBy = "dbo"
		return ref
	}
	declaredPublic := &schemamodel.Database{DeniedGrants: []schemamodel.Grant{
		{Role: "PUBLIC", Privileges: []string{"DELETE"}, OnTable: "dbo.t"},
	}}
	tests := []struct {
		name          string
		desired       *schemamodel.Database
		held          []catalog.Grant
		complete      bool
		wantAdded     []difftypes.GrantRef
		wantRemoved   []difftypes.GrantRef
		wantUnmanaged []difftypes.GrantRef
		wantMatched   int
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
			// The DENY on the table the schema leaves out stays, and is named.
			wantUnmanaged: []difftypes.GrantRef{heldDeny("SELECT", "")},
		},
		{
			name:        "a declared deny the database holds",
			desired:     &schemamodel.Database{Roles: managed, DeniedGrants: []schemamodel.Grant{denyOf("SELECT")}},
			held:        []catalog.Grant{deniedRow("SELECT", "")},
			wantMatched: 1,
		},
		{
			name:          "a deny the schema leaves out from a role it manages",
			desired:       &schemamodel.Database{Roles: managed},
			held:          []catalog.Grant{deniedRow("SELECT", "")},
			wantUnmanaged: []difftypes.GrantRef{heldDeny("SELECT", "")},
		},
		{
			name:          "a deny the schema leaves out from a role it does not manage",
			desired:       &schemamodel.Database{},
			held:          []catalog.Grant{deniedRow("UPDATE", "secret")},
			wantUnmanaged: []difftypes.GrantRef{heldDeny("UPDATE", "secret")},
		},
		{
			name:        "a deny to PUBLIC the database holds",
			desired:     declaredPublic,
			held:        []catalog.Grant{to("PUBLIC")},
			wantMatched: 1,
		},
		{
			name:          "a deny to PUBLIC beside one a role named Public holds",
			desired:       declaredPublic,
			held:          []catalog.Grant{to("Public")},
			wantAdded:     []difftypes.GrantRef{toRef("PUBLIC")},
			wantUnmanaged: []difftypes.GrantRef{heldTo("Public")},
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
			c.Assert(diff.UnmanagedDenies, qt.DeepEquals, test.wantUnmanaged)
			c.Assert(diff.Matched.Denies, qt.Equals, test.wantMatched)
			c.Assert(diff.GrantsAdded, qt.IsNil)
			c.Assert(diff.GrantsRemoved, qt.IsNil)
		})
	}
}

// grantRow is a grant of privilege on dbo.t to role as the SQL Server reader
// reports one, on column where one is named.
func grantRow(role, privilege, column string) catalog.Grant {
	return catalog.Grant{Role: role, Privilege: privilege, ObjectType: "TABLE", Schema: "dbo", ObjectName: "t", Column: column, GrantedBy: "dbo"}
}

// splitTable is a grant of SELECT on the whole of dbo.t to role as the reader
// reports it where a column row of SELECT sits under it: a grant on each
// column without a row of its own, here id and secret beside a grantable row
// for name.
func splitTable(role string) []catalog.Grant {
	name := grantRow(role, "SELECT", "name")
	name.WithOption = true
	return []catalog.Grant{grantRow(role, "SELECT", "id"), name, grantRow(role, "SELECT", "secret")}
}

// TestGrantsWithOptions_GrantsDeniesTake_HappyPath pins which column grants a
// DENY the diff adds on dbo.t takes and the plan grants again: the ones the
// desired state declares and the database holds, as declared. A grant the
// read reports on each column of a table grant, and one held by a grantee the
// schema does not manage, stays taken unless the schema declares it.
func TestGrantsWithOptions_GrantsDeniesTake_HappyPath(t *testing.T) {
	declaredName := schemamodel.Grant{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "dbo.t", Columns: []string{"name"}}
	tests := []struct {
		name      string
		desired   *schemamodel.Database
		held      []catalog.Grant
		wantTaken []difftypes.GrantRef
	}{
		{
			name:    "a table grant read on each column, to a role the schema does not manage",
			desired: &schemamodel.Database{DeniedGrants: []schemamodel.Grant{denyOf("SELECT")}},
			held:    splitTable("reader"),
		},
		{
			name:    "a table grant read on each column, to a database user",
			desired: &schemamodel.Database{DeniedGrants: []schemamodel.Grant{{Role: "u", Privileges: []string{"SELECT"}, OnTable: "dbo.t"}}},
			held:    []catalog.Grant{grantRow("u", "SELECT", "id"), grantRow("u", "SELECT", "name")},
		},
		{
			name:      "a declared column grant to a role the schema does not manage",
			desired:   &schemamodel.Database{DeniedGrants: []schemamodel.Grant{denyOf("SELECT")}, Grants: []schemamodel.Grant{declaredName}},
			held:      splitTable("reader"),
			wantTaken: []difftypes.GrantRef{tReader("SELECT", "name")},
		},
		{
			name: "a declared column grant to a role the schema manages",
			desired: &schemamodel.Database{
				Roles: []schemamodel.Role{{Name: "reader"}}, DeniedGrants: []schemamodel.Grant{denyOf("SELECT")}, Grants: []schemamodel.Grant{declaredName},
			},
			held:      []catalog.Grant{grantRow("reader", "SELECT", "name")},
			wantTaken: []difftypes.GrantRef{tReader("SELECT", "name")},
		},
		{
			name:    "a declared column grant the database lacks",
			desired: &schemamodel.Database{DeniedGrants: []schemamodel.Grant{denyOf("SELECT")}, Grants: []schemamodel.Grant{declaredName}},
		},
		{
			name:    "a column deny takes no column grant",
			desired: &schemamodel.Database{DeniedGrants: []schemamodel.Grant{denyOf("SELECT", "secret")}, Grants: []schemamodel.Grant{declaredName}},
			held:    []catalog.Grant{grantRow("reader", "SELECT", "name")},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.GrantsWithOptions(test.desired, &catalog.Database{Grants: test.held}, diff, identifier.ForDialect("sqlserver"), compare.GrantOptions{})

			c.Assert(diff.GrantsDeniesTake, qt.DeepEquals, test.wantTaken)
		})
	}
}

// TestGrantsWithOptions_GrantToPublic_HappyPath pins that a grant to PUBLIC
// matches the row the SQL Server read names PUBLIC, so a schema that grants
// it plans nothing on the next run, and no row of a role whose name differs
// from it by case: on a case-sensitive database that is another principal.
func TestGrantsWithOptions_GrantToPublic_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		held        string
		wantAdded   []difftypes.GrantRef
		wantMatched int
	}{
		{name: "the fixed role", held: "PUBLIC", wantMatched: 1},
		{
			name:      "a role named Public",
			held:      "Public",
			wantAdded: []difftypes.GrantRef{{Role: "PUBLIC", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "dbo.t"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}
			desired := &schemamodel.Database{Grants: []schemamodel.Grant{{Role: "PUBLIC", Privileges: []string{"SELECT"}, OnTable: "dbo.t"}}}

			compare.GrantsWithOptions(desired, &catalog.Database{Grants: []catalog.Grant{grantRow(test.held, "SELECT", "")}}, diff,
				identifier.ForDialect("sqlserver"), compare.GrantOptions{})

			c.Assert(diff.GrantsAdded, qt.DeepEquals, test.wantAdded)
			c.Assert(diff.Matched.Grants, qt.Equals, test.wantMatched)
		})
	}
}
