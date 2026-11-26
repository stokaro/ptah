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

// heldBy is a SQL Server row on dbo.t, which dbo owns, of reader, on column
// where one is named, that grantor gave: a grant, or with denied a DENY.
func heldBy(grantor, privilege, column string, denied, withOption bool) catalog.Grant {
	return catalog.Grant{
		Role: "reader", Privilege: privilege, ObjectType: "TABLE", Schema: "dbo", ObjectName: "t", Column: column,
		WithOption: withOption, IsPartialRevoke: denied, Denied: denied, GrantedBy: grantor, ObjectOwner: "dbo",
	}
}

// replacedRef is heldBy as a plan names it.
func replacedRef(grantor, privilege, column string, withOption bool) difftypes.GrantRef {
	ref := tReader(privilege, column)
	ref.WithOption = withOption
	ref.GrantedBy = grantor
	ref.ObjectOwner = "dbo"
	return ref
}

// grantable is ref with the grant option.
func grantable(ref difftypes.GrantRef) difftypes.GrantRef {
	ref.WithOption = true
	return ref
}

// grantOf is reader granted privilege on dbo.t in the desired state.
func grantOf(privilege string, withOption bool, columns ...string) schemamodel.Grant {
	return schemamodel.Grant{Role: "reader", Privileges: []string{privilege}, OnTable: "dbo.t", Columns: columns, WithOption: withOption}
}

// onApp is a row of reader on the schema app, which o owns, or on the table
// app.s in it, that o gave. owner is the owner the read names for it.
func onApp(objectType, object, owner string) catalog.Grant {
	grant := catalog.Grant{
		Role: "reader", Privilege: "SELECT", ObjectType: objectType, Schema: "app", ObjectName: object,
		GrantedBy: "o", ObjectOwner: owner,
	}
	if objectType == "SCHEMA" {
		grant.Schema = ""
	}
	return grant
}

// TestGrantsWithOptions_ReplacedRows_HappyPath pins which rows of a SQL Server
// database a statement the diff plans takes the place of, so that a rollback
// gives them back.
//
// A DENY takes the place of a grant of the same permission on the same
// securable or column, with or without the grant option, and a grant takes
// the place of a DENY. A DENY on a whole object also takes the DENY rows of
// its permission on the object's columns. Only a row the owner the read names
// for its securable gave is replaced: one another grantor gave stays, the
// connected principal's own among them, and a row the diff takes away itself
// comes back from the removal lists. Every row is read by deployer, which is
// not the owner of anything.
func TestGrantsWithOptions_ReplacedRows_HappyPath(t *testing.T) {
	managed := []schemamodel.Role{{Name: "reader"}}
	tests := []struct {
		name         string
		desired      *schemamodel.Database
		held         []catalog.Grant
		complete     bool
		wantGrants   []difftypes.GrantRef
		wantDenies   []difftypes.GrantRef
		wantAdded    []difftypes.GrantRef
		wantDenyAdds []difftypes.GrantRef
	}{
		{
			name:         "a grant a declared deny replaces",
			desired:      &schemamodel.Database{DeniedGrants: []schemamodel.Grant{denyOf("SELECT")}},
			held:         []catalog.Grant{heldBy("dbo", "SELECT", "", false, false)},
			wantGrants:   []difftypes.GrantRef{replacedRef("dbo", "SELECT", "", false)},
			wantDenyAdds: []difftypes.GrantRef{tReader("SELECT", "")},
		},
		{
			name:         "a grant with the grant option a declared deny replaces",
			desired:      &schemamodel.Database{DeniedGrants: []schemamodel.Grant{denyOf("SELECT")}},
			held:         []catalog.Grant{heldBy("dbo", "SELECT", "", false, true)},
			wantGrants:   []difftypes.GrantRef{replacedRef("dbo", "SELECT", "", true)},
			wantDenyAdds: []difftypes.GrantRef{tReader("SELECT", "")},
		},
		{
			name:         "a column grant a declared column deny replaces",
			desired:      &schemamodel.Database{DeniedGrants: []schemamodel.Grant{denyOf("SELECT", "a")}},
			held:         []catalog.Grant{heldBy("dbo", "SELECT", "a", false, false)},
			wantGrants:   []difftypes.GrantRef{replacedRef("dbo", "SELECT", "a", false)},
			wantDenyAdds: []difftypes.GrantRef{tReader("SELECT", "a")},
		},
		{
			name:    "a column grant a declared deny on the table takes",
			desired: &schemamodel.Database{DeniedGrants: []schemamodel.Grant{denyOf("SELECT")}},
			held:    []catalog.Grant{heldBy("dbo", "SELECT", "a", false, false)},
			// A rollback that takes the DENY on the table away grants the
			// column grants of the read again on its own.
			wantDenyAdds: []difftypes.GrantRef{tReader("SELECT", "")},
		},
		{
			name:         "a column deny a declared deny on the table takes",
			desired:      &schemamodel.Database{DeniedGrants: []schemamodel.Grant{denyOf("SELECT")}},
			held:         []catalog.Grant{heldBy("dbo", "SELECT", "b", true, false)},
			wantDenies:   []difftypes.GrantRef{replacedRef("dbo", "SELECT", "b", false)},
			wantDenyAdds: []difftypes.GrantRef{tReader("SELECT", "")},
		},
		{
			name:       "a deny a declared grant replaces",
			desired:    &schemamodel.Database{Grants: []schemamodel.Grant{grantOf("SELECT", false)}},
			held:       []catalog.Grant{heldBy("dbo", "SELECT", "", true, false)},
			wantDenies: []difftypes.GrantRef{replacedRef("dbo", "SELECT", "", false)},
			wantAdded:  []difftypes.GrantRef{tReader("SELECT", "")},
		},
		{
			name:       "a deny a declared grant with the grant option replaces",
			desired:    &schemamodel.Database{Grants: []schemamodel.Grant{grantOf("SELECT", true)}},
			held:       []catalog.Grant{heldBy("dbo", "SELECT", "", true, false)},
			wantDenies: []difftypes.GrantRef{replacedRef("dbo", "SELECT", "", false)},
			wantAdded:  []difftypes.GrantRef{grantable(tReader("SELECT", ""))},
		},
		{
			name:       "a column deny a declared column grant replaces",
			desired:    &schemamodel.Database{Grants: []schemamodel.Grant{grantOf("SELECT", false, "a")}},
			held:       []catalog.Grant{heldBy("dbo", "SELECT", "a", true, false)},
			wantDenies: []difftypes.GrantRef{replacedRef("dbo", "SELECT", "a", false)},
			wantAdded:  []difftypes.GrantRef{tReader("SELECT", "a")},
		},
		{
			name:       "a deny a declared grant of every privilege replaces",
			desired:    &schemamodel.Database{Grants: []schemamodel.Grant{grantOf("ALL", false)}},
			held:       []catalog.Grant{heldBy("dbo", "UPDATE", "", true, false)},
			wantDenies: []difftypes.GrantRef{replacedRef("dbo", "UPDATE", "", false)},
			wantAdded:  []difftypes.GrantRef{tReader("ALL", "")},
		},
		{
			// The connected principal gave it from its grant option, and a
			// DENY it runs with CONTROL names the owner as its grantor.
			name:         "a grant the connected principal gave stays beside the deny",
			desired:      &schemamodel.Database{DeniedGrants: []schemamodel.Grant{denyOf("SELECT")}},
			held:         []catalog.Grant{heldBy("deployer", "SELECT", "", false, false)},
			wantDenyAdds: []difftypes.GrantRef{tReader("SELECT", "")},
		},
		{
			name:         "a grant another grantor gave stays beside the deny",
			desired:      &schemamodel.Database{DeniedGrants: []schemamodel.Grant{denyOf("SELECT")}},
			held:         []catalog.Grant{heldBy("u", "SELECT", "", false, false)},
			wantDenyAdds: []difftypes.GrantRef{tReader("SELECT", "")},
		},
		{
			// A name compares as the read spells it, so a padded grantor is
			// another principal than the owner.
			name:         "a grant a grantor named like the owner with padding gave",
			desired:      &schemamodel.Database{DeniedGrants: []schemamodel.Grant{denyOf("SELECT")}},
			held:         []catalog.Grant{heldBy(" dbo", "SELECT", "", false, false)},
			wantDenyAdds: []difftypes.GrantRef{tReader("SELECT", "")},
		},
		{
			name:      "a deny another grantor gave",
			desired:   &schemamodel.Database{Grants: []schemamodel.Grant{grantOf("SELECT", false)}},
			held:      []catalog.Grant{heldBy("u", "SELECT", "", true, false)},
			wantAdded: []difftypes.GrantRef{tReader("SELECT", "")},
		},
		{
			name: "a grant on a schema its owner gave",
			desired: &schemamodel.Database{DeniedGrants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnSchema: "app"},
			}},
			held: []catalog.Grant{onApp("SCHEMA", "app", "o")},
			wantGrants: []difftypes.GrantRef{
				{Role: "reader", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: "app", GrantedBy: "o", ObjectOwner: "o"},
			},
			wantDenyAdds: []difftypes.GrantRef{{Role: "reader", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: "app"}},
		},
		{
			name: "a grant on a table the owner of its schema gave",
			desired: &schemamodel.Database{DeniedGrants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "app.s"},
			}},
			held: []catalog.Grant{onApp("TABLE", "s", "o")},
			wantGrants: []difftypes.GrantRef{
				{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.s", GrantedBy: "o", ObjectOwner: "o"},
			},
			wantDenyAdds: []difftypes.GrantRef{{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.s"}},
		},
		{
			name: "a grant on a procedure given to another principal",
			desired: &schemamodel.Database{DeniedGrants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"EXECUTE"}, OnTable: "dbo.p"},
			}},
			held: []catalog.Grant{{
				Role: "reader", Privilege: "EXECUTE", ObjectType: "TABLE", Schema: "dbo", ObjectName: "p",
				GrantedBy: "powner", ObjectOwner: "powner",
			}},
			wantGrants: []difftypes.GrantRef{
				{Role: "reader", Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: "dbo.p", GrantedBy: "powner", ObjectOwner: "powner"},
			},
			wantDenyAdds: []difftypes.GrantRef{{Role: "reader", Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: "dbo.p"}},
		},
		{
			name: "a grant on a schema the read names no owner of",
			desired: &schemamodel.Database{DeniedGrants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnSchema: "app"},
			}},
			held:         []catalog.Grant{onApp("SCHEMA", "app", "")},
			wantDenyAdds: []difftypes.GrantRef{{Role: "reader", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: "app"}},
		},
		{
			name: "a grant the diff revokes",
			desired: &schemamodel.Database{
				Roles: managed, DeniedGrants: []schemamodel.Grant{denyOf("SELECT")},
			},
			held:         []catalog.Grant{heldBy("dbo", "SELECT", "", false, true)},
			wantDenyAdds: []difftypes.GrantRef{tReader("SELECT", "")},
		},
		{
			name:      "a deny the diff takes away",
			desired:   &schemamodel.Database{Roles: managed, Grants: []schemamodel.Grant{grantOf("SELECT", false)}},
			held:      []catalog.Grant{heldBy("dbo", "SELECT", "", true, false)},
			complete:  true,
			wantAdded: []difftypes.GrantRef{tReader("SELECT", "")},
		},
		{
			name:    "a grant held twice on one table, from the owner and from the connected principal",
			desired: &schemamodel.Database{DeniedGrants: []schemamodel.Grant{denyOf("SELECT")}},
			held: []catalog.Grant{
				heldBy("dbo", "SELECT", "", false, false),
				heldBy("deployer", "SELECT", "", false, false),
			},
			wantGrants:   []difftypes.GrantRef{replacedRef("dbo", "SELECT", "", false)},
			wantDenyAdds: []difftypes.GrantRef{tReader("SELECT", "")},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}
			database := &catalog.Database{Grants: test.held, ConnectedRole: "deployer"}

			compare.GrantsWithOptions(test.desired, database, diff, identifier.ForDialect("sqlserver"),
				compare.GrantOptions{DesiredComplete: test.complete})

			c.Assert(diff.GrantsReplaced, qt.DeepEquals, test.wantGrants)
			c.Assert(diff.DeniesReplaced, qt.DeepEquals, test.wantDenies)
			c.Assert(diff.GrantsAdded, qt.DeepEquals, test.wantAdded)
			c.Assert(diff.DeniesAdded, qt.DeepEquals, test.wantDenyAdds)
		})
	}
}

// TestGrantsWithOptions_ReplacedRowsOutsideSQLServer_HappyPath pins that a
// target with no DENY replaces nothing: a grant there adds a row beside what
// the role holds, so a rollback has nothing to give back.
func TestGrantsWithOptions_ReplacedRowsOutsideSQLServer_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	held := catalog.Grant{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "public", ObjectName: "t", Column: "a", GrantedBy: "postgres"}
	desired := &schemamodel.Database{Grants: []schemamodel.Grant{{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "public.t"}}}

	compare.GrantsWithOptions(desired, &catalog.Database{Grants: []catalog.Grant{held}, ConnectedRole: "postgres"}, diff,
		identifier.ForDialect("postgres"), compare.GrantOptions{})

	c.Assert(diff.GrantsReplaced, qt.IsNil)
	c.Assert(diff.DeniesReplaced, qt.IsNil)
	c.Assert(diff.GrantsAdded, qt.HasLen, 1)
}
