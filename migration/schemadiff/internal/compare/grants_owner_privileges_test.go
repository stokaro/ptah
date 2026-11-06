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

// These are what a PostgreSQL read reports for a table's owner: an implicit row
// for each privilege it holds, and the same with the partial revoke set for each
// privilege it revoked from itself.
var (
	ownerSelect = catalog.Grant{Role: "owner", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t", Implicit: true}
	ownerUpdate = catalog.Grant{Role: "owner", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "t", Implicit: true}
	ownerLost   = catalog.Grant{Role: "owner", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t", Implicit: true, IsPartialRevoke: true}
)

// everyTablePrivilege is what the owner of a table holds by default, as a read
// reports it, with no MAINTAIN: a read from PostgreSQL 16 has no such row.
func everyTablePrivilege() []catalog.Grant {
	privileges := []string{"DELETE", "INSERT", "REFERENCES", "SELECT", "TRIGGER", "TRUNCATE", "UPDATE"}
	grants := make([]catalog.Grant, 0, len(privileges))
	for _, privilege := range privileges {
		grants = append(grants, catalog.Grant{Role: "owner", Privilege: privilege, ObjectType: "TABLE", ObjectName: "t", Implicit: true})
	}
	return grants
}

func compareOwnerGrants(desired *schemamodel.Database, held []catalog.Grant, options compare.GrantOptions) *difftypes.SchemaDiff {
	diff := &difftypes.SchemaDiff{}
	compare.GrantsWithOptions(desired, &catalog.Database{Grants: held}, diff, identifier.ForDialect("postgres"), options)
	return diff
}

// declaresOwner is a declaration that manages the owner role and names no privilege
// of it. A role the source manages is the one whose grants a comparison may remove.
func declaresOwner() *schemamodel.Database {
	return &schemamodel.Database{Roles: []schemamodel.Role{{Name: "owner"}}}
}

// TestGrantsWithOptions_AnOwnersDefaultPrivilegeIsNoDifference is a source that
// says nothing about what the owner holds. Revoking the privilege to match leaves
// the owner refused on its own table, and granting it changes nothing.
func TestGrantsWithOptions_AnOwnersDefaultPrivilegeIsNoDifference(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		options compare.GrantOptions
	}{
		{name: "a declaration that names no role", desired: &schemamodel.Database{}},
		{name: "a declaration that manages the owner", desired: declaresOwner()},
		{name: "a read of a server", desired: &schemamodel.Database{}, options: compare.GrantOptions{DesiredComplete: true}},
		{name: "a read of a server that names the owner", desired: declaresOwner(), options: compare.GrantOptions{DesiredComplete: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareOwnerGrants(test.desired, []catalog.Grant{ownerSelect, ownerUpdate}, test.options)

			c.Assert(diff.GrantsAdded, qt.HasLen, 0)
			c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
			c.Assert(diff.GrantOptionsAdded, qt.HasLen, 0)
			c.Assert(diff.GrantOptionsRevoked, qt.HasLen, 0)
		})
	}
}

// TestGrantsWithOptions_ADeclaredGrantToAnOwnerPairsWithWhatItHolds is the grant
// a source may write out. It is held already, so a plan for it would repeat on a
// database where the ACL is NULL.
func TestGrantsWithOptions_ADeclaredGrantToAnOwnerPairsWithWhatItHolds(t *testing.T) {
	tests := []struct {
		name      string
		privilege string
		held      []catalog.Grant
	}{
		{name: "one privilege", privilege: "SELECT", held: []catalog.Grant{ownerSelect}},
		{name: "ALL, without MAINTAIN", privilege: "ALL", held: everyTablePrivilege()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := declaresOwner()
			desired.Grants = []schemamodel.Grant{{Role: "owner", Privileges: []string{test.privilege}, OnTable: "t"}}

			diff := compareOwnerGrants(desired, test.held, compare.GrantOptions{})

			c.Assert(diff.GrantsAdded, qt.HasLen, 0)
			c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
			c.Assert(diff.GrantOptionsRevoked, qt.HasLen, 0)
			c.Assert(diff.Matched.Grants, qt.Equals, 1)
		})
	}
}

// TestGrantsWithOptions_ADeclaredGrantToAnOwnerThatRevokedItIsPlanned is the
// control on the pairing above: a privilege the owner gave up is not held.
func TestGrantsWithOptions_ADeclaredGrantToAnOwnerThatRevokedItIsPlanned(t *testing.T) {
	c := qt.New(t)
	desired := declaresOwner()
	desired.Grants = []schemamodel.Grant{{Role: "owner", Privileges: []string{"SELECT"}, OnTable: "t"}}

	diff := compareOwnerGrants(desired, []catalog.Grant{ownerLost, ownerUpdate}, compare.GrantOptions{})

	c.Assert(diff.GrantsAdded, qt.DeepEquals, []difftypes.GrantRef{
		{Role: "owner", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"},
	})
}

// TestGrantsWithOptions_ADeclaredRevokeReachesWhatAnOwnerHoldsByDefault is a
// revoke a source writes out, or that a read of a server reports. A source that
// names the privilege decides it, and once the owner gave it up nothing is left
// to plan.
func TestGrantsWithOptions_ADeclaredRevokeReachesWhatAnOwnerHoldsByDefault(t *testing.T) {
	tests := []struct {
		name      string
		privilege string
		held      []catalog.Grant
		want      []difftypes.GrantRef
	}{
		{
			name:      "the owner holds it",
			privilege: "SELECT",
			held:      []catalog.Grant{ownerSelect, ownerUpdate},
			want:      []difftypes.GrantRef{{Role: "owner", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"}},
		},
		{
			name:      "the owner gave it up",
			privilege: "SELECT",
			held:      []catalog.Grant{ownerLost, ownerUpdate},
		},
		{
			name:      "ALL names what the owner holds on this server",
			privilege: "ALL",
			held:      []catalog.Grant{ownerLost, ownerUpdate},
			want:      []difftypes.GrantRef{{Role: "owner", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "t"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			// A table the database holds, so the revoke is not one on a table
			// this plan creates.
			desired := &schemamodel.Database{
				Tables:        []schemamodel.Table{{Name: "t"}},
				RevokedGrants: []schemamodel.Grant{{Role: "owner", Privileges: []string{test.privilege}, OnTable: "t"}},
			}
			database := &catalog.Database{Tables: []catalog.Table{{Name: "t"}}, Grants: test.held}
			diff := &difftypes.SchemaDiff{}

			compare.GrantsWithOptions(desired, database, diff, identifier.ForDialect("postgres"), compare.GrantOptions{})

			c.Assert(diff.GrantsRemoved, qt.DeepEquals, test.want)
		})
	}
}

// TestGrantsWithOptions_AnOwnersRevokedPrivilegeIsGrantedBack is two reads of a
// server that differ by what one owner gave up. The other side's read has to
// say that the same role holds the privilege on the same object: Ptah describes no
// ownership, and PostgreSQL 16 has no MAINTAIN to give back.
func TestGrantsWithOptions_AnOwnersRevokedPrivilegeIsGrantedBack(t *testing.T) {
	tests := []struct {
		name string
		read []catalog.Grant
		want []difftypes.GrantRef
	}{
		{
			name: "the same owner holds it there",
			read: []catalog.Grant{ownerSelect},
			want: []difftypes.GrantRef{{Role: "owner", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"}},
		},
		{
			name: "the same owner gave it up there too",
			read: []catalog.Grant{ownerLost},
		},
		{
			name: "another role owns the object there",
			read: []catalog.Grant{{Role: "other", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t", Implicit: true}},
		},
		{
			name: "the object is not there",
			read: []catalog.Grant{{Role: "owner", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "other", Implicit: true}},
		},
		{
			name: "the privilege does not exist there",
			read: []catalog.Grant{ownerUpdate},
		},
		{
			name: "a plain grant is not what an owner holds by default",
			read: []catalog.Grant{{Role: "owner", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareOwnerGrants(&schemamodel.Database{}, []catalog.Grant{ownerLost},
				compare.GrantOptions{DesiredComplete: true, DesiredRead: test.read})

			c.Assert(diff.GrantsAdded, qt.DeepEquals, test.want)
			c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
		})
	}
}

// TestGrantsWithOptions_ADeclarationSaysNothingAboutARevokedPrivilege is a file
// that does not name the owner. It makes no claim about what the owner may do, so
// it asks for nothing to be given back.
func TestGrantsWithOptions_ADeclarationSaysNothingAboutARevokedPrivilege(t *testing.T) {
	c := qt.New(t)

	diff := compareOwnerGrants(declaresOwner(), []catalog.Grant{ownerLost, ownerUpdate}, compare.GrantOptions{})

	c.Assert(diff.GrantsAdded, qt.HasLen, 0)
	c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
}

// TestGrantsWithOptions_APartialRevokeOfAGrantIsNotGrantedBack is the row ClickHouse
// and SQL Server report, which subtracts from a grant rather than from ownership.
// Ptah has no shape for it, on either side of a comparison.
func TestGrantsWithOptions_APartialRevokeOfAGrantIsNotGrantedBack(t *testing.T) {
	c := qt.New(t)
	partial := catalog.Grant{Role: "owner", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t", IsPartialRevoke: true}

	diff := compareOwnerGrants(&schemamodel.Database{}, []catalog.Grant{partial},
		compare.GrantOptions{DesiredComplete: true, DesiredRead: []catalog.Grant{ownerSelect}})

	c.Assert(diff.GrantsAdded, qt.HasLen, 0)
	c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
}

// TestCurrentGrants_LeavesOutWhatANewObjectHolds is the list a plan gives back to
// an object it drops and creates again. The owner of the new object holds its
// default privileges without a grant, and a routine's implicit rows stay: they say
// whether PUBLIC could execute it.
func TestCurrentGrants_LeavesOutWhatANewObjectHolds(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{Grants: []catalog.Grant{
		ownerSelect,
		ownerLost,
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"},
		{Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "FUNCTION", ObjectName: "f", Implicit: true},
	}}

	c.Assert(compare.CurrentGrants(database), qt.DeepEquals, []difftypes.GrantRef{
		{Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "FUNCTION", ObjectName: "f"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"},
	})
}
