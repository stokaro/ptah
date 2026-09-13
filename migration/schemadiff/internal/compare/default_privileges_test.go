package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// defaultPrivilegeNames projects planned default privileges onto the privilege
// and the four components that identify the object holding it, which is the
// whole of what these tests are about. Stating the projected list rather than a
// length also says more: a length check on a case that plans one change only
// says that it planned one.
func defaultPrivilegeNames(refs []difftypes.DefaultPrivilegeRef) []string {
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		names = append(names, ref.String())
	}
	return names
}

// TestDefaultPrivilegesWithSemantics_TwoGrantorsAreTwoObjects is the refusal.
//
// PostgreSQL keys pg_default_acl on the grantor and refuses the statement from
// a non-member of that role, so `FOR ROLE alpha_owner` and `FOR ROLE
// beta_owner` describe two objects even when everything else agrees. A
// comparator that left the grantor out of the identity would key both
// declarations to one map entry and keep whichever arrived second: one role's
// privileges never issued, and against a database holding the other role's row,
// nothing revoked either.
func TestDefaultPrivilegesWithSemantics_TwoGrantorsAreTwoObjects(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		DefaultPrivileges: []schemamodel.DefaultPrivilege{
			{
				Grantor: "alpha_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
			},
			{
				Grantor: "beta_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
			},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.DefaultPrivilegesWithSemantics(
		desired, &catalog.Database{}, diff, identifier.ForDialect(platform.Postgres))

	c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesAdded), qt.DeepEquals, []string{
		"SELECT on TABLES in app for alpha_owner to app_reader",
		"SELECT on TABLES in app for beta_owner to app_reader",
	})
}

// TestDefaultPrivilegesWithSemantics_AMatchingDeclarationPlansNothing is the
// control for the refusal above.
//
// Two adds prove the grantors are told apart only if one declaration matching
// its own row plans nothing. Without this, a comparator that planned every
// declared privilege regardless of the database would satisfy the refusal and
// re-issue every default privilege on every run.
func TestDefaultPrivilegesWithSemantics_AMatchingDeclarationPlansNothing(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Roles: []schemamodel.Role{{Name: "alpha_owner"}},
		DefaultPrivileges: []schemamodel.DefaultPrivilege{{
			Grantor: "alpha_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader",
			Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
		}},
	}
	database := &catalog.Database{DefaultPrivileges: []catalog.DefaultPrivilege{{
		Grantor: "alpha_owner", Schema: "app", ObjectType: "TABLES",
		Grantee: "app_reader", Privilege: "SELECT",
	}}}
	diff := &difftypes.SchemaDiff{}

	compare.DefaultPrivilegesWithSemantics(
		desired, database, diff, identifier.ForDialect(platform.Postgres))

	c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesAdded), qt.HasLen, 0)
	c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesRemoved), qt.HasLen, 0)
	c.Assert(diff.DefaultPrivilegeOptionsAdded, qt.HasLen, 0)
	c.Assert(diff.DefaultPrivilegeOptionsRevoked, qt.HasLen, 0)
}

// TestDefaultPrivilegesWithSemantics_APublicGranteeIsRevocable pins the removal
// gate to the grantor rather than to the grantee.
//
// pg_default_acl records grantee 0 as PUBLIC, and no declaration creates PUBLIC
// as a role. A gate copied from the grant comparator asks whether the RECEIVING
// role is one the declaration manages, so it can never plan the removal of a
// default privilege granted to PUBLIC -- the grantee that hands the privilege
// to every role on the server, and the one most worth being able to take back.
func TestDefaultPrivilegesWithSemantics_APublicGranteeIsRevocable(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app_owner"}}}
	database := &catalog.Database{DefaultPrivileges: []catalog.DefaultPrivilege{{
		Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
		Grantee: "PUBLIC", Privilege: "SELECT",
	}}}
	diff := &difftypes.SchemaDiff{}

	compare.DefaultPrivilegesWithSemantics(
		desired, database, diff, identifier.ForDialect(platform.Postgres))

	c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesRemoved), qt.DeepEquals, []string{
		"SELECT on TABLES in app for app_owner to PUBLIC",
	})
}

// TestDefaultPrivilegesWithSemantics_ADeclaredObjectIsTrimmedForAnyGrantee is
// the second way a removal is reached, and it needs no managed role at all: the
// declaration names the object, so a privilege it leaves out is one the author
// took away.
func TestDefaultPrivilegesWithSemantics_ADeclaredObjectIsTrimmedForAnyGrantee(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		DefaultPrivileges: []schemamodel.DefaultPrivilege{{
			Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "PUBLIC",
			Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
		}},
	}
	database := &catalog.Database{DefaultPrivileges: []catalog.DefaultPrivilege{
		{
			Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
			Grantee: "PUBLIC", Privilege: "SELECT",
		},
		{
			Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
			Grantee: "PUBLIC", Privilege: "INSERT",
		},
	}}
	diff := &difftypes.SchemaDiff{}

	compare.DefaultPrivilegesWithSemantics(
		desired, database, diff, identifier.ForDialect(platform.Postgres))

	c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesRemoved), qt.DeepEquals, []string{
		"INSERT on TABLES in app for app_owner to PUBLIC",
	})
}

// TestDefaultPrivilegesWithSemantics_AnUndeclaredGrantorIsLeftAlone is the
// control for both removal cases above.
//
// A gate that planned every described row would satisfy them while revoking
// defaults that belong to a role nobody declared -- which on a shared server is
// somebody else's access control.
func TestDefaultPrivilegesWithSemantics_AnUndeclaredGrantorIsLeftAlone(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{DefaultPrivileges: []catalog.DefaultPrivilege{{
		Grantor: "other_owner", Schema: "app", ObjectType: "TABLES",
		Grantee: "PUBLIC", Privilege: "SELECT",
	}}}
	diff := &difftypes.SchemaDiff{}

	compare.DefaultPrivilegesWithSemantics(
		&schemamodel.Database{}, database, diff, identifier.ForDialect(platform.Postgres))

	c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesRemoved), qt.HasLen, 0)
}

// TestDefaultPrivilegesWithSemantics_AGrantOptionIsAdded pins that grantability
// is a change to the privilege the database already holds rather than a second
// privilege to grant.
//
// The catalog records is_grantable per privilege, so one identity can hold a
// grantable privilege beside a plain one. A comparison keyed on grantability
// would read the declared grantable SELECT and the database's plain SELECT as
// two entries, and plan a GRANT beside a REVOKE for one row.
func TestDefaultPrivilegesWithSemantics_AGrantOptionIsAdded(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		DefaultPrivileges: []schemamodel.DefaultPrivilege{{
			Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader",
			Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT", WithOption: true}},
		}},
	}
	database := &catalog.Database{DefaultPrivileges: []catalog.DefaultPrivilege{{
		Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
		Grantee: "app_reader", Privilege: "SELECT",
	}}}
	diff := &difftypes.SchemaDiff{}

	compare.DefaultPrivilegesWithSemantics(
		desired, database, diff, identifier.ForDialect(platform.Postgres))

	c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegeOptionsAdded), qt.DeepEquals, []string{
		"SELECT on TABLES in app for app_owner to app_reader",
	})
	c.Assert(diff.DefaultPrivilegesAdded, qt.HasLen, 0)
	c.Assert(diff.DefaultPrivilegesRemoved, qt.HasLen, 0)
}

// TestDefaultPrivilegesWithSemantics_AGrantOptionIsRevoked is the same change
// read the other way, and the entry carries the database's spelling because the
// grant option being taken away is the one the server holds.
func TestDefaultPrivilegesWithSemantics_AGrantOptionIsRevoked(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		DefaultPrivileges: []schemamodel.DefaultPrivilege{{
			Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader",
			Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
		}},
	}
	database := &catalog.Database{DefaultPrivileges: []catalog.DefaultPrivilege{{
		Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
		Grantee: "app_reader", Privilege: "SELECT", WithOption: true,
	}}}
	diff := &difftypes.SchemaDiff{}

	compare.DefaultPrivilegesWithSemantics(
		desired, database, diff, identifier.ForDialect(platform.Postgres))

	c.Assert(diff.DefaultPrivilegeOptionsRevoked, qt.DeepEquals, []difftypes.DefaultPrivilegeRef{{
		Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
		Grantee: "app_reader", Privilege: "SELECT", WithOption: true,
	}})
	c.Assert(diff.DefaultPrivilegesRemoved, qt.HasLen, 0)
}

// TestDefaultPrivilegesWithSemantics_PostgresKeepsTwoSpellingsApart pins what
// the identity fold answers on the one target that has the statement.
//
// The identifier-shaped components go through the target's own rule, and on
// PostgreSQL that rule is exact comparison. It has to be: the renderer quotes
// every role and schema name it writes, so a declared `App_Reader` is stored
// with its case and is a different role from `app_reader`.
func TestDefaultPrivilegesWithSemantics_PostgresKeepsTwoSpellingsApart(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Roles: []schemamodel.Role{{Name: "app_owner"}},
		DefaultPrivileges: []schemamodel.DefaultPrivilege{{
			Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "App_Reader",
			Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
		}},
	}
	database := &catalog.Database{DefaultPrivileges: []catalog.DefaultPrivilege{{
		Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
		Grantee: "app_reader", Privilege: "SELECT",
	}}}
	diff := &difftypes.SchemaDiff{}

	compare.DefaultPrivilegesWithSemantics(
		desired, database, diff, identifier.ForDialect(platform.Postgres))

	c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesAdded), qt.DeepEquals, []string{
		"SELECT on TABLES in app for app_owner to App_Reader",
	})
	c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesRemoved), qt.DeepEquals, []string{
		"SELECT on TABLES in app for app_owner to app_reader",
	})
}

// TestDefaultPrivilegesWithSemantics_TheOutputOrderIsNotTheInputOrder holds the
// sort.
//
// Each list is built by ranging over a map, so an unsorted comparator does not
// fail -- it flakes, and writes a different migration file for the same two
// schemas on the next run. Declaring the same objects in the opposite order is
// the half of that a single run can measure.
func TestDefaultPrivilegesWithSemantics_TheOutputOrderIsNotTheInputOrder(t *testing.T) {
	declared := []schemamodel.DefaultPrivilege{
		{
			Grantor: "beta_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader",
			Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
		},
		{
			Grantor: "alpha_owner", Schema: "app", ObjectType: "SEQUENCES", Grantee: "app_writer",
			Privileges: []schemamodel.PrivilegeGrant{{Privilege: "USAGE"}, {Privilege: "SELECT"}},
		},
		{
			Grantor: "alpha_owner", Schema: "reporting", ObjectType: "TABLES", Grantee: "PUBLIC",
			Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
		},
	}
	want := []string{
		"SELECT on SEQUENCES in app for alpha_owner to app_writer",
		"USAGE on SEQUENCES in app for alpha_owner to app_writer",
		"SELECT on TABLES in app for beta_owner to app_reader",
		"SELECT on TABLES in reporting for alpha_owner to PUBLIC",
	}

	tests := []struct {
		name    string
		desired []schemamodel.DefaultPrivilege
	}{
		{name: "as declared", desired: declared},
		{name: "declared in the opposite order", desired: []schemamodel.DefaultPrivilege{
			declared[2], declared[1], declared[0],
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.DefaultPrivilegesWithSemantics(
				&schemamodel.Database{DefaultPrivileges: test.desired},
				&catalog.Database{},
				diff,
				identifier.ForDialect(platform.Postgres),
			)

			c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesAdded), qt.DeepEquals, want)
		})
	}
}

// TestDefaultPrivilegesWithSemantics_TwoDeclarationsOfOneObjectMerge pins what
// happens when two declarations resolve to one entry.
//
// The map would keep whichever arrived last, and which one that is depends on
// the order the document was written in. PostgreSQL merges instead: granting
// SELECT and then SELECT WITH GRANT OPTION leaves one grantable row, so the
// grantable spelling wins here too and the comparison plans one change.
func TestDefaultPrivilegesWithSemantics_TwoDeclarationsOfOneObjectMerge(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		DefaultPrivileges: []schemamodel.DefaultPrivilege{
			{
				Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT", WithOption: true}},
			},
			{
				Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
			},
		},
	}
	database := &catalog.Database{DefaultPrivileges: []catalog.DefaultPrivilege{{
		Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
		Grantee: "app_reader", Privilege: "SELECT",
	}}}
	diff := &difftypes.SchemaDiff{}

	compare.DefaultPrivilegesWithSemantics(
		desired, database, diff, identifier.ForDialect(platform.Postgres))

	c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegeOptionsAdded), qt.DeepEquals, []string{
		"SELECT on TABLES in app for app_owner to app_reader",
	})
	c.Assert(diff.DefaultPrivilegeOptionsRevoked, qt.HasLen, 0)
}
