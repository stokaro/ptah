package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestDefaultPrivilegesWithSemantics_TheGlobalDefaultStartsFromTheBuiltInOne
// holds the comparison of the global default, ALTER DEFAULT PRIVILEGES without
// IN SCHEMA, to the built-in default it starts from (stokaro/ptah#3772).
//
// The read reports a global row as its difference from the built-in default,
// so a database with no row holds the built-in default whole. A comparison
// that did not know it would plan a declared revoke of PUBLIC's EXECUTE
// nowhere, because no row says PUBLIC holds it, and would plan a declared
// grant of it on every run, because no row says it is held.
func TestDefaultPrivilegesWithSemantics_TheGlobalDefaultStartsFromTheBuiltInOne(t *testing.T) {
	revokePublicExecute := schemamodel.DefaultPrivilege{
		Grantor: "app_owner", ObjectType: "FUNCTIONS", Grantee: "PUBLIC", Revoked: []string{"EXECUTE"},
	}
	publicExecuteRevoked := catalog.DefaultPrivilege{
		Grantor: "app_owner", ObjectType: "FUNCTIONS", Grantee: "PUBLIC", Privilege: "EXECUTE", Revoked: true,
	}
	tests := []struct {
		name        string
		desired     schemamodel.Database
		current     []catalog.DefaultPrivilege
		wantAdded   []string
		wantRemoved []string
	}{
		{
			name:        "a revoke of PUBLIC's EXECUTE against a database with no row",
			desired:     schemamodel.Database{DefaultPrivileges: []schemamodel.DefaultPrivilege{revokePublicExecute}},
			wantAdded:   make([]string, 0),
			wantRemoved: []string{"EXECUTE on FUNCTIONS in every schema for app_owner to PUBLIC"},
		},
		{
			name:        "the same revoke against a database that made it",
			desired:     schemamodel.Database{DefaultPrivileges: []schemamodel.DefaultPrivilege{revokePublicExecute}},
			current:     []catalog.DefaultPrivilege{publicExecuteRevoked},
			wantAdded:   make([]string, 0),
			wantRemoved: make([]string, 0),
		},
		{
			name: "a grant of a built-in privilege against a database with no row",
			desired: schemamodel.Database{DefaultPrivileges: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", ObjectType: "FUNCTIONS", Grantee: "PUBLIC",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "EXECUTE"}},
			}}},
			wantAdded:   make([]string, 0),
			wantRemoved: make([]string, 0),
		},
		{
			name:        "a built-in privilege a managed grantor took away is granted back",
			desired:     schemamodel.Database{Roles: []schemamodel.Role{{Name: "app_owner"}}},
			current:     []catalog.DefaultPrivilege{publicExecuteRevoked},
			wantAdded:   []string{"EXECUTE on FUNCTIONS in every schema for app_owner to PUBLIC"},
			wantRemoved: make([]string, 0),
		},
		{
			name:        "a built-in privilege a grantor nobody declared took away is left alone",
			current:     []catalog.DefaultPrivilege{publicExecuteRevoked},
			wantAdded:   make([]string, 0),
			wantRemoved: make([]string, 0),
		},
		{
			name: "a revoked ALL of the owner's own privileges is one statement",
			desired: schemamodel.Database{DefaultPrivileges: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", ObjectType: "TABLES", Grantee: "app_owner", Revoked: []string{"ALL"},
			}}},
			wantAdded:   make([]string, 0),
			wantRemoved: []string{"ALL on TABLES in every schema for app_owner to app_owner"},
		},
		{
			// REVOKE ALL takes what is left, and on CockroachDB it also takes
			// the privileges a list of PostgreSQL's cannot name.
			name: "a revoked ALL once part of it is gone is still one statement",
			desired: schemamodel.Database{DefaultPrivileges: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", ObjectType: "SEQUENCES", Grantee: "app_owner", Revoked: []string{"ALL"},
			}}},
			current: []catalog.DefaultPrivilege{{
				Grantor: "app_owner", ObjectType: "SEQUENCES", Grantee: "app_owner", Privilege: "UPDATE", Revoked: true,
			}},
			wantAdded:   make([]string, 0),
			wantRemoved: []string{"ALL on SEQUENCES in every schema for app_owner to app_owner"},
		},
		{
			// The SQL schema reader spells REVOKE ALL as the list, and
			// CockroachDB records it as ALL.
			name: "a revoke of every privilege from the owner against a database that revoked ALL",
			desired: schemamodel.Database{DefaultPrivileges: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", ObjectType: "SEQUENCES", Grantee: "app_owner",
				Revoked: []string{"USAGE", "SELECT", "UPDATE"},
			}}},
			current: []catalog.DefaultPrivilege{{
				Grantor: "app_owner", ObjectType: "SEQUENCES", Grantee: "app_owner", Privilege: "ALL", Revoked: true,
			}},
			wantAdded:   make([]string, 0),
			wantRemoved: make([]string, 0),
		},
		{
			name: "a revoke of every privilege from the owner against a database with no row",
			desired: schemamodel.Database{DefaultPrivileges: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", ObjectType: "SEQUENCES", Grantee: "app_owner",
				Revoked: []string{"USAGE", "SELECT", "UPDATE"},
			}}},
			wantAdded:   make([]string, 0),
			wantRemoved: []string{"ALL on SEQUENCES in every schema for app_owner to app_owner"},
		},
		{
			// PostgreSQL 16 and YugabyteDB have no MAINTAIN, so a read there
			// never reports it taken away.
			name: "a revoked ALL against a database that took every privilege but MAINTAIN",
			desired: schemamodel.Database{DefaultPrivileges: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", ObjectType: "TABLES", Grantee: "app_owner", Revoked: []string{"ALL"},
			}}},
			current:     ownerRevoked("TABLES", "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"),
			wantAdded:   make([]string, 0),
			wantRemoved: make([]string, 0),
		},
		{
			name: "a declared ALL for the owner against a database that took part of it",
			desired: schemamodel.Database{DefaultPrivileges: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", ObjectType: "TABLES", Grantee: "app_owner",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "ALL"}},
			}}},
			current:     ownerRevoked("TABLES", "DELETE"),
			wantAdded:   []string{"ALL on TABLES in every schema for app_owner to app_owner"},
			wantRemoved: make([]string, 0),
		},
		{
			// What CockroachDB reports for an owner who revoked everything.
			name: "a revoked ALL against a database that revoked ALL",
			desired: schemamodel.Database{DefaultPrivileges: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", ObjectType: "TABLES", Grantee: "app_owner", Revoked: []string{"ALL"},
			}}},
			current: []catalog.DefaultPrivilege{{
				Grantor: "app_owner", ObjectType: "TABLES", Grantee: "app_owner", Privilege: "ALL", Revoked: true,
			}},
			wantAdded:   make([]string, 0),
			wantRemoved: make([]string, 0),
		},
		{
			name: "a grant beyond the built-in default against a database with no row",
			desired: schemamodel.Database{DefaultPrivileges: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", ObjectType: "TABLES", Grantee: "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
			}}},
			wantAdded:   []string{"SELECT on TABLES in every schema for app_owner to app_reader"},
			wantRemoved: make([]string, 0),
		},
		{
			name:    "a grant a managed grantor holds and nothing declares",
			desired: schemamodel.Database{Roles: []schemamodel.Role{{Name: "app_owner"}}},
			current: []catalog.DefaultPrivilege{{
				Grantor: "app_owner", ObjectType: "SCHEMAS", Grantee: "app_reader", Privilege: "USAGE",
			}},
			wantAdded:   make([]string, 0),
			wantRemoved: []string{"USAGE on SCHEMAS in every schema for app_owner to app_reader"},
		},
		{
			// A schema-scoped revoke of a built-in privilege takes nothing
			// away, so there is nothing to plan against an empty database.
			name: "a schema-scoped revoke of a built-in privilege",
			desired: schemamodel.Database{DefaultPrivileges: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", Schema: "app", ObjectType: "FUNCTIONS", Grantee: "PUBLIC",
				Revoked: []string{"EXECUTE"},
			}}},
			wantAdded:   make([]string, 0),
			wantRemoved: make([]string, 0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.DefaultPrivilegesWithSemantics(&test.desired,
				&catalog.Database{DefaultPrivileges: test.current}, diff,
				identifier.ForDialect(platform.Postgres), compare.Coverage{})

			c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesAdded), qt.DeepEquals, test.wantAdded)
			c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesRemoved), qt.DeepEquals, test.wantRemoved)
		})
	}
}

// TestDefaultPrivilegesWithSemantics_AGrantOptionOnABuiltInPrivilegeIsAdded
// holds a built-in privilege as a row without the grant option: declaring it
// WITH GRANT OPTION plans the option, not the privilege.
func TestDefaultPrivilegesWithSemantics_AGrantOptionOnABuiltInPrivilegeIsAdded(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{DefaultPrivileges: []schemamodel.DefaultPrivilege{{
		Grantor: "app_owner", ObjectType: "TYPES", Grantee: "PUBLIC",
		Privileges: []schemamodel.PrivilegeGrant{{Privilege: "USAGE", WithOption: true}},
	}}}
	diff := &difftypes.SchemaDiff{}

	compare.DefaultPrivilegesWithSemantics(desired, &catalog.Database{}, diff,
		identifier.ForDialect(platform.Postgres), compare.Coverage{})

	c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesAdded), qt.HasLen, 0)
	c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegeOptionsAdded), qt.DeepEquals, []string{
		"USAGE on TYPES in every schema for app_owner to PUBLIC",
	})
}

// ownerRevoked is what the read reports for a global default of app_owner that
// took privileges of objectType away from app_owner itself.
func ownerRevoked(objectType string, privileges ...string) []catalog.DefaultPrivilege {
	revoked := make([]catalog.DefaultPrivilege, 0, len(privileges))
	for _, privilege := range privileges {
		revoked = append(revoked, catalog.DefaultPrivilege{
			Grantor: "app_owner", ObjectType: objectType, Grantee: "app_owner", Privilege: privilege, Revoked: true,
		})
	}
	return revoked
}

// TestDefaultPrivilegesWithSemantics_AnOwnerPartTheReadCouldNotDescribe plans
// the owner's part of a global default that the CockroachDB read reported as
// undescribed: the owner holds some of its own privileges and the read cannot
// say which.
//
// REVOKE ALL and GRANT ALL converge from there whatever the owner holds, and
// each leaves a state the read describes, so they are planned. A change to
// part of it is withheld and named as undecided: planned, it would run on every
// apply, because the read never shows it done. The grant to another role on the
// same class is the control in each row: the rule is about the owner's part,
// not the class.
func TestDefaultPrivilegesWithSemantics_AnOwnerPartTheReadCouldNotDescribe(t *testing.T) {
	readerGrant := schemamodel.DefaultPrivilege{
		Grantor: "app_owner", ObjectType: "TABLES", Grantee: "app_reader",
		Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
	}
	readerAdded := "SELECT on TABLES in every schema for app_owner to app_reader"
	tests := []struct {
		name          string
		owner         []schemamodel.DefaultPrivilege
		roles         []schemamodel.Role
		wantAdded     []string
		wantRemoved   []string
		wantUndecided []string
	}{
		{
			name: "a revoke of part of it",
			owner: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", ObjectType: "TABLES", Grantee: "app_owner", Revoked: []string{"DELETE"},
			}},
			wantAdded:     []string{readerAdded},
			wantRemoved:   make([]string, 0),
			wantUndecided: []string{"DELETE on TABLES in every schema for app_owner to app_owner"},
		},
		{
			name: "a grant of part of it",
			owner: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", ObjectType: "TABLES", Grantee: "app_owner",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
			}},
			wantAdded:     []string{readerAdded},
			wantRemoved:   make([]string, 0),
			wantUndecided: []string{"SELECT on TABLES in every schema for app_owner to app_owner"},
		},
		{
			name: "a revoke of all of it",
			owner: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", ObjectType: "TABLES", Grantee: "app_owner", Revoked: []string{"ALL"},
			}},
			wantAdded:     []string{readerAdded},
			wantRemoved:   []string{"ALL on TABLES in every schema for app_owner to app_owner"},
			wantUndecided: make([]string, 0),
		},
		{
			name: "a grant of all of it",
			owner: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", ObjectType: "TABLES", Grantee: "app_owner",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "ALL"}},
			}},
			wantAdded: []string{
				"ALL on TABLES in every schema for app_owner to app_owner",
				readerAdded,
			},
			wantRemoved:   make([]string, 0),
			wantUndecided: make([]string, 0),
		},
		{
			name:  "nothing said about it, for a grantor the declaration manages",
			roles: []schemamodel.Role{{Name: "app_owner"}},
			wantAdded: []string{
				"ALL on TABLES in every schema for app_owner to app_owner",
				readerAdded,
			},
			wantRemoved:   make([]string, 0),
			wantUndecided: make([]string, 0),
		},
		{
			name:          "nothing said about it, for a grantor nobody declared",
			wantAdded:     []string{readerAdded},
			wantRemoved:   make([]string, 0),
			wantUndecided: make([]string, 0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Roles:             test.roles,
				DefaultPrivileges: append([]schemamodel.DefaultPrivilege{readerGrant}, test.owner...),
			}
			current := &catalog.Database{UndescribedDefaultPrivileges: []catalog.UndescribedDefaultPrivilege{
				{Grantor: "app_owner", ObjectType: "TABLES"},
			}}
			cov := compare.CoverageOf(desired, current)
			diff := &difftypes.SchemaDiff{}

			compare.DefaultPrivilegesWithSemantics(desired, current, diff, identifier.ForDialect(platform.CockroachDB), cov)

			c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesAdded), qt.DeepEquals, test.wantAdded)
			c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesRemoved), qt.DeepEquals, test.wantRemoved)
			c.Assert(undecidedNames(cov.UndecidedAdditions()), qt.DeepEquals, test.wantUndecided)
		})
	}
}

// TestDefaultPrivilegesWithSemantics_AnUndecidedOwnerPartSaysWhy pins the
// record an undecided owner's part carries: the target cannot report it.
func TestDefaultPrivilegesWithSemantics_AnUndecidedOwnerPartSaysWhy(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{DefaultPrivileges: []schemamodel.DefaultPrivilege{{
		Grantor: "app_owner", ObjectType: "SEQUENCES", Grantee: "app_owner", Revoked: []string{"UPDATE"},
	}}}
	current := &catalog.Database{UndescribedDefaultPrivileges: []catalog.UndescribedDefaultPrivilege{
		{Grantor: "app_owner", ObjectType: "SEQUENCES"},
	}}
	cov := compare.CoverageOf(desired, current)

	compare.DefaultPrivilegesWithSemantics(desired, current, &difftypes.SchemaDiff{},
		identifier.ForDialect(platform.CockroachDB), cov)

	c.Assert(cov.UndecidedAdditions(), qt.DeepEquals, []coverage.Object{{
		Kind:       coverage.DefaultPrivilege,
		Name:       "UPDATE on SEQUENCES in every schema for app_owner to app_owner",
		Reason:     coverage.Unsupported,
		Provenance: coverage.DerivedFromTarget,
	}})
}

// undecidedNames lists the names of undecided objects, never nil, so a row
// can say it expects none.
func undecidedNames(objects []coverage.Object) []string {
	names := make([]string, 0, len(objects))
	for _, object := range objects {
		names = append(names, object.Name)
	}
	return names
}
