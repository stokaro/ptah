package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// A role the schema does not manage keeps a grant option the declaration
// does not give it, unless the schema says otherwise. A declared REVOKE GRANT
// OPTION FOR says so ([schemamodel.Grant.GrantOptionRevoked]): the comparison
// takes the option the database holds and keeps the privilege. A GRANT
// without the option alone keeps the policy, a role that holds no option has
// nothing to take, and a later GRANT WITH GRANT OPTION in the file gives the
// option back. The option goes from the privileges the revoke names and no
// others, and a revoke of it for ALL takes it from each privilege ALL grants,
// MAINTAIN included, which a PostgreSQL 16 server does not have.
func TestGrants_TakesAGrantOptionTheSchemaRevokes(t *testing.T) {
	selectOnUsers := schemamodel.Grant{Role: "u48", Privileges: []string{"SELECT"}, OnTable: "users"}
	optionRevoked := selectOnUsers
	optionRevoked.GrantOptionRevoked = true
	optionGiven := selectOnUsers
	optionGiven.WithOption = true
	held := func(withOption bool) []catalog.Grant {
		return []catalog.Grant{{Role: "u48", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users", WithOption: withOption}}
	}
	insertOnUsers := schemamodel.Grant{Role: "u48", Privileges: []string{"INSERT"}, OnTable: "users"}
	grantable := func(privileges ...string) []catalog.Grant {
		grants := make([]catalog.Grant, 0, len(privileges))
		for _, privilege := range privileges {
			grants = append(grants, catalog.Grant{Role: "u48", Privilege: privilege, ObjectType: "TABLE", ObjectName: "users", WithOption: true})
		}
		return grants
	}
	revoked := func(privileges ...string) []difftypes.GrantRef {
		refs := make([]difftypes.GrantRef, 0, len(privileges))
		for _, privilege := range privileges {
			refs = append(refs, difftypes.GrantRef{Role: "u48", Privilege: privilege, ObjectType: "TABLE", ObjectName: "users", WithOption: true})
		}
		return refs
	}
	allOnUsers := schemamodel.Grant{Role: "u48", Privileges: []string{"ALL"}, OnTable: "users"}
	allOptionRevoked := allOnUsers
	allOptionRevoked.GrantOptionRevoked = true
	everyTablePrivilege := []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"}
	tests := []struct {
		name        string
		roles       []schemamodel.Role
		grants      []schemamodel.Grant
		held        []catalog.Grant
		wantRevoked []difftypes.GrantRef
	}{
		{
			name:   "the option revoked from a role the schema does not manage",
			grants: []schemamodel.Grant{optionRevoked},
			held:   held(true),
			wantRevoked: []difftypes.GrantRef{
				{Role: "u48", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users", WithOption: true},
			},
		},
		{
			name:   "the option revoked from a role that holds none",
			grants: []schemamodel.Grant{optionRevoked},
			held:   held(false),
		},
		{
			name:   "a grant without the option",
			grants: []schemamodel.Grant{selectOnUsers},
			held:   held(true),
		},
		{
			name:   "the option granted again after the revoke",
			grants: []schemamodel.Grant{optionRevoked, optionGiven},
			held:   held(true),
		},
		{
			name:        "the option revoked on one of two privileges the declaration grants",
			grants:      []schemamodel.Grant{optionRevoked, insertOnUsers},
			held:        grantable("SELECT", "INSERT"),
			wantRevoked: revoked("SELECT"),
		},
		{
			name:        "the option revoked for ALL where the database holds MAINTAIN",
			grants:      []schemamodel.Grant{allOptionRevoked},
			held:        grantable(everyTablePrivilege...),
			wantRevoked: revoked("DELETE", "INSERT", "MAINTAIN", "REFERENCES", "SELECT", "TRIGGER", "TRUNCATE", "UPDATE"),
		},
		{
			name:        "ALL without the option for a role the schema manages, where the database holds MAINTAIN",
			roles:       []schemamodel.Role{{Name: "u48"}},
			grants:      []schemamodel.Grant{allOnUsers},
			held:        grantable(everyTablePrivilege...),
			wantRevoked: revoked("DELETE", "INSERT", "MAINTAIN", "REFERENCES", "SELECT", "TRIGGER", "TRUNCATE", "UPDATE"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.Grants(&schemamodel.Database{Roles: test.roles, Grants: test.grants}, &catalog.Database{Grants: test.held}, diff)

			c.Assert(diff.GrantOptionsRevoked, qt.DeepEquals, test.wantRevoked)
			c.Assert(diff.GrantsAdded, qt.HasLen, 0)
			c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
			c.Assert(diff.GrantOptionsAdded, qt.HasLen, 0)
		})
	}
}
