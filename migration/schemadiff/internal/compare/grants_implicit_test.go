package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestGrants_KeepsAManagedRolesImplicitPrivilege pins that a privilege a role
// holds by default is not revoked because a declaration leaves it out.
//
// CockroachDB reports a routine's owner as holding EXECUTE, and keeps it
// through REVOKE ALL, so the reader marks the row implicit
// (stokaro/ptah#3815). Revoking it from a managed owner the declaration did not
// name would be planned again on every run. The explicit row is the inverse:
// the same privilege, written by a GRANT, is revoked.
func TestGrants_KeepsAManagedRolesImplicitPrivilege(t *testing.T) {
	tests := []struct {
		name        string
		implicit    bool
		wantRemoved int
	}{
		{name: "implicit", implicit: true, wantRemoved: 0},
		{name: "explicit", implicit: false, wantRemoved: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app_owner"}}}
			database := &catalog.Database{
				Roles: []catalog.Role{{Name: "app_owner"}},
				Grants: []catalog.Grant{{
					Role: "app_owner", Privilege: "EXECUTE", ObjectType: "FUNCTION",
					Schema: "public", ObjectName: "f", Implicit: test.implicit,
				}},
			}
			diff := &difftypes.SchemaDiff{}

			compare.GrantsWithSemantics(desired, database, diff, identifier.ForDialect("postgres"))

			c.Assert(diff.GrantsRemoved, qt.HasLen, test.wantRemoved)
		})
	}
}

// TestGrants_RevokesAnImplicitPrivilegeTheDeclarationRevokes is the other
// half: a declaration that revokes the privilege still reaches the implicit
// row, which is how REVOKE EXECUTE ... FROM PUBLIC takes back a routine's
// default.
func TestGrants_RevokesAnImplicitPrivilegeTheDeclarationRevokes(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		RevokedGrants: []schemamodel.Grant{{
			Role: "PUBLIC", Privileges: []string{"EXECUTE"}, OnRoutine: "f", RoutineKind: "FUNCTION",
		}},
	}
	database := &catalog.Database{
		Grants: []catalog.Grant{{
			Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "FUNCTION",
			Schema: "public", ObjectName: "f", Implicit: true,
		}},
	}
	diff := &difftypes.SchemaDiff{}

	compare.GrantsWithSemantics(desired, database, diff, identifier.ForDialect("postgres"))

	c.Assert(diff.GrantsRemoved, qt.HasLen, 1)
	c.Assert(diff.GrantsRemoved[0].Role, qt.Equals, "PUBLIC")
}
