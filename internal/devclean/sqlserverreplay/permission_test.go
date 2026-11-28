package sqlserverreplay_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/devclean/sqlserverreplay"
)

// rolesDropped is the scope of a run whose every cleanup drops the database
// roles the claim did not find, and whose claim found one.
var rolesDropped = replay.Scope{DatabaseRoles: replay.DatabaseRoleScope{Dropped: true, Kept: []string{"Kept Role"}}}

// permissionMutation is the refusal of a permission change the cleanup does
// not undo.
const permissionMutation = `sqlserver migration replay rejects permission mutation because its effects cannot be confined to the disposable database realm.*`

// validateAs validates statement with the guard of a dev session in ptah_dev
// whose database user is dev_user.
func validateAs(c *qt.C, statement string, scope replay.Scope) error {
	c.Helper()
	return guardFor(c, databaseSession{database: "ptah_dev", user: "dev_user", asked: new([]string)}).Validate(statement, scope)
}

// TestGuard_PermissionChangeOfARoleTheRunCreated_HappyPath pins the GRANT,
// DENY and REVOKE a rehearsal runs: each grantee is a role the run created,
// which every cleanup drops with every permission granted or denied to it, on
// an object, a schema or the database.
func TestGuard_PermissionChangeOfARoleTheRunCreated_HappyPath(t *testing.T) {
	statements := []string{
		`GRANT SELECT ON [t1] TO [APP];`,
		`GRANT SELECT, INSERT ON dbo.t1 TO [App Readers], app_writers WITH GRANT OPTION`,
		`GRANT SELECT ([id], [name]) ON [dbo].[t1] TO [APP]`,
		`GRANT SELECT ON SCHEMA::[app] TO [APP]`,
		`GRANT EXECUTE ON OBJECT::[dbo].[f] TO [APP]`,
		`GRANT CREATE TABLE TO [APP]`,
		`DENY UPDATE ON [t1] TO [APP] CASCADE`,
		`REVOKE GRANT OPTION FOR SELECT ON [t1] FROM [APP] CASCADE AS [dbo];`,
		`revoke select on t1 to app`,
	}
	for _, statement := range statements {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(validateAs(c, statement, rolesDropped), qt.IsNil)
		})
	}
}

// TestGuard_PermissionChangeOfARoleTheRunCreated_FailurePath pins the
// permission changes a rehearsal still refuses, with trust as well: every one
// where no cleanup drops the run's roles, and every one naming a principal
// that outlives the run, recording another grantor, or written in a shape the
// guard does not read.
func TestGuard_PermissionChangeOfARoleTheRunCreated_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		scope     replay.Scope
		statement string
	}{
		{"no cleanup drops the roles", replay.Scope{}, `GRANT SELECT ON [t1] TO [APP]`},
		{"no cleanup drops the roles, under trust", replay.Scope{TrustedSQL: true}, `GRANT SELECT ON [t1] TO [APP]`},
		{"a role the claim found", rolesDropped, `GRANT SELECT ON [t1] TO [Kept Role]`},
		{"a role the claim found, in another case", rolesDropped, `DENY SELECT ON [t1] TO [KEPT ROLE]`},
		{"one grantee of two the claim found", rolesDropped, `GRANT SELECT ON [t1] TO [APP], [Kept Role]`},
		{"public", rolesDropped, `GRANT SELECT ON [t1] TO [public]`},
		{"PUBLIC", rolesDropped, `REVOKE SELECT ON [t1] FROM PUBLIC`},
		{"dbo", rolesDropped, `GRANT SELECT ON [t1] TO dbo`},
		{"guest", rolesDropped, `GRANT CONNECT TO [guest]`},
		{"a fixed role", rolesDropped, `GRANT SELECT ON [t1] TO [db_datareader]`},
		{"a name beginning with ##", rolesDropped, `GRANT SELECT ON [t1] TO [##MS_Reader##]`},
		{"the session's user", rolesDropped, `GRANT CREATE TABLE TO [DEV_USER]`},
		{"a grantor on a GRANT", rolesDropped, `GRANT SELECT ON [t1] TO [APP] AS [Readers]`},
		{"a grantor on a DENY", rolesDropped, `DENY SELECT ON [t1] TO [APP] CASCADE AS [Readers]`},
		{"no grantee", rolesDropped, `GRANT SELECT ON [t1]`},
		{"a list ending in a comma", rolesDropped, `GRANT SELECT ON [t1] TO [APP],`},
		{"words after the grantees", rolesDropped, `GRANT SELECT ON [t1] TO [APP] [Readers]`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(validateAs(c, test.statement, test.scope), qt.ErrorMatches, permissionMutation)
		})
	}
}

// TestGuard_PermissionChangeWithoutASession pins that a guard that read no
// session refuses a grant to a role the run created: it does not know the
// session's user, which outlives the run.
func TestGuard_PermissionChangeWithoutASession(t *testing.T) {
	c := qt.New(t)
	err := sqlserverreplay.Services()[0].Guard.Validate(`GRANT SELECT ON [t1] TO [APP]`, rolesDropped)
	c.Assert(err, qt.ErrorMatches, permissionMutation)
}
