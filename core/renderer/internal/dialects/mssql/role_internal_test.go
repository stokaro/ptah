package mssql

// White-box testing required: these tests render AST nodes through the
// package's own Renderer, whose constructor and visitors are unexported. The
// statements under test are the T-SQL spellings measured against a live server,
// and reaching them from outside the package would mean going through
// core/renderer's dialect registry, which answers a different question -- that
// the registry routes to this renderer, not what this renderer emits.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
)

// TestRenderer_CreateRoleHasNoAttributes pins the T-SQL shape: a database role
// is a name and nothing else.
func TestRenderer_CreateRoleHasNoAttributes(t *testing.T) {
	c := qt.New(t)

	sql, err := New().Render(ast.NewCreateRole("app_reader"))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "CREATE ROLE [app_reader];\n")
}

// TestRenderer_CreateRoleRefusesServerLevelAttributes pins the fail-closed
// answer to a declaration a database role cannot honor.
//
// `CREATE ROLE [r] LOGIN` is `Incorrect syntax near 'LOGIN'` on 17.0.4075.5.
// Writing a comment and creating the role anyway would be a silent trap twice
// over: the author gets a principal that cannot do what they wrote, and because
// the reader can only ever report those attributes false, the comparison
// reports the same pending change on every run forever.
func TestRenderer_CreateRoleRefusesServerLevelAttributes(t *testing.T) {
	tests := []struct {
		name string
		node *ast.CreateRoleNode
		want string
	}{
		{name: "login", node: ast.NewCreateRole("r").SetLogin(true), want: "LOGIN"},
		{name: "password", node: ast.NewCreateRole("r").SetPassword("secret"), want: "a password"},
		{name: "superuser", node: ast.NewCreateRole("r").SetSuperuser(true), want: "SUPERUSER"},
		{name: "createdb", node: ast.NewCreateRole("r").SetCreateDB(true), want: "CREATEDB"},
		{name: "createrole", node: ast.NewCreateRole("r").SetCreateRole(true), want: "CREATEROLE"},
		{name: "replication", node: ast.NewCreateRole("r").SetReplication(true), want: "REPLICATION"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := New().Render(test.node)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err.Error(), qt.Contains, test.want)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestRenderer_InheritIsNotAServerLevelAttribute is the control. A renderer
// that refused every declared role would satisfy the rows above and would never
// create one -- and INHERIT is exactly what a database role does.
func TestRenderer_InheritIsNotAServerLevelAttribute(t *testing.T) {
	c := qt.New(t)

	sql, err := New().Render(ast.NewCreateRole("r").SetInherit(true))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "CREATE ROLE [r];\n")
}

// TestRenderer_AlterRoleRefusesRatherThanCommenting pins the other half of the
// permanent-diff hazard. A comment would let the plan apply, report success,
// and leave the role exactly as it was.
func TestRenderer_AlterRoleRefusesRatherThanCommenting(t *testing.T) {
	c := qt.New(t)

	_, err := New().Render(ast.NewAlterRole("r"))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err.Error(), qt.Contains, "carries no attributes to alter")
}

// TestRenderer_DropRoleKeepsIfExists pins the guard the engine accepts.
func TestRenderer_DropRoleKeepsIfExists(t *testing.T) {
	c := qt.New(t)

	sql, err := New().Render(ast.NewDropRole("r").SetIfExists())

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "DROP ROLE IF EXISTS [r];\n")
}

// TestRenderer_GrantWritesTheSchemaPrefix pins the measured hazard that has no
// error behind it.
//
// A schema grant is spelled `ON SCHEMA::[name]`. Omitting the `::` does not
// fail safely: the server resolves the name as an OBJECT, so the grant lands on
// a TABLE of that name whenever one exists, and only says `Cannot find the
// object` when none does.
func TestRenderer_GrantWritesTheSchemaPrefix(t *testing.T) {
	c := qt.New(t)

	sql, err := New().Render(ast.NewGrantPrivilege("app", "SCHEMA", "reporting", []string{"SELECT"}))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "GRANT SELECT ON SCHEMA::[reporting] TO [app];\n")
}

// TestRenderer_GrantOnATableCarriesNoSchemaPrefix is that pin's control: a
// renderer that wrote SCHEMA:: for every target would satisfy the row above and
// would grant on the wrong kind of object every time.
func TestRenderer_GrantOnATableCarriesNoSchemaPrefix(t *testing.T) {
	c := qt.New(t)

	sql, err := New().Render(ast.NewGrantPrivilege("app", "TABLE", "dbo.orders", []string{"SELECT"}))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "GRANT SELECT ON [dbo].[orders] TO [app];\n")
}

// TestRenderer_GrantReportsUsageInsteadOfEmittingIt pins the privilege T-SQL
// has no keyword for: `GRANT USAGE` is `Incorrect syntax near 'USAGE'`.
func TestRenderer_GrantReportsUsageInsteadOfEmittingIt(t *testing.T) {
	c := qt.New(t)

	sql, err := New().Render(
		ast.NewGrantPrivilege("app", "SCHEMA", "reporting", []string{"USAGE", "SELECT"}))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "T-SQL has no privilege for")
	c.Assert(sql, qt.Contains, "GRANT SELECT ON SCHEMA::[reporting] TO [app];")
	c.Assert(sql, qt.Not(qt.Contains), "GRANT USAGE")
}

// TestRenderer_GrantOfOnlyUsageEmitsNoStatement pins that a grant left with
// nothing to say emits no statement rather than `GRANT  ON ...`.
func TestRenderer_GrantOfOnlyUsageEmitsNoStatement(t *testing.T) {
	c := qt.New(t)

	sql, err := New().Render(ast.NewGrantPrivilege("app", "SCHEMA", "reporting", []string{"USAGE"}))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "T-SQL has no privilege for")
	c.Assert(sql, qt.Not(qt.Contains), "GRANT ")
}

// TestRenderer_GrantWithOptionAndItsRevocation pins both spellings of the grant
// option, including the CASCADE the revocation needs.
func TestRenderer_GrantWithOptionAndItsRevocation(t *testing.T) {
	c := qt.New(t)

	granted, err := New().Render(
		ast.NewGrantPrivilege("app", "TABLE", "dbo.orders", []string{"INSERT"}).SetWithOption(true))
	c.Assert(err, qt.IsNil)
	c.Assert(granted, qt.Equals, "GRANT INSERT ON [dbo].[orders] TO [app] WITH GRANT OPTION;\n")

	revoked, err := New().Render(
		ast.NewRevokePrivilege("app", "TABLE", "dbo.orders", []string{"INSERT"}).SetGrantOptionFor(true))
	c.Assert(err, qt.IsNil)
	c.Assert(revoked, qt.Equals,
		"REVOKE GRANT OPTION FOR INSERT ON [dbo].[orders] FROM [app] CASCADE;\n")
}

// TestRenderer_PlainRevokeCarriesNoCascade is the control on the row above: the
// CASCADE belongs to the grant-option revocation, not to every REVOKE.
func TestRenderer_PlainRevokeCarriesNoCascade(t *testing.T) {
	c := qt.New(t)

	sql, err := New().Render(ast.NewRevokePrivilege("app", "TABLE", "dbo.orders", []string{"SELECT"}))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "REVOKE SELECT ON [dbo].[orders] FROM [app];\n")
}

// TestRenderer_RolesStillRefuseWhenTheCapabilityIsOff is the gate's inverse
// control: the emission is reached because the preset claims the key, not
// because the visitor stopped asking.
func TestRenderer_RolesStillRefuseWhenTheCapabilityIsOff(t *testing.T) {
	c := qt.New(t)
	without := capability.SQLServer2022().With(capability.RoleManagement, false)

	role, err := NewWithCapabilities(without).Render(ast.NewCreateRole("r"))
	c.Assert(err, qt.IsNil)
	c.Assert(role, qt.Contains, "is not generated for this target; skipped.")
	c.Assert(role, qt.Not(qt.Contains), "CREATE ROLE [r]")

	grant, err := NewWithCapabilities(without).Render(
		ast.NewGrantPrivilege("app", "TABLE", "dbo.orders", []string{"SELECT"}))
	c.Assert(err, qt.IsNil)
	c.Assert(grant, qt.Contains, "is not generated for this target; skipped.")
	c.Assert(grant, qt.Not(qt.Contains), "GRANT SELECT ON")
}
