package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/ydb"
)

// TestRender_AccessStatements_HappyPath pins the statements YDB's access model
// takes, each measured on 25.1.4.7 and 26.2.1.14: a permission is written by
// its name as a string, which is how the server reports it back, a name is
// quoted with backticks, and a path is relative to the database root where the
// line resolves it.
func TestRender_AccessStatements_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		node ast.Node
		want string
	}{
		{name: "a user that logs in", caps: capability.YDB262(),
			node: ast.NewCreateRole("app").SetLogin(true),
			want: "CREATE USER `app`;\n"},
		{name: "a user that does not log in", caps: capability.YDB262(),
			node: ast.NewCreateRole("app"),
			want: "CREATE USER `app` NOLOGIN;\n"},
		{name: "a user with a password, escaped and warned about", caps: capability.YDB262(),
			node: ast.NewCreateRole("app").SetLogin(true).SetPassword(`a'b\c`),
			want: "-- WARNING: the password is written in plain text; declare its hash to keep it out of the file\n" +
				"CREATE USER `app` PASSWORD 'a\\'b\\\\c';\n"},
		{name: "a user with a password hash", caps: capability.YDB251(),
			node: ast.NewCreateRole("app").SetLogin(true).SetPassword(`{"hash":"h","salt":"s","type":"argon2id"}`),
			want: "CREATE USER `app` HASH '{\"hash\":\"h\",\"salt\":\"s\",\"type\":\"argon2id\"}';\n"},
		{name: "a group with its comment", caps: capability.YDB262(),
			node: ast.NewCreateRole("readers").SetGroup(true).SetComment("reads\nthe shop"),
			want: "-- reads the shop\nCREATE GROUP `readers`;\n"},
		{name: "a changed password and login in one statement", caps: capability.YDB262(),
			node: ast.NewAlterRole("app").AddOperation(ast.NewSetPasswordOperation("Pw1!")).
				AddOperation(ast.NewSetLoginOperation(false)),
			want: "-- WARNING: the password is written in plain text; declare its hash to keep it out of the file\n" +
				"ALTER USER `app` PASSWORD 'Pw1!' NOLOGIN;\n"},
		{name: "a user allowed to log in again", caps: capability.YDB262(),
			node: ast.NewAlterRole("app").AddOperation(ast.NewSetLoginOperation(true)),
			want: "ALTER USER `app` LOGIN;\n"},
		{name: "a dropped user", caps: capability.YDB262(),
			node: ast.NewDropRole("app").SetIfExists(),
			want: "DROP USER IF EXISTS `app`;\n"},
		{name: "a dropped group", caps: capability.YDB262(),
			node: ast.NewDropRole("readers").SetGroup(true),
			want: "DROP GROUP `readers`;\n"},
		{name: "a member added", caps: capability.YDB262(),
			node: ast.NewGrantRoleMembership("DATA-READERS", "app"),
			want: "ALTER GROUP `DATA-READERS` ADD USER `app`;\n"},
		{name: "a member dropped", caps: capability.YDB262(),
			node: ast.NewRevokeRoleMembership("readers", "app"),
			want: "ALTER GROUP `readers` DROP USER `app`;\n"},
		{name: "keywords and names granted on a table in a directory", caps: capability.YDB251(),
			node: ast.NewGrantPrivilege("readers", "TABLE", "shop.orders", []string{"SELECT ROW", "ydb.generic.list"}),
			want: "GRANT 'ydb.granular.select_row', 'ydb.generic.list' ON `shop/orders` TO `readers`;\n"},
		{name: "ALL granted on a table at the root where the line resolves a single name", caps: capability.YDB262(),
			node: ast.NewGrantPrivilege("app", "TABLE", "events", []string{"ALL"}),
			want: "GRANT 'ydb.generic.full' ON `events` TO `app`;\n"},
		{name: "a grant on a directory", caps: capability.YDB262(),
			node: ast.NewGrantPrivilege("app", "SCHEMA", "shop/eu", []string{"LIST"}),
			want: "GRANT 'ydb.generic.list' ON `shop/eu` TO `app`;\n"},
		{name: "a grant on the database by the path a plan names it with", caps: capability.YDB251(),
			node: ast.NewGrantPrivilege("app", "DATABASE", "/local", []string{"CONNECT"}),
			want: "GRANT 'ydb.database.connect' ON `/local` TO `app`;\n"},
		{name: "a revoke on a table at the root by its absolute path", caps: capability.YDB251(),
			node: ast.NewRevokePrivilege("app", "TABLE", "/local/events", []string{"UPDATE ROW"}),
			want: "REVOKE 'ydb.granular.update_row' ON `/local/events` FROM `app`;\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestRender_AccessStatements_FailurePath pins what the renderer refuses
// rather than writes: what a YDB user or group cannot carry, the grant option
// YDB records as a permission of its own, and an object only an absolute path
// reaches, which a render with no database connection cannot know.
func TestRender_AccessStatements_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		node    ast.Node
		wantErr string
	}{
		{name: "a name YDB refuses", caps: capability.YDB262(),
			node:    ast.NewCreateRole("app_user").SetLogin(true),
			wantErr: `user app_user: YDB takes a user or group name of lower-case ASCII letters and digits only: "app_user" holds '_'`},
		{name: "a superuser", caps: capability.YDB262(),
			node:    ast.NewCreateRole("app").SetSuperuser(true).SetPassword("Secret1!"),
			wantErr: `user app: it declares superuser, which a YDB user or group does not carry; a group never logs in`},
		{name: "a group that logs in with a password", caps: capability.YDB262(),
			node:    ast.NewCreateRole("readers").SetGroup(true).SetLogin(true).SetPassword("Secret1!"),
			wantErr: `group readers: it declares login, a password, which a YDB user or group does not carry; a group never logs in`},
		{name: "a change no YDB user carries", caps: capability.YDB262(),
			node:    ast.NewAlterRole("app").AddOperation(ast.NewSetCreateDBOperation(true)),
			wantErr: `ALTER USER app: it changes createdb, which a YDB user does not carry; .*`},
		{name: "a grant with the grant option", caps: capability.YDB262(),
			node:    ast.NewGrantPrivilege("app", "TABLE", "t", []string{"SELECT"}).SetWithOption(true),
			wantErr: `GRANT on table t to app: YDB records WITH GRANT OPTION as the permission ydb.access.grant .*`},
		{name: "a revoke of the grant option alone", caps: capability.YDB262(),
			node:    ast.NewRevokePrivilege("app", "TABLE", "t", []string{"SELECT"}).SetGrantOptionFor(true),
			wantErr: `REVOKE on table t from app: YDB's REVOKE GRANT OPTION FOR takes the permission it names .*`},
		{name: "a privilege YDB does not have", caps: capability.YDB262(),
			node:    ast.NewGrantPrivilege("app", "TABLE", "t", []string{"TRUNCATE"}),
			wantErr: `GRANT on table t to app: "TRUNCATE" is not a YDB permission`},
		{name: "a column grant", caps: capability.YDB262(),
			node:    ast.NewGrantPrivilege("app", "TABLE", "t", []string{"SELECT"}).SetColumns([]string{"a"}),
			wantErr: `GRANT on table t to app: a YDB permission is on a whole object, not on columns`},
		{name: "a sequence", caps: capability.YDB262(),
			node:    ast.NewGrantPrivilege("app", "SEQUENCE", "s", []string{"SELECT"}),
			wantErr: `GRANT on sequence s to app: a YDB grant is on the database, a directory or a table, not a SEQUENCE`},
		{name: "the database with no path to name it by", caps: capability.YDB262(),
			node:    ast.NewGrantPrivilege("app", "DATABASE", "", []string{"CONNECT"}),
			wantErr: `GRANT on the database to app: YDB takes this object only by its absolute path, .*; plan it against the database, .*`},
		{name: "a root table on a line that does not resolve a single name", caps: capability.YDB251(),
			node:    ast.NewGrantPrivilege("app", "TABLE", "events", []string{"SELECT"}),
			wantErr: `GRANT on table events to app: YDB takes this object only by its absolute path, .*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestRender_AccessStatements_RefusedWithoutTheKeys pins that every access
// statement is refused by the key it needs on a set that withholds it, the
// answer a later line without the feature would get.
func TestRender_AccessStatements_RefusedWithoutTheKeys(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		node    ast.Node
		wantKey capability.Capability
	}{
		{name: "a user", caps: capability.YDB262().With(capability.RoleManagement, false),
			node: ast.NewCreateRole("app"), wantKey: capability.RoleManagement},
		{name: "a group", caps: capability.YDB262().With(capability.GroupPrincipals, false),
			node: ast.NewCreateRole("readers").SetGroup(true), wantKey: capability.GroupPrincipals},
		{name: "a membership", caps: capability.YDB262().With(capability.RoleMembership, false),
			node: ast.NewGrantRoleMembership("readers", "app"), wantKey: capability.RoleMembership},
		{name: "a grant on the database", caps: capability.YDB262().With(capability.DatabaseGrants, false),
			node: ast.NewGrantPrivilege("app", "DATABASE", "/local", []string{"CONNECT"}), wantKey: capability.DatabaseGrants},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			var capabilityErr *ptaherr.CapabilityError
			c.Assert(err, qt.ErrorAs, &capabilityErr)
			c.Assert(capabilityErr.Feature, qt.Equals, string(test.wantKey))
			c.Assert(got, qt.Equals, "")
		})
	}
}
