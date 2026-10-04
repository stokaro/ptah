package ydbacl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbacl"
)

// TestValidateDeclared_HappyPath pins the declarations YDB holds: a user, a
// group, a membership in a declared group and in one the cluster made, and
// grants on the database, a directory and a table spelled as keywords or as
// permission names. Another dialect's declaration is not this package's to
// judge.
func TestValidateDeclared_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		database *schemamodel.Database
	}{
		{name: "the YDB model", dialect: "ydb", database: &schemamodel.Database{
			Roles: []schemamodel.Role{
				{Name: "readers", Group: true, Inherit: true},
				{Name: "app", Login: true, Password: "Secret1!", Inherit: true, MemberOf: []string{"readers", "DATA-READERS"}},
				{Name: "auditors", Group: true, MemberOf: []string{"readers"}},
			},
			Grants: []schemamodel.Grant{
				{Role: "readers", Privileges: []string{"SELECT ROW", "ydb.generic.list"}, OnTable: "shop.orders"},
				{Role: "readers", Privileges: []string{"LIST"}, OnSchema: "shop"},
				{Role: "app", Privileges: []string{"CONNECT"}, OnDatabase: true},
				{Role: "nobody", Privileges: []string{"ALL"}, OnTable: "events"},
			},
			Views:         []schemamodel.View{{Name: "shop.active", Body: "SELECT 1 AS a"}},
			RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: []string{"ERASE ROW"}, OnTable: "events"}},
		}},
		{name: "another dialect", dialect: "postgres", database: &schemamodel.Database{
			Roles:  []schemamodel.Role{{Name: "app_user", Superuser: true}},
			Grants: []schemamodel.Grant{{Role: "PUBLIC", Privileges: []string{"USAGE"}, OnSchema: "public", WithOption: true}},
		}},
		{name: "no database", dialect: "ydb"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbacl.ValidateDeclared(test.dialect, test.database), qt.IsNil)
		})
	}
}

// TestValidateDeclared_FailurePath pins each refusal, the reason in it, and
// that a password's value never reaches the message.
func TestValidateDeclared_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		database *schemamodel.Database
		wantErr  string
	}{
		{name: "a name YDB refuses", database: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app_user"}}},
			wantErr: `user "app_user": YDB takes a user or group name .*; measured, other names answer ` + "`Name is not allowed`"},
		{name: "an attribute YDB has no counterpart for", database: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app", CreateRole: true}}},
			wantErr: `user "app" declares createrole: a YDB user carries no such attribute; .*`},
		{name: "a group that logs in", database: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "readers", Group: true, Login: true}}},
			wantErr: `group "readers" declares login: a YDB group never logs in`},
		{name: "a group with a password, its value kept out", database: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "readers", Group: true, Password: "Hunter2!"}}},
			wantErr: `group "readers" declares a password: a YDB group never logs in`},
		{name: "a member of itself", database: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "readers", Group: true, MemberOf: []string{"readers"}}}},
			wantErr: `group "readers" names itself in member_of`},
		{name: "a member of a declared user", database: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app"}, {Name: "etl", MemberOf: []string{"app"}}}},
			wantErr: `user "etl" is a member of "app", which the schema declares as a user: only a YDB group has members`},
		{name: "an empty group", database: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app", MemberOf: []string{" "}}}},
			wantErr: `user "app" names an empty group in member_of`},
		{name: "a grant to PUBLIC", database: &schemamodel.Database{Grants: []schemamodel.Grant{{Role: "public", Privileges: []string{"SELECT"}, OnTable: "t"}}},
			wantErr: `grant to "public": YDB has no PUBLIC; .*`},
		{name: "two targets", database: &schemamodel.Database{Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnTable: "t", OnDatabase: true}}},
			wantErr: `grant to "app" names 2 targets: .*`},
		{name: "no target", database: &schemamodel.Database{Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}}}},
			wantErr: `grant to "app" names 0 targets: .*`},
		{name: "a sequence", database: &schemamodel.Database{Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnSequence: "s"}}},
			wantErr: `grant to "app" on sequence "s": a YDB grant is on the database, a directory or a table`},
		{name: "a routine", database: &schemamodel.Database{Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"EXECUTE"}, OnRoutine: "f", RoutineArguments: "int"}}},
			wantErr: `(?s)grant to "app" on routine "f": YDB has no routines.*`},
		{name: "columns", database: &schemamodel.Database{Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnTable: "t", Columns: []string{"a", "b"}}}},
			wantErr: `grant to "app" on columns a, b of "t": a YDB permission is on a whole object`},
		{name: "the grant option", database: &schemamodel.Database{Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnTable: "t", WithOption: true}}},
			wantErr: `grant to "app" declares with_option: YDB records it as a permission of its own, ydb.access.grant, .*`},
		{name: "no privilege", database: &schemamodel.Database{Grants: []schemamodel.Grant{{Role: "app", OnTable: "t"}}},
			wantErr: `grant to "app" names no privilege`},
		{name: "a privilege YDB does not have, in a revoke", database: &schemamodel.Database{RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: []string{"TRUNCATE"}, OnTable: "t"}}},
			wantErr: `revoke to "app" names privilege "TRUNCATE", which is not a YDB permission: .*`},
		{name: "a view", database: &schemamodel.Database{
			Views:  []schemamodel.View{{Name: "shop.active", Body: "SELECT 1 AS a"}},
			Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnTable: "shop.active"}}},
			wantErr: `grant to "app" on view "shop.active": Ptah does not read a YDB view's permissions yet, .*`},
		{name: "a default privilege", database: &schemamodel.Database{DefaultPrivileges: []schemamodel.DefaultPrivilege{{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}}},
			wantErr: `default privilege for "owner" in "app": YDB has no default privileges; .*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := ydbacl.ValidateDeclared("ydb", test.database)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err.Error(), qt.Not(qt.Contains), "Hunter2!")
		})
	}
}

// TestValidateDeclared_ReportsEveryProblem pins that a schema with several
// mistakes is refused once, naming each, rather than one run per mistake.
func TestValidateDeclared_ReportsEveryProblem(t *testing.T) {
	c := qt.New(t)
	err := ydbacl.ValidateDeclared("ydb", &schemamodel.Database{
		Roles:  []schemamodel.Role{{Name: "b_user"}, {Name: "A"}},
		Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"USAGE"}, OnSchema: "s"}},
	})
	c.Assert(err, qt.ErrorMatches, `user "A": .*\nuser "b_user": .*\ngrant to "app" names privilege "USAGE", .*`)
}
