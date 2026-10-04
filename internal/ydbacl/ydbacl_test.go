package ydbacl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbacl"
)

// TestPermission_HappyPath pins the keyword each permission name is written
// with, measured on 25.1.4.7 and 26.2.1.14: `GRANT <keyword> ...` stores the
// name beside it, and a name is matched in any case because a declaration's
// privileges are upper-cased before they get here.
func TestPermission_HappyPath(t *testing.T) {
	tests := []struct {
		privilege string
		want      string
	}{
		{privilege: "CONNECT", want: "ydb.database.connect"},
		{privilege: "CREATE", want: "ydb.database.create"},
		{privilege: "DROP", want: "ydb.database.drop"},
		{privilege: "SELECT ROW", want: "ydb.granular.select_row"},
		{privilege: "UPDATE ROW", want: "ydb.granular.update_row"},
		{privilege: "ERASE ROW", want: "ydb.granular.erase_row"},
		{privilege: "SELECT ATTRIBUTES", want: "ydb.granular.read_attributes"},
		{privilege: "MODIFY ATTRIBUTES", want: "ydb.granular.write_attributes"},
		{privilege: "CREATE DIRECTORY", want: "ydb.granular.create_directory"},
		{privilege: "CREATE TABLE", want: "ydb.granular.create_table"},
		{privilege: "CREATE QUEUE", want: "ydb.granular.create_queue"},
		{privilege: "REMOVE SCHEMA", want: "ydb.granular.remove_schema"},
		{privilege: "DESCRIBE SCHEMA", want: "ydb.granular.describe_schema"},
		{privilege: "ALTER SCHEMA", want: "ydb.granular.alter_schema"},
		{privilege: "GRANT", want: "ydb.access.grant"},
		{privilege: "SELECT", want: "ydb.generic.read"},
		{privilege: "INSERT", want: "ydb.generic.write"},
		{privilege: "LIST", want: "ydb.generic.list"},
		{privilege: "USE LEGACY", want: "ydb.generic.use_legacy"},
		{privilege: "USE", want: "ydb.generic.use"},
		{privilege: "MANAGE", want: "ydb.generic.manage"},
		{privilege: "FULL LEGACY", want: "ydb.generic.full_legacy"},
		{privilege: "FULL", want: "ydb.generic.full"},
		{privilege: "MODIFY TABLES", want: "ydb.tables.modify"},
		{privilege: "SELECT TABLES", want: "ydb.tables.read"},
		{privilege: "ALL", want: "ydb.generic.full"},
		{privilege: "all  privileges", want: "ydb.generic.full"},
		{privilege: " select\trow ", want: "ydb.granular.select_row"},
		{privilege: "ydb.granular.select_row", want: "ydb.granular.select_row"},
		{privilege: "YDB.GENERIC.LIST", want: "ydb.generic.list"},
	}

	for _, test := range tests {
		t.Run(test.privilege, func(t *testing.T) {
			c := qt.New(t)
			got, ok := ydbacl.Permission(test.privilege)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestPermission_FailurePath pins that a privilege YDB has no permission for
// names none, PostgreSQL's spellings among them, rather than the nearest one.
func TestPermission_FailurePath(t *testing.T) {
	for _, privilege := range []string{"", "  ", "TRUNCATE", "USAGE", "ydb.bogus.thing", "SELECTROW", "ydb.generic"} {
		t.Run(privilege, func(t *testing.T) {
			c := qt.New(t)
			got, ok := ydbacl.Permission(privilege)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestPermissions pins that every permission a keyword names is listed once,
// sorted, so a declaration validated against the list and a grant rendered
// from it cannot disagree.
func TestPermissions(t *testing.T) {
	c := qt.New(t)
	got := ydbacl.Permissions()
	c.Assert(got, qt.HasLen, 25)
	c.Assert(got[0], qt.Equals, "ydb.access.grant")
	c.Assert(got[len(got)-1], qt.Equals, "ydb.tables.read")
	for _, name := range got {
		resolved, ok := ydbacl.Permission(name)
		c.Assert(ok, qt.IsTrue)
		c.Assert(resolved, qt.Equals, name)
	}
}

// TestCheckName_HappyPath pins the names YDB takes for a user or group:
// lower-case letters and digits, a leading digit and a long name included.
func TestCheckName_HappyPath(t *testing.T) {
	for _, name := range []string{"app", "9app", "a1b2", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbacl.CheckName(name), qt.IsNil)
		})
	}
}

// TestCheckName_FailurePath pins the characters YDB refuses with `Name is not
// allowed`, measured on 25.1.4.7 to 26.2.1.14, and names the first offender.
func TestCheckName_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		wantErr string
	}{
		{name: "", wantErr: `YDB takes a user or group name of lower-case ASCII letters and digits only, and the name is empty`},
		{name: "app_user", wantErr: `.*: "app_user" holds '_'`},
		{name: "app-user", wantErr: `.*: "app-user" holds '-'`},
		{name: "app.user", wantErr: `.*: "app.user" holds '\.'`},
		{name: "app@corp", wantErr: `.*: "app@corp" holds '@'`},
		{name: "App", wantErr: `.*: "App" holds 'A'`},
		{name: "DATA-READERS", wantErr: `.*: "DATA-READERS" holds 'D'`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbacl.CheckName(test.name), qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestPasswordClause pins the clause a declared password is written with:
// HASH for the hash object YDB reports, PASSWORD with the value escaped
// otherwise.
func TestPasswordClause(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		want     string
		hash     bool
	}{
		{name: "a password", declared: "Secret1!", want: `PASSWORD 'Secret1!'`},
		{name: "a quote and a backslash", declared: `a'b\c`, want: `PASSWORD 'a\'b\\c'`},
		{name: "a hash", declared: ` {"hash":"h","salt":"s","type":"argon2id"} `,
			want: `HASH '{"hash":"h","salt":"s","type":"argon2id"}'`, hash: true},
		{name: "JSON that is not a hash", declared: `{"hash":"h"}`, want: `PASSWORD '{"hash":"h"}'`},
		{name: "a brace that is not JSON", declared: `{secret`, want: `PASSWORD '{secret'`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbacl.PasswordClause(test.declared), qt.Equals, test.want)
			c.Assert(ydbacl.IsPasswordHash(test.declared), qt.Equals, test.hash)
		})
	}
}
