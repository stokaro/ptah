package grantcover_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/grantcover"
)

// revokes is a schema that revokes privileges on table from r2.
func revokes(table string, privileges ...string) *schemamodel.Database {
	return &schemamodel.Database{RevokedGrants: []schemamodel.Grant{{Role: "r2", Privileges: privileges, OnTable: table}}}
}

// readHolding is a read that establishes the grants on every table, holding
// grants and wide.
func readHolding(grants []catalog.Grant, wide ...catalog.WideGrant) *catalog.Database {
	return &catalog.Database{Grants: grants, WideGrants: append(make([]catalog.WideGrant, 0, len(wide)), wide...)}
}

// databaseGrant is privilege granted to role on the whole of database d2.
func databaseGrant(role, privilege string) catalog.Grant {
	return catalog.Grant{Role: role, Privilege: privilege, ObjectType: "SCHEMA", ObjectName: "d2"}
}

// TestFind_HappyPath lists what keeps each revoke and what does not.
func TestFind_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		desired    *schemamodel.Database
		privileges []string
		table      string
		current    *catalog.Database
		want       []grantcover.Cover
	}{
		{
			name: "a grant on the database", privileges: []string{"SELECT"}, table: "t3",
			current: readHolding([]catalog.Grant{databaseGrant("r2", "SELECT")}),
			want:    []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ON d2.*"}},
		},
		{
			name: "a grant on the database a qualified table names", privileges: []string{"SELECT"}, table: "d2.t3",
			current: readHolding([]catalog.Grant{databaseGrant("r2", "SELECT")}),
			want:    []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ON d2.*"}},
		},
		{
			name: "a grant to PUBLIC on the database", privileges: []string{"select"}, table: "t3",
			current: readHolding([]catalog.Grant{databaseGrant("PUBLIC", "SELECT")}),
			want:    []grantcover.Cover{{Grantee: "PUBLIC", Grant: "SELECT ON d2.*"}},
		},
		{
			name: "a grant on every table, to the role and to PUBLIC", privileges: []string{"ALL"}, table: "t3",
			current: readHolding(nil,
				catalog.WideGrant{Role: "r2", Privilege: "ALTER", Grant: "ALTER ON *.*"},
				catalog.WideGrant{Role: "PUBLIC", Privilege: "SELECT", Grant: "SELECT ANY TABLE"},
			),
			want: []grantcover.Cover{{Grantee: "r2", Grant: "ALTER ON *.*"}, {Grantee: "PUBLIC", Grant: "SELECT ANY TABLE"}},
		},
		{
			name: "a grant on another database", privileges: []string{"SELECT"}, table: "d9.t3",
			current: readHolding([]catalog.Grant{databaseGrant("r2", "SELECT")}),
		},
		{
			name: "a grant of another privilege", privileges: []string{"INSERT"}, table: "t3",
			current: readHolding([]catalog.Grant{databaseGrant("r2", "SELECT")},
				catalog.WideGrant{Role: "r2", Privilege: "SELECT", Grant: "SELECT ON *.*"}),
		},
		{
			name: "a grant to another role", privileges: []string{"SELECT"}, table: "t3",
			current: readHolding([]catalog.Grant{databaseGrant("r7", "SELECT")},
				catalog.WideGrant{Role: "r7", Privilege: "SELECT", Grant: "SELECT ON *.*"}),
		},
		{
			name: "a grant on the table", privileges: []string{"SELECT"}, table: "t3",
			current: readHolding([]catalog.Grant{{Role: "r2", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t3"}}),
		},
		{
			name:    "a grant on the database the plan removes",
			desired: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "r2"}}}, privileges: []string{"SELECT"}, table: "t3",
			current: readHolding([]catalog.Grant{databaseGrant("r2", "SELECT")}),
		},
		{
			name: "a grant on the database the schema revokes",
			desired: &schemamodel.Database{RevokedGrants: []schemamodel.Grant{
				{Role: "r2", Privileges: []string{"ALL"}, OnSchema: "d2"},
				{Role: "PUBLIC", Privileges: []string{"SELECT"}, OnSchema: "d2"},
			}},
			privileges: []string{"SELECT"}, table: "t3",
			current: readHolding([]catalog.Grant{databaseGrant("r2", "SELECT"), databaseGrant("PUBLIC", "SELECT")}),
		},
		{
			name: "a grant on the database the schema revokes elsewhere",
			desired: &schemamodel.Database{RevokedGrants: []schemamodel.Grant{
				{Role: "r2", Privileges: []string{"SELECT"}, OnSchema: "d9"},
				{Role: "r7", Privileges: []string{"SELECT"}, OnSchema: "d2"},
				{Role: "r2", Privileges: []string{"INSERT"}, OnSchema: "d2"},
			}},
			privileges: []string{"SELECT"}, table: "t3",
			current: readHolding([]catalog.Grant{databaseGrant("r2", "SELECT")}),
			want:    []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ON d2.*"}},
		},
		{
			name: "a grant on the database the schema keeps",
			desired: &schemamodel.Database{
				Roles:  []schemamodel.Role{{Name: "r2"}},
				Grants: []schemamodel.Grant{{Role: "r2", Privileges: []string{"ALL PRIVILEGES"}, OnSchema: "d2"}},
			},
			privileges: []string{"SELECT"}, table: "t3",
			current: readHolding([]catalog.Grant{databaseGrant("r2", "SELECT")}),
			want:    []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ON d2.*"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			covers, known := grantcover.Find(test.desired, test.current, "r2", test.privileges, test.table)

			c.Assert(known, qt.IsTrue)
			c.Assert(covers, qt.DeepEquals, test.want)
		})
	}
}

// TestFind_NotKnown reports the grants on the database of a read that does
// not establish the grants on every table, and says it does not know the
// rest.
func TestFind_NotKnown(t *testing.T) {
	c := qt.New(t)

	covers, known := grantcover.Find(nil, &catalog.Database{Grants: []catalog.Grant{databaseGrant("r2", "SELECT")}},
		"r2", []string{"SELECT"}, "t3")

	c.Assert(known, qt.IsFalse)
	c.Assert(covers, qt.DeepEquals, []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ON d2.*"}})
}

// TestValidate_FailurePath refuses a revoke a wider grant keeps, naming the
// grant and where to revoke it.
func TestValidate_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		current *catalog.Database
		want    string
	}{
		{
			name: "a grant on the database", dialect: "mysql",
			current: readHolding([]catalog.Grant{databaseGrant("r2", "SELECT")}),
			want: `role "r2" holds SELECT ON d2\.\*, which covers table t3, so a revoke on the table cannot take it: ` +
				`revoke SELECT ON d2\.\* from "r2" instead, or grant the privilege on each table that needs it`,
		},
		{
			name: "a grant on every database", dialect: "mysql",
			current: readHolding(nil, catalog.WideGrant{Role: "r2", Privilege: "SELECT", Grant: "SELECT ON *.*"}),
			want: `role "r2" holds SELECT ON \*\.\*, which covers table t3, so a revoke on the table cannot take it: ` +
				`revoke SELECT ON \*\.\* from "r2" instead, or grant the privilege on each table that needs it`,
		},
		{
			name: "a grant to PUBLIC", dialect: "mariadb",
			current: readHolding([]catalog.Grant{databaseGrant("PUBLIC", "SELECT")}),
			want: `PUBLIC holds SELECT ON d2\.\*, which gives role "r2" the privilege on table t3, so a revoke on the table cannot take it: ` +
				`revoke SELECT ON d2\.\* from PUBLIC instead, or grant the privilege on each table that needs it`,
		},
		{
			name: "a grant on the database, in a read that does not say what covers every table", dialect: "mysql",
			current: &catalog.Database{Grants: []catalog.Grant{databaseGrant("r2", "SELECT")}},
			want: `role "r2" holds SELECT ON d2\.\*, which covers table t3, so a revoke on the table cannot take it: ` +
				`revoke SELECT ON d2\.\* from "r2" instead, or grant the privilege on each table that needs it`,
		},
		{
			name: "a system privilege", dialect: "oracle",
			current: readHolding(nil, catalog.WideGrant{Role: "r2", Privilege: "SELECT", Grant: "SELECT ANY TABLE"}),
			want: `role "r2" holds SELECT ANY TABLE, which covers table t3, so a revoke on the table cannot take it: ` +
				`revoke SELECT ANY TABLE from "r2" instead, or grant the privilege on each table that needs it`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := grantcover.Validate(test.dialect, revokes("t3", "SELECT"), test.current)

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestValidate_HappyPath accepts a revoke nothing wider keeps, a revoke that
// is not on a table, and every revoke on a target where a REVOKE on a table
// takes more than a table grant.
func TestValidate_HappyPath(t *testing.T) {
	covered := readHolding([]catalog.Grant{databaseGrant("r2", "SELECT")},
		catalog.WideGrant{Role: "r2", Privilege: "SELECT", Grant: "SELECT ON *.*"})
	tests := []struct {
		name    string
		dialect string
		desired *schemamodel.Database
		current *catalog.Database
	}{
		{name: "nothing wider", dialect: "mysql", desired: revokes("t3", "SELECT"), current: readHolding(nil)},
		{name: "a read that does not say", dialect: "mysql", desired: revokes("t3", "SELECT"), current: &catalog.Database{}},
		{
			name: "a revoke on the database", dialect: "mysql", current: covered,
			desired: &schemamodel.Database{RevokedGrants: []schemamodel.Grant{{Role: "r2", Privileges: []string{"SELECT"}, OnSchema: "d2"}}},
		},
		{name: "PostgreSQL", dialect: "postgres", desired: revokes("t3", "SELECT"), current: covered},
		{name: "ClickHouse, which refuses it on its own", dialect: "clickhouse", desired: revokes("t3", "SELECT"), current: covered},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(grantcover.Validate(test.dialect, test.desired, test.current), qt.IsNil)
		})
	}
}
