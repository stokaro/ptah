package grantcover_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/grantcover"
)

// listsOf is what ALL grants on each object kind of dialect.
func listsOf(dialect string) descriptor.AllPrivileges {
	target, _ := platform.Lookup(dialect)
	return target.AllPrivileges
}

// revokes is a schema that revokes privileges on table from r2.
func revokes(table string, privileges ...string) *schemamodel.Database {
	return &schemamodel.Database{RevokedGrants: []schemamodel.Grant{{Role: "r2", Privileges: privileges, OnTable: table}}}
}

// readHolding is a read that establishes the grants on every table, holding
// grants and wide.
func readHolding(grants []catalog.Grant, wide ...catalog.WideGrant) *catalog.Database {
	return &catalog.Database{Grants: grants, WideGrants: append(make([]catalog.WideGrant, 0, len(wide)), wide...)}
}

// selecting is a read of the one database d2, as a MySQL-family connection
// that selected it makes, holding grants.
func selecting(grants ...catalog.Grant) *catalog.Database {
	read := readHolding(grants)
	read.CurrentDatabase = "d2"
	return read
}

// holding is read with the memberships added.
func holding(read *catalog.Database, memberships ...catalog.RoleMembership) *catalog.Database {
	read.RoleMemberships = memberships
	return read
}

// tableGrant is privilege granted to role on table t3, or on its column where
// column is not empty.
func tableGrant(role, privilege, column string) catalog.Grant {
	return catalog.Grant{Role: role, Privilege: privilege, ObjectType: "TABLE", ObjectName: "t3", Column: column}
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
			name: "a grant on every table of the schema, to the role and to PUBLIC", privileges: []string{"SELECT", "INSERT"}, table: "t3",
			current: readHolding(nil,
				catalog.WideGrant{Role: "r2", Privilege: "INSERT", Grant: "INSERT ANY TABLE ON SCHEMA APP", Schema: "APP"},
				catalog.WideGrant{Role: "PUBLIC", Privilege: "SELECT", Grant: "SELECT ANY TABLE ON SCHEMA APP", Schema: "APP"},
			),
			want: []grantcover.Cover{
				{Grantee: "r2", Grant: "INSERT ANY TABLE ON SCHEMA APP"},
				{Grantee: "PUBLIC", Grant: "SELECT ANY TABLE ON SCHEMA APP"},
			},
		},
		{
			name: "a grant on every table of the schema a qualified table names", privileges: []string{"SELECT"}, table: "app.t3",
			current: readHolding(nil, catalog.WideGrant{Role: "r2", Privilege: "SELECT", Grant: "SELECT ANY TABLE ON SCHEMA APP", Schema: "APP"}),
			want:    []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ANY TABLE ON SCHEMA APP"}},
		},
		{
			name: "a grant on every table of another schema", privileges: []string{"SELECT"}, table: "OTHER.t3",
			current: readHolding(nil, catalog.WideGrant{Role: "r2", Privilege: "SELECT", Grant: "SELECT ANY TABLE ON SCHEMA APP", Schema: "APP"}),
		},
		{
			name: "a grant on another database", privileges: []string{"SELECT"}, table: "d9.t3",
			current: readHolding([]catalog.Grant{databaseGrant("r2", "SELECT")}),
		},
		{
			name: "a grant on the database a read of one database names", privileges: []string{"SELECT"}, table: "t3",
			current: selecting(databaseGrant("r2", "SELECT")),
			want:    []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ON d2.*"}},
		},
		{
			name:       "a grant on a database that differs in case from the one a qualified table names, in a read that names no database",
			privileges: []string{"SELECT"}, table: "D2.t3",
			current: readHolding([]catalog.Grant{databaseGrant("r2", "SELECT")}),
		},
		{
			name:       "a grant on the database a read of one database names, for a table that names it in another case",
			privileges: []string{"SELECT"}, table: "D2.t3",
			current: selecting(databaseGrant("r2", "SELECT")),
			want:    []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ON d2.*"}},
		},
		{
			name: "a grant on the database the schema declares on another database", desired: declares("r2", "d9", "SELECT"),
			privileges: []string{"SELECT"}, table: "d2.t3",
			current: readHolding([]catalog.Grant{databaseGrant("r2", "SELECT")}),
			want:    []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ON d2.*"}},
		},
		{
			name: "a grant on the database the schema declares to another role", desired: declares("r7", "d2", "SELECT"),
			privileges: []string{"SELECT"}, table: "t3",
			current: readHolding([]catalog.Grant{databaseGrant("r2", "SELECT")}),
			want:    []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ON d2.*"}},
		},
		{
			name: "a grant on the database the schema declares of another privilege", desired: declares("r2", "d2", "INSERT"),
			privileges: []string{"SELECT"}, table: "t3",
			current: readHolding([]catalog.Grant{databaseGrant("r2", "SELECT")}),
			want:    []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ON d2.*"}},
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
			name: "a grant on the database held by a role the role holds", privileges: []string{"SELECT"}, table: "t3",
			current: holding(readHolding([]catalog.Grant{databaseGrant("parent", "SELECT")}), catalog.RoleMembership{Role: "parent", Member: "r2"}),
			want:    []grantcover.Cover{{Grantee: "parent", Grant: "SELECT ON d2.*"}},
		},
		{
			name: "a grant on every table held through a chain of memberships", privileges: []string{"SELECT"}, table: "t3",
			current: holding(readHolding(nil, catalog.WideGrant{Role: "top", Privilege: "SELECT", Grant: "SELECT ON *.*"}),
				catalog.RoleMembership{Role: "middle", Member: "r2"}, catalog.RoleMembership{Role: "top", Member: "middle"}),
			want: []grantcover.Cover{{Grantee: "top", Grant: "SELECT ON *.*"}},
		},
		{
			name: "a grant on a pattern held by a role granted to PUBLIC", privileges: []string{"SELECT"}, table: "t3",
			current: holding(readHolding(nil, catalog.WideGrant{Role: "everyone", Privilege: "SELECT", Grant: "SELECT ON `d%`.*", Schema: "d2"}),
				catalog.RoleMembership{Role: "everyone", Member: "PUBLIC"}),
			want: []grantcover.Cover{{Grantee: "everyone", Grant: "SELECT ON `d%`.*"}},
		},
		{
			name:    "a grant on the database held by a role the schema makes the role a member of",
			desired: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "r2", MemberOf: []string{"parent"}}}}, privileges: []string{"SELECT"}, table: "t3",
			current: readHolding([]catalog.Grant{databaseGrant("parent", "SELECT")}),
			want:    []grantcover.Cover{{Grantee: "parent", Grant: "SELECT ON d2.*"}},
		},
		{
			name: "a grant on the table held by a role the role holds", privileges: []string{"SELECT"}, table: "t3",
			current: holding(readHolding([]catalog.Grant{tableGrant("parent", "SELECT", "")}), catalog.RoleMembership{Role: "parent", Member: "r2"}),
			want:    []grantcover.Cover{{Grantee: "parent", Grant: "SELECT ON t3", OnTable: true}},
		},
		{
			name: "a grant on the table to PUBLIC", privileges: []string{"ALL"}, table: "t3",
			current: readHolding([]catalog.Grant{tableGrant("PUBLIC", "INSERT", "")}),
			want:    []grantcover.Cover{{Grantee: "PUBLIC", Grant: "INSERT ON t3", OnTable: true}},
		},
		{
			name: "a grant on some columns of the table held by a role the role holds", privileges: []string{"SELECT"}, table: "t3",
			current: holding(readHolding([]catalog.Grant{tableGrant("parent", "SELECT", "id")}), catalog.RoleMembership{Role: "parent", Member: "r2"}),
		},
		{
			name: "a grant on another table held by a role the role holds", privileges: []string{"SELECT"}, table: "t3",
			current: holding(readHolding([]catalog.Grant{{Role: "parent", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t9"}}),
				catalog.RoleMembership{Role: "parent", Member: "r2"}),
		},
		{
			name: "a grant on the table of another database held by a role the role holds", privileges: []string{"SELECT"}, table: "t3",
			current: holding(selecting(catalog.Grant{Role: "parent", Privilege: "SELECT", ObjectType: "TABLE", Schema: "d9", ObjectName: "t3"}),
				catalog.RoleMembership{Role: "parent", Member: "r2"}),
		},
		{
			name:       "a grant on the table held by a role, which the schema revokes",
			desired:    &schemamodel.Database{RevokedGrants: []schemamodel.Grant{{Role: "parent", Privileges: []string{"SELECT"}, OnTable: "t3"}}},
			privileges: []string{"SELECT"}, table: "t3",
			current: holding(readHolding([]catalog.Grant{tableGrant("parent", "SELECT", "")}), catalog.RoleMembership{Role: "parent", Member: "r2"}),
		},
		{
			name: "a grant on the table held by a role, which the schema grants too",
			desired: &schemamodel.Database{
				Roles:  []schemamodel.Role{{Name: "parent"}},
				Grants: []schemamodel.Grant{{Role: "parent", Privileges: []string{"SELECT"}, OnTable: "t3"}},
			},
			privileges: []string{"SELECT"}, table: "t3",
			current: holding(readHolding([]catalog.Grant{tableGrant("parent", "SELECT", "")}), catalog.RoleMembership{Role: "parent", Member: "r2"}),
			want:    []grantcover.Cover{{Grantee: "parent", Grant: "SELECT ON t3", OnTable: true, AlsoDeclared: true}},
		},
		{
			name: "a grant on the table the schema grants to a role the schema makes the role a member of",
			desired: &schemamodel.Database{
				Roles:  []schemamodel.Role{{Name: "r2", MemberOf: []string{"parent"}}},
				Grants: []schemamodel.Grant{{Role: "parent", Privileges: []string{"select", "INSERT"}, OnTable: "t3"}},
			},
			privileges: []string{"SELECT"}, table: "t3",
			current: readHolding(nil),
			want:    []grantcover.Cover{{Grantee: "parent", Grant: "SELECT ON t3", OnTable: true, Declared: true}},
		},
		{
			name: "a grant held by a member of the role", privileges: []string{"SELECT"}, table: "t3",
			current: holding(readHolding([]catalog.Grant{databaseGrant("member", "SELECT")},
				catalog.WideGrant{Role: "member", Privilege: "SELECT", Grant: "SELECT ON *.*"}), catalog.RoleMembership{Role: "r2", Member: "member"}),
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
			want:    []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ON d2.*", AlsoDeclared: true}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			covers, known := grantcover.Find(test.desired, test.current, "r2", test.privileges, test.table, descriptor.AllPrivileges{})

			c.Assert(known, qt.IsTrue)
			c.Assert(covers, qt.DeepEquals, test.want)
		})
	}
}

// declares is a schema that grants privileges on database to grantee.
func declares(grantee, database string, privileges ...string) *schemamodel.Database {
	return &schemamodel.Database{Grants: []schemamodel.Grant{{Role: grantee, Privileges: privileges, OnSchema: database}}}
}

// TestFind_HappyPathDeclared lists which grants the schema declares keep a
// revoke on a table: a grant on the table's database that the database does
// not hold yet, which the plan adds, spelled with the privileges it adds. A
// target that grants nothing on a schema, as Oracle, has no such grant.
func TestFind_HappyPathDeclared(t *testing.T) {
	tests := []struct {
		name       string
		desired    *schemamodel.Database
		privileges []string
		table      string
		all        descriptor.AllPrivileges
		current    *catalog.Database
		want       []grantcover.Cover
	}{
		{
			name: "ALL on the database", desired: declares("r2", "d2", "ALL"), privileges: []string{"SELECT"}, table: "t3",
			current: readHolding(nil),
			want:    []grantcover.Cover{{Grantee: "r2", Grant: "ALL ON d2.*", Declared: true}},
		},
		{
			name: "the privileges a revoke of ALL names, in the grant's order", desired: declares("r2", "d2", "insert", "EXECUTE", "SELECT"),
			privileges: []string{"ALL"}, table: "t3", all: listsOf("mysql"),
			current: readHolding(nil),
			want:    []grantcover.Cover{{Grantee: "r2", Grant: "INSERT, SELECT ON d2.*", Declared: true}},
		},
		{
			name: "to PUBLIC, on the database a qualified table names", desired: declares("public", "d2", "SELECT"),
			privileges: []string{"SELECT"}, table: "d2.t3",
			current: readHolding(nil),
			want:    []grantcover.Cover{{Grantee: "public", Grant: "SELECT ON d2.*", Declared: true}},
		},
		{
			name: "held already, which the read reports", desired: declares("r2", "d2", "SELECT"), privileges: []string{"SELECT"}, table: "t3",
			current: readHolding([]catalog.Grant{databaseGrant("r2", "SELECT")}),
			want:    []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ON d2.*", AlsoDeclared: true}},
		},
		{
			name: "to PUBLIC, held already under the spelling the read reports", desired: declares("public", "d2", "SELECT"),
			privileges: []string{"SELECT"}, table: "t3",
			current: readHolding([]catalog.Grant{databaseGrant("PUBLIC", "SELECT")}),
			want:    []grantcover.Cover{{Grantee: "PUBLIC", Grant: "SELECT ON d2.*", AlsoDeclared: true}},
		},
		{
			name: "ALL, of which the read reports one privilege", desired: declares("r2", "d2", "ALL PRIVILEGES"),
			privileges: []string{"ALL"}, table: "t3", all: listsOf("mariadb"),
			current: readHolding([]catalog.Grant{databaseGrant("r2", "SELECT")}),
			want: []grantcover.Cover{
				{Grantee: "r2", Grant: "SELECT ON d2.*", AlsoDeclared: true},
				{Grantee: "r2", Grant: "ALL ON d2.*", Declared: true},
			},
		},
		{
			name: "ALL, of which the read reports every privilege the revoke names", desired: declares("r2", "d2", "ALL"),
			privileges: []string{"SELECT"}, table: "t3",
			current: readHolding([]catalog.Grant{databaseGrant("r2", "SELECT")}),
			want:    []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ON d2.*", AlsoDeclared: true}},
		},
		{
			name: "on the database a read of one database names", desired: declares("r2", "d2", "SELECT"),
			privileges: []string{"SELECT"}, table: "t3", current: selecting(),
			want: []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ON d2.*", Declared: true}},
		},
		{
			name: "on the database a read of one database names, spelled in another case", desired: declares("r2", "D2", "SELECT"),
			privileges: []string{"SELECT"}, table: "t3", current: selecting(),
			want: []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ON D2.*", Declared: true}},
		},
		{
			name: "on another database", desired: declares("r2", "d9", "SELECT"), privileges: []string{"SELECT"}, table: "d2.t3",
			current: readHolding(nil),
		},
		{
			name:    "on the database a read of one database names, spelled in another case, for a table that names it",
			desired: declares("r2", "D2", "SELECT"), privileges: []string{"SELECT"}, table: "d2.t3", current: selecting(),
			want: []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ON D2.*", Declared: true}},
		},
		{
			name: "on another database than a read of one database names", desired: declares("r2", "d9", "SELECT"),
			privileges: []string{"SELECT"}, table: "t3", current: selecting(),
		},
		{
			name: "to another role", desired: declares("r7", "d2", "SELECT"), privileges: []string{"SELECT"}, table: "t3",
			current: readHolding(nil),
		},
		{
			name: "of another privilege", desired: declares("r2", "d2", "INSERT"), privileges: []string{"SELECT"}, table: "t3",
			current: readHolding(nil),
		},
		{
			name: "of a privilege no table carries, under a revoke of ALL", desired: declares("r2", "d2", "EXECUTE"),
			privileges: []string{"ALL"}, table: "t3", all: listsOf("mysql"),
			current: readHolding(nil),
		},
		{
			name: "on Oracle, where a grant on a schema reaches none of its tables", desired: declares("r2", "d2", "ALL"),
			privileges: []string{"SELECT"}, table: "t3", all: listsOf("oracle"),
			current: readHolding(nil),
		},
		{
			name: "on the table",
			desired: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "r2", Privileges: []string{"SELECT"}, OnTable: "t3"},
			}},
			privileges: []string{"SELECT"}, table: "t3",
			current: readHolding(nil),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			covers, known := grantcover.Find(test.desired, test.current, "r2", test.privileges, test.table, test.all)

			c.Assert(known, qt.IsTrue)
			c.Assert(covers, qt.DeepEquals, test.want)
		})
	}
}

// TestFind_HappyPathTableAll reads ALL in a revoke on a table as the target's
// table list, so a grant on the database of a privilege no table carries
// keeps nothing it names, and as every privilege where the target lists
// nothing.
func TestFind_HappyPathTableAll(t *testing.T) {
	current := readHolding([]catalog.Grant{databaseGrant("r2", "EXECUTE"), databaseGrant("r2", "SELECT")},
		catalog.WideGrant{Role: "r2", Privilege: "LOCK TABLES", Grant: "LOCK TABLES ON *.*"})
	tests := []struct {
		name string
		all  descriptor.AllPrivileges
		want []grantcover.Cover
	}{
		{
			name: "the table list", all: listsOf("mysql"),
			want: []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ON d2.*"}},
		},
		{
			name: "no list",
			want: []grantcover.Cover{
				{Grantee: "r2", Grant: "EXECUTE ON d2.*"},
				{Grantee: "r2", Grant: "SELECT ON d2.*"},
				{Grantee: "r2", Grant: "LOCK TABLES ON *.*"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			covers, known := grantcover.Find(nil, current, "r2", []string{"all privileges"}, "t3", test.all)

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
		"r2", []string{"SELECT"}, "t3", descriptor.AllPrivileges{})

	c.Assert(known, qt.IsFalse)
	c.Assert(covers, qt.DeepEquals, []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ON d2.*"}})
}

// TestFind_PartlyKnown reports the system privilege of a read that was refused
// the privileges granted on a schema, and says it does not know the rest.
func TestFind_PartlyKnown(t *testing.T) {
	c := qt.New(t)
	current := readHolding(nil, catalog.WideGrant{Role: "r2", Privilege: "SELECT", Grant: "SELECT ANY TABLE"})
	current.WideGrantsPartial = true

	covers, known := grantcover.Find(nil, current, "r2", []string{"SELECT"}, "t3", descriptor.AllPrivileges{})

	c.Assert(known, qt.IsFalse)
	c.Assert(covers, qt.DeepEquals, []grantcover.Cover{{Grantee: "r2", Grant: "SELECT ANY TABLE"}})
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
			name: "a grant on a pattern held by a role the role holds", dialect: "mysql",
			current: holding(readHolding(nil, catalog.WideGrant{Role: "parent", Privilege: "SELECT", Grant: "SELECT ON `d%`.*", Schema: "d2"}),
				catalog.RoleMembership{Role: "parent", Member: "r2"}),
			want: "parent holds SELECT ON `d%`\\.\\*, which gives role \"r2\" the privilege on table t3, so a revoke on the table cannot take it: " +
				"revoke SELECT ON `d%`\\.\\* from parent instead, or grant the privilege on each table that needs it",
		},
		{
			name: "a grant on the table held by a role the role holds", dialect: "mysql",
			current: holding(readHolding([]catalog.Grant{tableGrant("parent", "SELECT", "")}), catalog.RoleMembership{Role: "parent", Member: "r2"}),
			want: `parent holds SELECT ON t3, which gives role "r2" the privilege on table t3, so a revoke from "r2" cannot take it: ` +
				`revoke SELECT ON t3 from parent instead, or end the membership that passes it on`,
		},
		{
			name: "a grant on the table to PUBLIC", dialect: "mariadb",
			current: readHolding([]catalog.Grant{tableGrant("PUBLIC", "SELECT", "")}),
			want: `PUBLIC holds SELECT ON t3, which gives role "r2" the privilege on table t3, so a revoke from "r2" cannot take it: ` +
				`revoke SELECT ON t3 from PUBLIC instead`,
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
		{
			name: "a system privilege, in a read refused the privileges on a schema", dialect: "oracle",
			current: &catalog.Database{
				WideGrants:        []catalog.WideGrant{{Role: "r2", Privilege: "SELECT", Grant: "SELECT ANY TABLE"}},
				WideGrantsPartial: true,
			},
			want: `role "r2" holds SELECT ANY TABLE, which covers table t3, so a revoke on the table cannot take it: ` +
				`revoke SELECT ANY TABLE from "r2" instead, or grant the privilege on each table that needs it`,
		},
		{
			name: "a system privilege on the schema", dialect: "oracle",
			current: readHolding(nil, catalog.WideGrant{Role: "r2", Privilege: "SELECT", Grant: "SELECT ANY TABLE ON SCHEMA APP", Schema: "APP"}),
			want: `role "r2" holds SELECT ANY TABLE ON SCHEMA APP, which covers table t3, so a revoke on the table cannot take it: ` +
				`revoke SELECT ANY TABLE ON SCHEMA APP from "r2" instead, or grant the privilege on each table that needs it`,
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

// TestValidate_FailurePathDeclared refuses a revoke that a grant on the
// database the schema declares keeps, and a revoke of ALL that a held grant
// of a table privilege keeps, naming only that grant.
func TestValidate_FailurePathDeclared(t *testing.T) {
	withRevoke := func(desired *schemamodel.Database, privileges ...string) *schemamodel.Database {
		desired.RevokedGrants = revokes("t3", privileges...).RevokedGrants
		return desired
	}
	tests := []struct {
		name    string
		desired *schemamodel.Database
		current *catalog.Database
		want    string
	}{
		{
			name: "a grant the schema declares", desired: withRevoke(declares("r2", "d2", "ALL"), "SELECT"),
			current: readHolding(nil),
			want: `the schema grants ALL ON d2\.\* to role "r2", which covers table t3, so a revoke on the table cannot take it: ` +
				`grant the privilege on each table that needs it instead of on the database`,
		},
		{
			name: "a grant the schema declares to PUBLIC", desired: withRevoke(declares("PUBLIC", "d2", "SELECT"), "SELECT"),
			current: readHolding(nil),
			want: `the schema grants SELECT ON d2\.\* to PUBLIC, which gives role "r2" the privilege on table t3, so a revoke on the table cannot take it: ` +
				`grant the privilege on each table that needs it instead of on the database`,
		},
		{
			name: "a held grant the schema declares too", desired: withRevoke(declares("r2", "d2", "ALL"), "SELECT"),
			current: readHolding([]catalog.Grant{databaseGrant("r2", "SELECT")}),
			want: `role "r2" holds SELECT ON d2\.\*, which the schema grants too and which covers table t3, so a revoke on the table cannot take it: ` +
				`grant the privilege on each table that needs it instead of on the database`,
		},
		{
			name: "a grant to PUBLIC the schema declares too", desired: withRevoke(declares("PUBLIC", "d2", "SELECT"), "SELECT"),
			current: readHolding([]catalog.Grant{databaseGrant("PUBLIC", "SELECT")}),
			want: `PUBLIC holds SELECT ON d2\.\*, which the schema grants too and which gives role "r2" the privilege on table t3, ` +
				`so a revoke on the table cannot take it: grant the privilege on each table that needs it instead of on the database`,
		},
		{
			name: "a grant on the table the schema declares to a role the role is a member of",
			desired: withRevoke(&schemamodel.Database{
				Roles:  []schemamodel.Role{{Name: "r2", MemberOf: []string{"parent"}}},
				Grants: []schemamodel.Grant{{Role: "parent", Privileges: []string{"SELECT"}, OnTable: "t3"}},
			}, "SELECT"),
			current: readHolding(nil),
			want: `the schema grants SELECT ON t3 to parent, which gives role "r2" the privilege on table t3, so a revoke from "r2" cannot take it: ` +
				`leave the grant out, or end the membership that passes it on`,
		},
		{
			name: "a grant on the table to PUBLIC the schema declares too", desired: withRevoke(&schemamodel.Database{
				Grants: []schemamodel.Grant{{Role: "PUBLIC", Privileges: []string{"SELECT"}, OnTable: "t3"}},
			}, "SELECT"),
			current: readHolding([]catalog.Grant{tableGrant("PUBLIC", "SELECT", "")}),
			want: `PUBLIC holds SELECT ON t3, which the schema grants too and which gives role "r2" the privilege on table t3, ` +
				`so a revoke from "r2" cannot take it: leave the grant out`,
		},
		{
			name: "a revoke of ALL under a grant of a table privilege and one no table carries", desired: revokes("t3", "ALL"),
			current: readHolding([]catalog.Grant{databaseGrant("r2", "EXECUTE"), databaseGrant("r2", "SELECT")}),
			want: `role "r2" holds SELECT ON d2\.\*, which covers table t3, so a revoke on the table cannot take it: ` +
				`revoke SELECT ON d2\.\* from "r2" instead, or grant the privilege on each table that needs it`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := grantcover.Validate("mysql", test.desired, test.current)

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
		{
			name: "a wider grant held by a role the role does not hold", dialect: "mysql", desired: revokes("t3", "SELECT"),
			current: holding(readHolding([]catalog.Grant{databaseGrant("other", "SELECT")}), catalog.RoleMembership{Role: "r2", Member: "other"}),
		},
		{name: "a read that does not say", dialect: "mysql", desired: revokes("t3", "SELECT"), current: &catalog.Database{}},
		{
			name: "a revoke on the database", dialect: "mysql", current: covered,
			desired: &schemamodel.Database{RevokedGrants: []schemamodel.Grant{{Role: "r2", Privileges: []string{"SELECT"}, OnSchema: "d2"}}},
		},
		{
			name: "a revoke of ALL under grants on the database no table carries", dialect: "mysql", desired: revokes("t3", "ALL"),
			current: readHolding([]catalog.Grant{
				databaseGrant("r2", "EXECUTE"), databaseGrant("r2", "CREATE ROUTINE"), databaseGrant("r2", "EVENT"), databaseGrant("r2", "LOCK TABLES"),
			}),
		},
		{
			name: "a grant the schema declares of another privilege", dialect: "mariadb", current: readHolding(nil),
			desired: &schemamodel.Database{
				Grants:        declares("r2", "d2", "INSERT").Grants,
				RevokedGrants: revokes("t3", "SELECT").RevokedGrants,
			},
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
