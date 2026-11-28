package mysql

// White-box testing required: readRoleMemberships and the catalog discovery it
// depends on are unexported, and the fact under test is which table each engine
// records the role graph in. Reaching them through ReadSchema would mean
// scripting every other catalog query to observe this one. The same holds for
// the steps that classify what the read found: which grantee is a login, which
// principal is unread, and which name is refused.

import (
	"context"
	"database/sql/driver"
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/mysqlgrants"
)

// TestReadRoleMemberships_ReadsTheTableThisEngineHas pins the discovery and the
// projection together.
//
// The two engines record the same GRANT in different tables, and a query
// written for one does not degrade on the other -- MariaDB has no
// mysql.role_edges at all. The admin flag is part of the projection rather than
// a detail: MariaDB inserts an admin edge from a role's creator by itself, so
// an analysis that could not tell an admin edge from a membership would report
// every MariaDB role as held (stokaro/ptah#1950).
func TestReadRoleMemberships_ReadsTheTableThisEngineHas(t *testing.T) {
	tests := []struct {
		name string
		// hasRoleEdges is what information_schema.tables answers for
		// mysql.role_edges; the second lookup answers for roles_mapping.
		hasRoleEdges bool
		hasRolesMap  bool
		rows         [][]driver.Value
		want         []catalog.RoleMembership
	}{
		{
			name:         "MySQL, through role_edges",
			hasRoleEdges: true,
			rows: [][]driver.Value{
				{"reader", "%", "alice", "%", true},
				{"reader", "%", "root", "%", false},
			},
			want: []catalog.RoleMembership{
				{Role: "reader", Member: "alice", AdminOption: true},
				{Role: "reader", Member: "root", AdminOption: false},
			},
		},
		{
			// A login that shares a role's user name on another host is a
			// principal of its own: its memberships are not the role's.
			name:         "MySQL, a login named like a role on another host",
			hasRoleEdges: true,
			rows: [][]driver.Value{
				{"mbig", "%", "macct", "localhost", false},
				{"mlocal", "localhost", "macct", "%", false},
			},
			want: []catalog.RoleMembership{
				{Role: "mbig", Member: "macct@localhost"},
				{Role: "mlocal@localhost", Member: "macct"},
			},
		},
		{
			name:        "MariaDB, through roles_mapping",
			hasRolesMap: true,
			rows: [][]driver.Value{
				{"reader", "", "root", "localhost", true},
				{"reader", "", "alice", "", false},
				{"reader", "", "alice", "%", false},
				{"reader", "", "bob", "%", false},
			},
			want: []catalog.RoleMembership{
				{Role: "reader", Member: "root@localhost", AdminOption: true},
				{Role: "reader", Member: "alice", AdminOption: false},
				{Role: "reader", Member: "alice@%", AdminOption: false},
				{Role: "reader", Member: "bob", AdminOption: false},
			},
		},
		{
			// Roles arrived in MySQL 8.0 and MariaDB 10.0.5. A server with
			// neither table is describing a world where the question does not
			// exist, and answers an empty list rather than an error.
			name: "a server with neither table",
			want: make([]catalog.RoleMembership, 0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := membershipDB(c, test.hasRoleEdges, test.hasRolesMap, test.rows)
			reader := NewMySQLReader(db.SQL, "app")

			memberships, err := reader.readRoleMemberships(t.Context(), &mysqlgrants.Names{MariaDB: test.hasRolesMap})

			c.Assert(err, qt.IsNil)
			c.Assert(memberships, qt.DeepEquals, test.want)
		})
	}
}

// membershipDB answers the two existence lookups and then the membership query,
// and refuses anything else, so a projection that stopped selecting the admin
// flag fails here rather than reading something else.
func membershipDB(c *qt.C, hasRoleEdges, hasRolesMap bool, rows [][]driver.Value) *dbtest.DB {
	return dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		existence := strings.Contains(query, "FROM information_schema.tables")
		countRow := func(present bool) dbtest.QueryResult {
			count := int64(0)
			if present {
				count = 1
			}
			return dbtest.QueryResult{Columns: []string{"count"}, Rows: [][]driver.Value{{count}}}
		}
		switch {
		case existence && strings.Contains(query, "'role_edges'"):
			return countRow(hasRoleEdges), nil
		case existence && strings.Contains(query, "'roles_mapping'"):
			return countRow(hasRolesMap), nil
		case strings.Contains(query, "FROM mysql.role_edges"):
			// The admin flag has to be READ, not defaulted: a scripted result
			// answers whatever the stub was given, so the projection is what
			// this asserts. MariaDB writes an admin edge by itself, and an
			// analysis that lost the flag would report every role as held.
			c.Assert(query, qt.Contains, "WITH_ADMIN_OPTION")
			return dbtest.QueryResult{
				Columns: []string{"role_name", "role_host", "member_name", "member_host", "admin_option"},
				Rows:    rows,
			}, nil
		case strings.Contains(query, "FROM mysql.roles_mapping"):
			c.Assert(query, qt.Contains, "Admin_option")
			return dbtest.QueryResult{
				Columns: []string{"role_name", "role_host", "member_name", "member_host", "admin_option"},
				Rows:    rows,
			}, nil
		}
		c.Fatalf("unexpected query: %s", query)
		return dbtest.QueryResult{}, nil
	})
}

// TestReadRoleMemberships_Collision reads a graph where a role's name is the
// name another account reads as, the role `macct@localhost` on host % and the
// login 'macct'@'localhost', and records the name as one both accounts read
// as, so the read can leave the role out rather than fail.
func TestReadRoleMemberships_Collision(t *testing.T) {
	c := qt.New(t)
	db := membershipDB(c, true, false, [][]driver.Value{
		{"mbig", "%", "macct@localhost", "%", false},
		{"mbig", "%", "macct", "localhost", false},
	})
	reader := NewMySQLReader(db.SQL, "app")
	names := &mysqlgrants.Names{}

	memberships, err := reader.readRoleMemberships(t.Context(), names)

	c.Assert(err, qt.IsNil)
	c.Assert(memberships, qt.DeepEquals, []catalog.RoleMembership{
		{Role: "mbig", Member: "macct@localhost"},
		{Role: "mbig", Member: "macct@localhost"},
	})
	c.Assert(names.Collisions(), qt.DeepEquals, []mysqlgrants.Collision{{
		Name:     "macct@localhost",
		Accounts: []mysqlgrants.Account{{User: "macct@localhost", Host: "%"}, {User: "macct", Host: "localhost"}},
	}})
}

// TestUnreadRoles finds each principal the read does not describe that a
// described role holds, or PUBLIC holds where it stands for every account,
// whose privileges the read then did not read: a MySQL role on another host,
// or a login granted as a role.
func TestUnreadRoles(t *testing.T) {
	roles := []catalog.Role{{Name: "reader"}, {Name: "writer"}}
	tests := []struct {
		name         string
		memberships  []catalog.RoleMembership
		publicIsRole bool
		want         []string
	}{
		{name: "between described roles", memberships: []catalog.RoleMembership{{Role: "reader", Member: "writer"}}},
		{name: "a described role held by a login", memberships: []catalog.RoleMembership{{Role: "reader", Member: "app@localhost"}}},
		{
			name: "a role on another host held by two described roles",
			memberships: []catalog.RoleMembership{
				{Role: "reader@localhost", Member: "writer"}, {Role: "legacy", Member: "reader"}, {Role: "reader@localhost", Member: "reader"},
			},
			want: []string{"legacy", "reader@localhost"},
		},
		{name: "a role on another host held by PUBLIC", memberships: []catalog.RoleMembership{{Role: "reader@localhost", Member: "PUBLIC"}}, want: []string{"reader@localhost"}},
		{
			name:         "a role on another host held by a role named PUBLIC the read does not describe",
			memberships:  []catalog.RoleMembership{{Role: "reader@localhost", Member: "PUBLIC"}},
			publicIsRole: true,
		},
		{name: "a role on another host held by a login", memberships: []catalog.RoleMembership{{Role: "reader@localhost", Member: "app@%"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			schema := &catalog.Database{Roles: roles, RoleMemberships: test.memberships, PublicIsRole: test.publicIsRole}

			c.Assert(unreadRoles(schema), qt.DeepEquals, test.want)
		})
	}
}

// TestMarkLoginGrants marks a grant whose grantee is no role the read
// describes as a login's. MariaDB's PUBLIC, read in upper case, is every
// account's grantee; a MariaDB login named public is a login, and on MySQL a
// grantee named PUBLIC that no role has is a login too.
func TestMarkLoginGrants(t *testing.T) {
	roles := []catalog.Role{{Name: "reader"}}
	tests := []struct {
		name    string
		grantee string
		mariaDB bool
		want    bool
	}{
		{name: "a MySQL role", grantee: "reader"},
		{name: "a MySQL login", grantee: "app", want: true},
		{name: "a MySQL login named PUBLIC", grantee: "PUBLIC", want: true},
		{name: "a MariaDB role", grantee: "reader", mariaDB: true},
		{name: "a MariaDB login", grantee: "app", mariaDB: true, want: true},
		{name: "MariaDB's PUBLIC", grantee: "PUBLIC", mariaDB: true},
		{name: "a MariaDB login named public", grantee: "public", mariaDB: true, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			grants := []catalog.Grant{{Role: test.grantee, Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1"}}

			marked := markLoginGrants(grants, roles, test.mariaDB)

			c.Assert(marked, qt.HasLen, 1)
			c.Assert(marked[0].Login, qt.Equals, test.want)
		})
	}
}

// TestRefuseCollisions leaves a role two accounts read as out of the read,
// with its grants, its access and the connected role it would be, records it
// as not described, and keeps every other role and the memberships.
func TestRefuseCollisions(t *testing.T) {
	c := qt.New(t)
	schema := &catalog.Database{
		Roles:           []catalog.Role{{Name: "nm@localhost"}, {Name: "reader"}},
		Grants:          []catalog.Grant{{Role: "nm@localhost", Privilege: "SELECT"}, {Role: "reader", Privilege: "SELECT"}},
		WideGrants:      []catalog.WideGrant{{Role: "nm@localhost", Privilege: "SELECT"}, {Role: "reader", Privilege: "SELECT"}},
		WideAccess:      []catalog.WideAccess{{Role: "nm@localhost"}, {Role: "reader", Subject: "nm@localhost"}, {Role: "reader", Subject: "reader"}},
		RoleMemberships: []catalog.RoleMembership{{Role: "reader", Member: "nm@localhost"}},
		ConnectedRole:   "nm@localhost",
	}

	refuseCollisions(schema, []mysqlgrants.Collision{{Name: "nm@localhost"}})

	c.Assert(schema.Roles, qt.DeepEquals, []catalog.Role{{Name: "reader"}})
	c.Assert(schema.Grants, qt.DeepEquals, []catalog.Grant{{Role: "reader", Privilege: "SELECT"}})
	c.Assert(schema.WideGrants, qt.DeepEquals, []catalog.WideGrant{{Role: "reader", Privilege: "SELECT"}})
	c.Assert(schema.WideAccess, qt.DeepEquals, []catalog.WideAccess{{Role: "reader", Subject: "reader"}})
	c.Assert(schema.RoleMemberships, qt.DeepEquals, []catalog.RoleMembership{{Role: "reader", Member: "nm@localhost"}})
	c.Assert(schema.ConnectedRole, qt.Equals, "")
	c.Assert(schema.NotDescribed.Describes(coverage.Role, "nm@localhost"), qt.IsFalse)
	c.Assert(schema.NotDescribed.Describes(coverage.Role, "reader"), qt.IsTrue)
}

// TestReadConnectedRole names the connected account the way a membership names
// it, so a plan finds the memberships of the account that runs it.
// CURRENT_USER() splits at its last @.
func TestReadConnectedRole(t *testing.T) {
	tests := []struct {
		name, current string
		mariaDB       bool
		roles         []string
		want          string
	}{
		{name: "MySQL account on host %", current: "app@%", want: "app"},
		{name: "MySQL account on another host", current: "app@localhost", want: "app@localhost"},
		{name: "an @ in the user name", current: "r@x@%", want: "r@x"},
		{name: "MariaDB login with no role of its name", current: "app@%", mariaDB: true, want: "app"},
		{name: "MariaDB login named like a role", current: "app@%", mariaDB: true, roles: []string{"app"}, want: "app@%"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
				return dbtest.QueryResult{Columns: []string{"CURRENT_USER()"}, Rows: [][]driver.Value{{test.current}}}, nil
			})
			names := &mysqlgrants.Names{MariaDB: test.mariaDB}
			for _, role := range test.roles {
				names.Name(mysqlgrants.Account{User: role, Host: mysqlgrants.Options{MariaDB: test.mariaDB}.RoleHost()})
			}
			schema := &catalog.Database{}

			err := NewMySQLReader(db.SQL, "app").readConnectedRole(t.Context(), schema, names)

			c.Assert(err, qt.IsNil)
			c.Assert(schema.ConnectedRole, qt.Equals, test.want)
		})
	}
}

// TestWithoutPublic leaves MariaDB's PUBLIC row out of the described roles,
// since CREATE ROLE PUBLIC is refused there, and keeps a MySQL role of that
// name.
func TestWithoutPublic(t *testing.T) {
	tests := []struct {
		name    string
		mariaDB bool
		want    []catalog.Role
	}{
		{name: "MariaDB", mariaDB: true, want: []catalog.Role{{Name: "reader"}}},
		{name: "MySQL", mariaDB: false, want: []catalog.Role{{Name: "PUBLIC"}, {Name: "reader"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(withoutPublic([]catalog.Role{{Name: "PUBLIC"}, {Name: "reader"}}, test.mariaDB), qt.DeepEquals, test.want)
		})
	}
}

// mandatoryQuery models a session whose APPLICABLE_ROLES view fails and whose
// grant tables can still identify the accounts in mandatory_roles. The
// question that holds refused fails as a server's does.
func mandatoryQuery(value, version, refused string) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case refused != "" && strings.Contains(query, refused):
			return dbtest.QueryResult{}, fmt.Errorf("Error 1142 (42000): SELECT command denied")
		case strings.Contains(query, "@@GLOBAL.mandatory_roles"):
			return dbtest.QueryResult{
				Columns: []string{"mandatory_roles", "version"},
				Rows:    [][]driver.Value{{value, version}},
			}, nil
		case strings.Contains(query, "FROM mysql.user") && len(args) == 2:
			host, found := map[mysqlgrants.Account]string{
				{User: "mand", Host: "%"}:         "%",
				{User: "mloc", Host: "LOCALHOST"}: "localhost",
				{User: "mloc", Host: "localhost"}: "localhost",
				{User: "a", Host: "%"}:            "%",
				{User: "b@h", Host: "%"}:          "%",
				{User: "a,b", Host: "h"}:          "h",
				{User: "r", Host: "K"}:            "k",
				{User: "r", Host: "k"}:            "k",
				{User: "r", Host: "K"}:            "k",
				{User: "R", Host: "k"}:            "k",
			}[mysqlgrants.Account{User: fmt.Sprint(args[0].Value), Host: fmt.Sprint(args[1].Value)}]
			result := dbtest.QueryResult{Columns: []string{"Host"}}
			if found {
				result.Rows = [][]driver.Value{{host}}
			}
			return result, nil
		default:
			return dbtest.QueryResult{}, fmt.Errorf("Error 1267 (HY000): Illegal mix of collations in %s", query)
		}
	}
}

// TestReadMandatoryRoles_HappyPath reads the setting without APPLICABLE_ROLES,
// whose internal joins fail under
// default_collation_for_utf8mb4=utf8mb4_general_ci. A name counts where an
// account exists whose host matches the setting's byte for byte, and a name
// whose host differs in case is left out: the server makes that spelling
// active, and it holds nothing. MariaDB has no mandatory_roles and is not
// queried.
func TestReadMandatoryRoles_HappyPath(t *testing.T) {
	tests := []struct {
		name, value, version string
		mariaDB              bool
		want                 []catalog.RoleMembership
	}{
		{name: "MySQL", value: "mand, 'mloc'@'localhost', missing, mand", version: "8.4.13", want: []catalog.RoleMembership{
			{Role: "mand", Member: "PUBLIC"},
			{Role: "mloc@localhost", Member: "PUBLIC"},
		}},
		{name: "a host in another case", value: "mand, 'mloc'@'LOCALHOST'", version: "8.4.13", want: []catalog.RoleMembership{
			{Role: "mand", Member: "PUBLIC"},
		}},
		{name: "empty setting", version: "26.10.0"},
		{name: "MariaDB", value: "mand", version: "8.4.13", mariaDB: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, mandatoryQuery(test.value, test.version, ""))

			got, err := NewMySQLReader(db.SQL, "app").readMandatoryRoles(t.Context(), &mysqlgrants.Names{MariaDB: test.mariaDB})

			c.Assert(err, qt.IsNil)
			c.Assert(got.unread, qt.IsNil)
			c.Assert(got.memberships, qt.DeepEquals, test.want)
		})
	}
}

// TestReadMandatoryRoles_GrammarAndDuplicates applies the server version's
// list grammar, and keeps the first of the hosts that differ in case alone,
// as the server does, without merging differently cased user names. A host
// kept in another case than the account's holds nothing, so a setting that
// names it first records no membership for the account.
func TestReadMandatoryRoles_GrammarAndDuplicates(t *testing.T) {
	tests := []struct {
		name, value, version string
		want                 []catalog.RoleMembership
	}{
		{name: "before double quoted lists", value: `"a,b"@"h"`, version: "9.6.0", want: []catalog.RoleMembership{
			{Role: "a", Member: "PUBLIC"}, {Role: "b@h", Member: "PUBLIC"},
		}},
		{name: "first double quoted lists", value: `"a,b"@"h"`, version: "9.7.0", want: []catalog.RoleMembership{
			{Role: "a,b@h", Member: "PUBLIC"},
		}},
		{name: "current double quoted lists", value: `"a,b"@"h"`, version: "26.10.0", want: []catalog.RoleMembership{
			{Role: "a,b@h", Member: "PUBLIC"},
		}},
		{name: "a host in another case first", value: "r@K,r@k,r@K,R@k,r@K", version: "8.4.13", want: []catalog.RoleMembership{
			{Role: "R@k", Member: "PUBLIC"},
		}},
		{name: "the account's host first", value: "r@k,r@K", version: "26.10.0", want: []catalog.RoleMembership{
			{Role: "r@k", Member: "PUBLIC"},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, mandatoryQuery(test.value, test.version, ""))

			got, err := NewMySQLReader(db.SQL, "app").readMandatoryRoles(t.Context(), &mysqlgrants.Names{})

			c.Assert(err, qt.IsNil)
			c.Assert(got.unread, qt.IsNil)
			c.Assert(got.memberships, qt.DeepEquals, test.want)
		})
	}
}

// TestReadMandatoryRoles_Unread leaves the mandatory roles unread, rather than
// failing the read, where the setting or an account in it cannot be read, or
// the server's version does not say which account-list grammar it applies.
// Each records its cause, and the roles the setting names where the read got
// that far.
func TestReadMandatoryRoles_Unread(t *testing.T) {
	tests := []struct {
		name, version, refused string
		wantCause              string
		wantRoles              []string
	}{
		{
			name: "the setting", version: "8.4.13", refused: "@@GLOBAL.mandatory_roles",
			wantCause: `the read could not read @@GLOBAL\.mandatory_roles: Error 1142 \(42000\): SELECT command denied`,
		},
		{
			name: "an account", version: "8.4.13", refused: "FROM mysql.user",
			wantCause: `the read could not look up the mandatory role 'mand'@'%' in mysql\.user: Error 1142 \(42000\): SELECT command denied`,
			wantRoles: []string{"mand"},
		},
		{
			name: "an unrecognized version", version: "unknown",
			wantCause: `the server version "unknown" does not say which account-list grammar reads @@GLOBAL\.mandatory_roles = "mand"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, mandatoryQuery("mand", test.version, test.refused))

			got, err := NewMySQLReader(db.SQL, "app").readMandatoryRoles(t.Context(), &mysqlgrants.Names{})

			c.Assert(err, qt.IsNil)
			c.Assert(got.memberships, qt.IsNil)
			c.Assert(got.unread, qt.IsNotNil)
			c.Assert(got.unread.Cause, qt.Matches, test.wantCause)
			c.Assert(got.unread.Roles, qt.DeepEquals, test.wantRoles)
		})
	}
}

// TestReadMandatoryRoles_FailurePath fails the read when it was canceled: a
// cancellation is no answer about the setting.
func TestReadMandatoryRoles_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	db := dbtest.Open(t, mandatoryQuery("mand", "8.4.13", ""))

	got, err := NewMySQLReader(db.SQL, "app").readMandatoryRoles(ctx, &mysqlgrants.Names{})

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(got.memberships, qt.IsNil)
}

// TestMandatoryRoles_AddTo adds PUBLIC's memberships in the mandatory roles,
// and records that every account holds roles the read does not state where
// the mandatory roles were not read, or where a MySQL role is named PUBLIC
// beside a mandatory role, leaving the memberships as they are then.
func TestMandatoryRoles_AddTo(t *testing.T) {
	held := []catalog.RoleMembership{{Role: "reader", Member: "app"}}
	mandatory := []catalog.RoleMembership{{Role: "mand", Member: "PUBLIC"}}
	unread := &catalog.EveryAccountUnread{Cause: "the read could not read @@GLOBAL.mandatory_roles: denied"}
	tests := []struct {
		name         string
		mandatory    []catalog.RoleMembership
		unread       *catalog.EveryAccountUnread
		publicIsRole bool
		want         []catalog.RoleMembership
		wantUnread   *catalog.EveryAccountUnread
	}{
		{
			name:      "mandatory roles",
			mandatory: mandatory,
			want:      []catalog.RoleMembership{{Role: "reader", Member: "app"}, {Role: "mand", Member: "PUBLIC"}},
		},
		{name: "mandatory roles not read", unread: unread, want: held, wantUnread: unread},
		{
			name: "a role named PUBLIC", mandatory: mandatory, publicIsRole: true, want: held,
			wantUnread: &catalog.EveryAccountUnread{Roles: []string{"mand"}, Cause: publicRoleHidesMandatory},
		},
		{name: "a role named PUBLIC and no mandatory role", publicIsRole: true, want: held},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			schema := &catalog.Database{RoleMemberships: slices.Clone(held), PublicIsRole: test.publicIsRole}

			mandatoryRoles{memberships: test.mandatory, unread: test.unread}.addTo(schema)

			c.Assert(schema.RoleMemberships, qt.DeepEquals, test.want)
			c.Assert(schema.EveryAccountHoldsUnread, qt.DeepEquals, test.wantUnread)
		})
	}
}
