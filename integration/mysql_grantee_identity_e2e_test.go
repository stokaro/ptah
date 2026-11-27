//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlschema"
)

type granteeIdentityRun struct {
	server                   mysqlFamilyServer
	database, target, suffix string
	role, pattern            string
	// held is a role granted to the login and to no role, which reads the
	// table.
	held string
}

// newGranteeIdentityRun gives a role and a login different privileges in
// every scope the privilege views hold, and grants the login another role.
// Only the role's privileges and memberships belong in its description,
// including when the names contain the grantee delimiter.
func newGranteeIdentityRun(c *qt.C, engine dbtarget.Engine, rolePrefix, userPrefix, host string) granteeIdentityRun {
	c.Helper()
	server := newMySQLFamilyServer(c, engine)
	database := server.database(c, "grantee")
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano()%1_000_000_000_000)
	run := granteeIdentityRun{
		server: server, database: database, target: server.url("mysql", database), suffix: suffix,
		role: rolePrefix + suffix, pattern: database[:len(database)-4] + "%", held: "gimheld" + suffix,
	}
	role, held := granteeIdentifier(run.role), granteeIdentifier(run.held)
	account := granteeIdentifier(userPrefix+suffix) + "@" + granteeIdentifier(host)
	c.Cleanup(func() {
		for _, statement := range []string{"DROP USER IF EXISTS " + account, "DROP ROLE IF EXISTS " + role, "DROP ROLE IF EXISTS " + held} {
			_, err := server.admin.ExecContext(context.Background(), statement)
			c.Check(err, qt.IsNil, qt.Commentf("%s", statement))
		}
	})
	for _, statement := range []string{
		"CREATE ROLE " + role,
		"CREATE ROLE " + held,
		"CREATE USER " + account,
		"CREATE TABLE " + granteeIdentifier(database) + ".items (id INT PRIMARY KEY)",
		"GRANT SELECT ON " + granteeIdentifier(database) + ".* TO " + account,
		"GRANT INSERT ON " + granteeIdentifier(database) + ".items TO " + account,
		"GRANT UPDATE (id) ON " + granteeIdentifier(database) + ".items TO " + account,
		"GRANT DELETE ON " + granteeIdentifier(run.pattern) + ".* TO " + account,
		"GRANT SHOW VIEW ON " + granteeIdentifier(database) + ".* TO " + role,
		"GRANT REFERENCES ON " + granteeIdentifier(database) + ".items TO " + role,
		"GRANT SELECT (id) ON " + granteeIdentifier(database) + ".items TO " + role,
		"GRANT ALTER ON " + granteeIdentifier(run.pattern) + ".* TO " + role,
		"GRANT SELECT ON " + granteeIdentifier(database) + ".items TO " + held,
		"GRANT " + held + " TO " + account,
	} {
		_, err := server.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return run
}

func granteeIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// A same-named login on another host contributes no privilege to the role.
// The complete account comparison also preserves names the privilege views
// surround with quotes without escaping their bytes.
//
// The login's membership reads under the name `GRANT r TO name` means: the
// user alone for an account on host %, and `user@host` elsewhere. MariaDB
// gives a role no host, and grants to the role where one has the name, so its
// login on host % carries the host only beside a role of the same name.
func TestMySQLFamilyGrantAccountIdentityE2E(t *testing.T) {
	names := []struct {
		name, role, user, host string
		// shown is how the login's name in a membership spells its host,
		// by engine: nothing for the host a role takes.
		shown map[string]string
	}{
		{name: "same user on another host", role: "acct", user: "acct", host: "localhost",
			shown: map[string]string{"mysql": "@localhost", "mariadb": "@localhost"}},
		{name: "at sign", role: "r@x", user: "r@x", host: "localhost",
			shown: map[string]string{"mysql": "@localhost", "mariadb": "@localhost"}},
		{name: "apostrophe", role: "r'x", user: "r'x", host: "localhost",
			shown: map[string]string{"mysql": "@localhost", "mariadb": "@localhost"}},
		{name: "backslash", role: `r\x`, user: `r\x`, host: "localhost",
			shown: map[string]string{"mysql": "@localhost", "mariadb": "@localhost"}},
		{name: "grantee separator", role: "r'@'x", user: "r'@'x", host: "localhost",
			shown: map[string]string{"mysql": "@localhost", "mariadb": "@localhost"}},
		{name: "user name case", role: "Case", user: "case", host: "%",
			shown: map[string]string{"mysql": "", "mariadb": ""}},
	}
	for _, engine := range roleInheritEngines {
		for _, name := range names {
			t.Run(engine.name+"/"+name.name, func(t *testing.T) {
				c := qt.New(t)
				run := newGranteeIdentityRun(c, engine.engine, name.role, name.user, name.host)
				conn, err := dbschema.ConnectToDatabase(c.Context(), run.target)
				c.Assert(err, qt.IsNil)
				defer dbschema.CloseAndWarn(conn)

				read, err := conn.Reader().ReadSchemaContext(c.Context())

				c.Assert(err, qt.IsNil)
				grants := slices.DeleteFunc(read.Grants, func(grant catalog.Grant) bool { return grant.Role != run.role })
				c.Assert(grants, qt.ContentEquals, []catalog.Grant{
					{Role: run.role, Privilege: "SHOW VIEW", ObjectType: "SCHEMA", ObjectName: run.database},
					{Role: run.role, Privilege: "REFERENCES", ObjectType: "TABLE", Schema: run.database, ObjectName: "items"},
					{Role: run.role, Privilege: "SELECT", ObjectType: "TABLE", Schema: run.database, ObjectName: "items", Column: "id"},
				})
				wide := slices.DeleteFunc(read.WideGrants, func(grant catalog.WideGrant) bool { return grant.Role != run.role })
				c.Assert(wide, qt.DeepEquals, []catalog.WideGrant{{
					Role: run.role, Privilege: "ALTER", Grant: "ALTER ON " + granteeIdentifier(run.pattern) + ".*", Schema: run.database,
				}})
				// MariaDB grants a new role to its creator with the admin
				// option, and that edge names the admin connection.
				memberships := slices.DeleteFunc(read.RoleMemberships, func(membership catalog.RoleMembership) bool {
					return membership.Role != run.held || membership.AdminOption
				})
				c.Assert(memberships, qt.DeepEquals, []catalog.RoleMembership{{
					Role: run.held, Member: name.user + run.suffix + name.shown[engine.name],
				}})
			})
		}
	}
}

// db read and schema security use the same account identity. A role holding
// SELECT on one column reaches no whole relation; its same-named login's
// database SELECT must not turn that into access to every relation.
//
// The control grants the role SELECT on the table, not on the database. The
// fixture's database name holds "_", which a database-level grant reads as a
// wildcard, so the role's row on it is not an exact name. A MySQL role session
// applies the exact-name row, or else the byte-smallest matching wildcard name,
// and the role's ALTER pattern sorts before the database's own name: measured
// on MySQL 26.10, a member of a role holding SELECT ON `mgi_dbx`.* and ALTER ON
// `mgi_d%`.* is refused SELECT on mgi_dbx.items with Error 1142, in either
// grant order. A table grant names its database literally.
func TestMySQLFamilyGrantAccountIdentityCommandsE2E(t *testing.T) {
	for _, engine := range roleInheritEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newGranteeIdentityRun(c, engine.engine, "acct", "acct", "localhost")

			printed, err := runPtahNativeStdout("db", "read", "--db-url", run.target)

			c.Assert(err, qt.IsNil)
			c.Assert(printed, qt.Not(qt.Contains), "GRANT "+granteeIdentifier(run.held)+" TO")
			model, _, err := sqlschema.Read([]byte(printed), engine.scheme)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", printed))
			own := slices.DeleteFunc(model.Grants, func(grant schemamodel.Grant) bool { return grant.Role != run.role })
			c.Assert(own, qt.HasLen, 3)
			c.Assert(reachFindingLines(c, run.target, run.suffix), qt.DeepEquals, []string{
				"roles/read-reach error gimheld 1/1 through=[] sources=[]",
			})

			_, err = run.server.admin.ExecContext(c.Context(),
				// #nosec G202 -- Both names belong to this fixture and granteeIdentifier doubles embedded backticks.
				"GRANT SELECT ON "+granteeIdentifier(run.database)+".items TO "+granteeIdentifier(run.role),
			)
			c.Assert(err, qt.IsNil)
			c.Assert(reachFindingLines(c, run.target, run.suffix), qt.DeepEquals, []string{
				"roles/read-reach error acct 1/1 through=[] sources=[]",
				"roles/read-reach error gimheld 1/1 through=[] sources=[]",
			})
		})
	}
}

// TestMySQLRoleHostIdentityE2E holds two MySQL roles that share a user name on
// two hosts apart. A statement naming the role by its name alone means the one
// on host %, and that is the only one a read describes as the role: the server
// keeps the other's privileges and memberships to the accounts that hold it.
// Measured on MySQL 8.4.13 and 26.10.0, a session with 'mdup'@'%' active is
// refused a table 'mdup'@'localhost' reads, with Error 1142. MariaDB has no
// role hosts, and refuses CREATE ROLE 'r'@'localhost' with Error 1064.
func TestMySQLRoleHostIdentityE2E(t *testing.T) {
	c := qt.New(t)
	server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
	database := server.database(c, "role_host")
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano()%1_000_000_000_000)
	role, held := "rhost"+suffix, "rhheld"+suffix
	onAny, local := granteeIdentifier(role)+"@'%'", granteeIdentifier(role)+"@'localhost'"
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+onAny+", "+local+", "+granteeIdentifier(held))
		c.Check(err, qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE TABLE " + granteeIdentifier(database) + ".items (id int PRIMARY KEY)",
		"CREATE ROLE " + onAny + ", " + local + ", " + granteeIdentifier(held),
		"GRANT SELECT ON " + granteeIdentifier(database) + ".* TO " + local,
		"GRANT SELECT ON " + granteeIdentifier(database) + ".items TO " + granteeIdentifier(held),
		"GRANT " + granteeIdentifier(held) + " TO " + local,
	} {
		_, err := server.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	target := server.url("mysql", database)
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	read, err := conn.Reader().ReadSchemaContext(c.Context())

	c.Assert(err, qt.IsNil)
	roles := slices.DeleteFunc(read.Roles, func(described catalog.Role) bool { return !strings.HasSuffix(described.Name, suffix) })
	c.Assert(roles, qt.DeepEquals, []catalog.Role{
		{Name: held, Inherit: true, PasswordState: catalog.RolePasswordAbsent},
		{Name: role, Inherit: true, PasswordState: catalog.RolePasswordAbsent},
	})
	c.Assert(slices.DeleteFunc(read.Grants, func(grant catalog.Grant) bool { return grant.Role != role }), qt.HasLen, 0)
	memberships := slices.DeleteFunc(read.RoleMemberships, func(membership catalog.RoleMembership) bool { return membership.Role != held })
	c.Assert(memberships, qt.DeepEquals, []catalog.RoleMembership{{Role: held, Member: role + "@localhost"}})
	c.Assert(reachFindingLines(c, target, suffix), qt.DeepEquals, []string{
		"roles/read-reach error rhheld 1/1 through=[] sources=[]",
	})
}

// TestMySQLRoleInheritedFromAnotherHostE2E grants a role on host % a role on
// another host. A read describes only the first, so the privileges the second
// passes on were not read, and a reach measured without them says the member
// reaches less than the server lets it: measured on MySQL 8.4.13, a session
// with the first active reads the table the second holds SELECT on. The read
// then leaves the reach unmeasured rather than short. The control is the same
// graph without that membership, measured.
func TestMySQLRoleInheritedFromAnotherHostE2E(t *testing.T) {
	tests := []struct {
		name string
		// grant is the membership between the two roles, which it spells
		// {via} and {local}.
		grant     []string
		wantReach bool
	}{
		{name: "a role on another host is inherited", grant: []string{"GRANT {local} TO {via}"}, wantReach: false},
		{name: "control: nothing is inherited from another host", grant: nil, wantReach: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
			database := server.database(c, "role_via_host")
			suffix := fmt.Sprintf("_%d", time.Now().UnixNano()%1_000_000_000_000)
			via, local := granteeIdentifier("rvia"+suffix)+"@'%'", granteeIdentifier("rloc"+suffix)+"@'localhost'"
			c.Cleanup(func() {
				_, err := server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+via+", "+local)
				c.Check(err, qt.IsNil)
			})
			statements := []string{
				"CREATE TABLE " + granteeIdentifier(database) + ".items (id int PRIMARY KEY)",
				"CREATE ROLE " + via + ", " + local,
				"GRANT SELECT ON " + granteeIdentifier(database) + ".* TO " + local,
			}
			for _, grant := range test.grant {
				statements = append(statements, strings.NewReplacer("{local}", local, "{via}", via).Replace(grant))
			}
			for _, statement := range statements {
				_, err := server.admin.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
			}
			conn, err := dbschema.ConnectToDatabase(c.Context(), server.url("mysql", database))
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			read, err := conn.Reader().ReadSchemaContext(c.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(read.WideAccess != nil, qt.Equals, test.wantReach)
		})
	}
}

// TestMariaDBLoginNamedLikeARoleE2E grants a role to a MariaDB login on host
// % that shares its user name with a role. `GRANT r TO name` grants the role
// there, measured on MariaDB 11.8.9 and 12.3.3, so the login's membership
// reads as `user@%`, and the role's own memberships stay its own. MySQL has no
// such pair: a role and a login on one host are one account.
func TestMariaDBLoginNamedLikeARoleE2E(t *testing.T) {
	c := qt.New(t)
	run := newGranteeIdentityRun(c, dbtarget.MariaDBAdmin, "pct", "pct", "%")
	conn, err := dbschema.ConnectToDatabase(c.Context(), run.target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	read, err := conn.Reader().ReadSchemaContext(c.Context())

	c.Assert(err, qt.IsNil)
	memberships := slices.DeleteFunc(read.RoleMemberships, func(membership catalog.RoleMembership) bool {
		return membership.Role != run.held || membership.AdminOption
	})
	c.Assert(memberships, qt.DeepEquals, []catalog.RoleMembership{{Role: run.held, Member: run.role + "@%"}})
	c.Assert(reachFindingLines(c, run.target, run.suffix), qt.DeepEquals, []string{
		"roles/read-reach error gimheld 1/1 through=[] sources=[]",
	})
}
