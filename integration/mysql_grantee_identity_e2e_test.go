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
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlschema"
)

type granteeIdentityRun struct {
	server                   mysqlFamilyServer
	database, target, suffix string
	role, pattern            string
}

// newGranteeIdentityRun gives a role and a login different privileges in
// every scope the privilege views hold. Only the role's privileges belong in
// its description, including when the names contain the grantee delimiter.
func newGranteeIdentityRun(c *qt.C, engine dbtarget.Engine, rolePrefix, userPrefix, host string) granteeIdentityRun {
	c.Helper()
	server := newMySQLFamilyServer(c, engine)
	database := server.database(c, "grantee")
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano()%1_000_000_000_000)
	run := granteeIdentityRun{
		server: server, database: database, target: server.url("mysql", database), suffix: suffix,
		role: rolePrefix + suffix, pattern: database[:len(database)-4] + "%",
	}
	role := granteeIdentifier(run.role)
	account := granteeIdentifier(userPrefix+suffix) + "@" + granteeIdentifier(host)
	c.Cleanup(func() {
		for _, statement := range []string{"DROP USER IF EXISTS " + account, "DROP ROLE IF EXISTS " + role} {
			_, err := server.admin.ExecContext(context.Background(), statement)
			c.Check(err, qt.IsNil, qt.Commentf("%s", statement))
		}
	})
	for _, statement := range []string{
		"CREATE ROLE " + role,
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
func TestMySQLFamilyGrantAccountIdentityE2E(t *testing.T) {
	names := []struct {
		name, role, user, host string
	}{
		{name: "same user on another host", role: "acct", user: "acct", host: "localhost"},
		{name: "at sign", role: "r@x", user: "r@x", host: "localhost"},
		{name: "apostrophe", role: "r'x", user: "r'x", host: "localhost"},
		{name: "backslash", role: `r\x`, user: `r\x`, host: "localhost"},
		{name: "grantee separator", role: "r'@'x", user: "r'@'x", host: "localhost"},
		{name: "user name case", role: "Case", user: "case", host: "%"},
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
			})
		}
	}
}

// db read and schema security use the same account identity. A role holding
// SELECT on one column reaches no whole relation; its same-named login's
// database SELECT must not turn that into access to every relation.
func TestMySQLFamilyGrantAccountIdentityCommandsE2E(t *testing.T) {
	for _, engine := range roleInheritEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newGranteeIdentityRun(c, engine.engine, "acct", "acct", "localhost")

			printed, err := runPtahNativeStdout("db", "read", "--db-url", run.target)

			c.Assert(err, qt.IsNil)
			model, _, err := sqlschema.Read([]byte(printed), engine.scheme)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", printed))
			c.Assert(model.Grants, qt.HasLen, 3)
			for _, grant := range model.Grants {
				c.Assert(grant.Role, qt.Equals, run.role)
			}
			c.Assert(reachFindingLines(c, run.target, run.suffix), qt.HasLen, 0)

			_, err = run.server.admin.ExecContext(c.Context(),
				"GRANT SELECT ON "+granteeIdentifier(run.database)+".* TO "+granteeIdentifier(run.role))
			c.Assert(err, qt.IsNil)
			c.Assert(reachFindingLines(c, run.target, run.suffix), qt.DeepEquals, []string{
				"roles/read-reach error acct 1/1 through=[] sources=[]",
			})
		})
	}
}
