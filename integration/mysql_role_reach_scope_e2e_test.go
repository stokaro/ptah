//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// roleLoginReads is how many of the tables a, b and c of database a session
// of a new login reads with role active through SET ROLE, the login granted
// role and nothing else. SET ROLE takes one role on MariaDB, where PUBLIC's
// privileges and the roles granted to PUBLIC apply without it. A statement
// the server refuses for want of a privilege counts as not read, and any
// other failure fails the test. The login is dropped when the test ends.
func roleLoginReads(c *qt.C, server mysqlFamilyServer, database, login, role string) int {
	c.Helper()
	account := granteeIdentifier(login) + "@'%'"
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+account)
		c.Check(err, qt.IsNil)
	})
	partialRevokeExec(c, server, []string{
		"CREATE USER " + account + " IDENTIFIED BY 'reach_scope_login_pw'",
		"GRANT " + role + " TO " + account,
	})
	config := server.config.Clone()
	config.User, config.Passwd, config.DBName = login, "reach_scope_login_pw", ""
	db, err := sql.Open("mysql", config.FormatDSN())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	session, err := db.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(session.Close(), qt.IsNil) })
	_, err = session.ExecContext(c.Context(), "SET ROLE "+role)
	c.Assert(err, qt.IsNil)
	reads := 0
	reached := map[bool]int{true: 1}
	for _, table := range []string{"a", "b", "c"} {
		_, err := session.ExecContext(c.Context(), readProbe(database, table))
		c.Assert(err == nil || refusedForPrivilege(err), qt.IsTrue, qt.Commentf("%s reading %s: %v", login, table, err))
		reads += reached[err == nil]
	}
	return reads
}

// unreadLoginSkip is the reason the reach rules give for the role lr, which
// holds the login legacy: the read reads the login's grants on the database
// and not its global privileges.
const unreadLoginSkip = "the read did not read every privilege of legacy, which lr can use: " +
	"it is a login, and the read reads a login's grants on the databases it covers, " +
	"not its global privileges or its grants on database name patterns; so their reach was not measured"

// reachSkips runs `ptah schema security` against dbURL and states the reach
// rules it skipped for a reason naming a role whose name ends in suffix, one
// line each, with the suffix taken off.
func reachSkips(c *qt.C, dbURL, suffix string) []string {
	c.Helper()
	stdout, err := runPtahNativeStdout("schema", "security", "--db-url", dbURL, "--format", "json", "--fail-on", "none")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", stdout))
	var report struct {
		Skipped []struct {
			Code   string `json:"code"`
			Reason string `json:"reason"`
		} `json:"skipped_rules"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil, qt.Commentf("%s", stdout))
	lines := make([]string, 0, len(report.Skipped))
	for _, skipped := range report.Skipped {
		if strings.HasPrefix(skipped.Code, "roles/") && strings.Contains(skipped.Reason, suffix) {
			lines = append(lines, strings.ReplaceAll(skipped.Code+": "+skipped.Reason, suffix, ""))
		}
	}
	return lines
}

// TestMariaDBPublicRoleDatabaseGrantReachE2E grants PUBLIC a role holding
// SELECT on a whole database, beside a role holding two of its three tables.
// A login granted only that other role reads every table with it active,
// measured on MariaDB 11.8.9 and 12.3.3, so the analysis counts PUBLIC's
// role's database row for every role: PUBLIC is reported reaching every
// table, and the other role, which reaches no more, is not reported on its
// own. Without the row it would be reported reaching two tables of three,
// over the 50% the warning starts at.
func TestMariaDBPublicRoleDatabaseGrantReachE2E(t *testing.T) {
	for _, row := range mariaDBPublicEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLFamilyServer(c, row.engine)
			database := server.database(c, "public_db_reach")
			suffix := fmt.Sprintf("_%d", time.Now().UnixNano()%1_000_000_000_000)
			public, other := granteeIdentifier("pr6"+suffix), granteeIdentifier("other6"+suffix)
			c.Cleanup(func() {
				for _, statement := range []string{
					"REVOKE " + public + " FROM PUBLIC",
					"DROP ROLE IF EXISTS " + public,
					"DROP ROLE IF EXISTS " + other,
				} {
					_, err := server.admin.ExecContext(context.Background(), statement)
					c.Check(err, qt.IsNil, qt.Commentf("%s", statement))
				}
			})
			partialRevokeExec(c, server, []string{
				"CREATE TABLE " + granteeIdentifier(database) + ".a (id int PRIMARY KEY)",
				"CREATE TABLE " + granteeIdentifier(database) + ".b (id int PRIMARY KEY)",
				"CREATE TABLE " + granteeIdentifier(database) + ".c (id int PRIMARY KEY)",
				"CREATE ROLE " + public,
				"CREATE ROLE " + other,
				"GRANT SELECT ON " + granteeIdentifier(database) + ".* TO " + public,
				"GRANT SELECT ON " + granteeIdentifier(database) + ".a TO " + other,
				"GRANT SELECT ON " + granteeIdentifier(database) + ".b TO " + other,
				"GRANT " + public + " TO PUBLIC",
			})

			lines := publicFindingLines(c, server.url("mariadb", database), suffix)

			c.Assert(roleLoginReads(c, server, database, "o6l"+suffix, other), qt.Equals, 3)
			c.Assert(lines, qt.DeepEquals, []string{
				"ROL04 info other6",
				"roles/read-reach error PUBLIC 3/3 through=[pr6]",
			})
		})
	}
}

// TestMySQLRoleNamedPublicReachE2E creates a MySQL role named PUBLIC, which
// MySQL allows, holding a role that holds SELECT on a whole database, beside
// a role holding two tables of it. The role named PUBLIC is a role like any
// other: a login granted only the other role reads those two tables with it
// active, and is refused the third with Error 1142. The analysis measures the
// other role's own reach, reports the role named PUBLIC and the role it holds
// as roles, the role named PUBLIC among the roles nobody holds, and no grant
// to PUBLIC as one every role holds. A role named pUbLiC is the same case:
// the read folds the name as it folds MariaDB's grantee.
func TestMySQLRoleNamedPublicReachE2E(t *testing.T) {
	tests := []struct {
		public string
		want   []string
	}{
		{public: "PUBLIC", want: []string{
			"ROL04 info PUBLIC",
			"ROL04 info other7",
			"roles/read-reach error PUBLIC 3/3 through=[big7]",
			"roles/read-reach error big7 3/3 through=[]",
			"roles/read-reach warning other7 2/3 through=[]",
		}},
		{public: "pUbLiC", want: []string{
			"ROL04 info other7",
			"ROL04 info pUbLiC",
			"roles/read-reach error big7 3/3 through=[]",
			"roles/read-reach warning other7 2/3 through=[]",
			"roles/read-reach error pUbLiC 3/3 through=[big7]",
		}},
	}
	for _, test := range tests {
		t.Run(test.public, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
			database := server.database(c, "public_role_reach")
			suffix := fmt.Sprintf("_%d", time.Now().UnixNano()%1_000_000_000_000)
			big, other := granteeIdentifier("big7"+suffix), granteeIdentifier("other7"+suffix)
			named := granteeIdentifier(test.public)
			c.Cleanup(func() {
				_, err := server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+big+", "+other+", "+named)
				c.Check(err, qt.IsNil)
			})
			partialRevokeExec(c, server, []string{
				"CREATE TABLE " + granteeIdentifier(database) + ".a (id int PRIMARY KEY)",
				"CREATE TABLE " + granteeIdentifier(database) + ".b (id int PRIMARY KEY)",
				"CREATE TABLE " + granteeIdentifier(database) + ".c (id int PRIMARY KEY)",
				"CREATE ROLE " + named + ", " + big + ", " + other,
				"GRANT SELECT ON " + granteeIdentifier(database) + ".* TO " + big,
				"GRANT " + big + " TO " + named,
				"GRANT SELECT ON " + granteeIdentifier(database) + ".a TO " + other,
				"GRANT SELECT ON " + granteeIdentifier(database) + ".c TO " + other,
				"GRANT SELECT ON " + granteeIdentifier(database) + ".b TO " + named,
			})
			target := server.url("mysql", database)

			stdout, err := runPtahNativeStdout("schema", "security", "--db-url", target, "--format", "json", "--fail-on", "none")
			lines := publicFindingLines(c, target, suffix)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", stdout))
			c.Assert(stdout, qt.Not(qt.Contains), `"code": "PRV03"`)
			c.Assert(roleLoginReads(c, server, database, "o7l"+suffix, other), qt.Equals, 2)
			c.Assert(lines, qt.DeepEquals, test.want)
		})
	}
}

// TestMySQLReachSkipsTheRolesHoldingAnUnreadAccountE2E grants a role an
// account the read does not read as a role, a locked user holding SELECT on
// a whole database. The role's reach is not measured, and the skip names the
// account and the role; another role on the server is measured as usual.
func TestMySQLReachSkipsTheRolesHoldingAnUnreadAccountE2E(t *testing.T) {
	c := qt.New(t)
	server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
	database := server.database(c, "unread_reach")
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano()%1_000_000_000_000)
	legacy := granteeIdentifier("legacy"+suffix) + "@'%'"
	holder, other := granteeIdentifier("lr"+suffix), granteeIdentifier("lother"+suffix)
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+holder+", "+other)
		c.Check(err, qt.IsNil)
		_, err = server.admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+legacy)
		c.Check(err, qt.IsNil)
	})
	partialRevokeExec(c, server, []string{
		"CREATE TABLE " + granteeIdentifier(database) + ".a (id int PRIMARY KEY)",
		"CREATE TABLE " + granteeIdentifier(database) + ".b (id int PRIMARY KEY)",
		"CREATE ROLE " + holder + ", " + other,
		"CREATE USER " + legacy + " ACCOUNT LOCK",
		"GRANT SELECT ON " + granteeIdentifier(database) + ".* TO " + legacy,
		"GRANT " + legacy + " TO " + holder,
		"GRANT SELECT ON " + granteeIdentifier(database) + ".* TO " + other,
	})
	target := server.url("mysql", database)

	lines := publicFindingLines(c, target, suffix)
	skips := reachSkips(c, target, suffix)

	c.Assert(slices.DeleteFunc(lines, func(line string) bool { return strings.HasPrefix(line, "ROL04 ") }), qt.DeepEquals, []string{
		"roles/read-reach error lother 2/2 through=[]",
	})
	c.Assert(skips, qt.DeepEquals, []string{
		"roles/read-reach: " + unreadLoginSkip,
		"roles/write-reach: " + unreadLoginSkip,
	})
}

// TestMySQLReadRefusesOnlyTheNameTwoAccountsShareE2E creates the role
// 'nm@localhost'@'%' and the login 'nm'@'localhost', which both read as the
// principal nm@localhost, and grants the login a role. The read leaves that
// name out and reads the rest: `db read`, `schema security` and a plan
// that names other roles succeed, and a plan that names the shared name is
// refused, since it cannot say which account it changes.
func TestMySQLReadRefusesOnlyTheNameTwoAccountsShareE2E(t *testing.T) {
	c := qt.New(t)
	server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
	database := server.database(c, "shared_name")
	suffix := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000_000_000)
	shared := "nm" + suffix + "@localhost"
	role, login := granteeIdentifier(shared)+"@'%'", granteeIdentifier("nm"+suffix)+"@'localhost'"
	held := granteeIdentifier("nmr" + suffix)
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+login)
		c.Check(err, qt.IsNil)
		_, err = server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role+", "+held)
		c.Check(err, qt.IsNil)
	})
	partialRevokeExec(c, server, []string{
		"CREATE TABLE " + granteeIdentifier(database) + ".t1 (id int PRIMARY KEY)",
		"CREATE ROLE " + role + ", " + held,
		"CREATE USER " + login + " IDENTIFIED BY 'shared_name_pw'",
		"GRANT SELECT ON " + granteeIdentifier(database) + ".* TO " + role,
		"GRANT " + held + " TO " + login,
	})
	target := server.url("mysql", database)
	schema := func(role string) string {
		path := filepath.Join(c.TempDir(), "schema.yaml")
		c.Assert(os.WriteFile(path, []byte("tables:\n  t1:\n    columns:\n      id:\n        type: INT\n        primary: true\n"+
			"roles:\n  "+strconv.Quote(role)+": {}\n"), 0o600), qt.IsNil)
		return path
	}
	run := func(args ...string) clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute}, args...)
	}

	read := run("db", "read", "--db-url", target)
	security := run("schema", "security", "--db-url", target, "--fail-on", "none")
	other := run("schema", "apply", "--dry-run", "--db-url", target, "--schema-file", schema("nmr"+suffix))
	named := run("schema", "apply", "--dry-run", "--db-url", target, "--schema-file", schema(shared))

	c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", read.Stdout, read.Stderr))
	c.Assert(read.Stdout, qt.Contains, "CREATE ROLE IF NOT EXISTS `nmr"+suffix+"`;")
	c.Assert(read.Stdout, qt.Not(qt.Contains), "`"+shared+"`")
	c.Assert(security.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", security.Stdout, security.Stderr))
	c.Assert(other.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", other.Stdout, other.Stderr))
	c.Assert(named.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", named.Stdout, named.Stderr))
	c.Assert(named.Stderr, qt.Contains, fmt.Sprintf("role %q names a principal the read could not resolve", shared))
}
