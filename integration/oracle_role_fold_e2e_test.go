//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A role or a table an Oracle schema names in lower case is the object Oracle
// stores under the name: READER for a bare reader, and reader for a quoted
// "reader". So the second apply plans nothing, and a member reads the table.
// Measured on Oracle 23.26.3 with the names compared as written, the second
// apply of a bare reader plans CREATE ROLE again and fails with ORA-01921, and
// the second apply of a grant on a quoted "t_q" plans a REVOKE on PTAH.t_q
// and fails with ORA-00942. The quoted role row holds the fold to bare names:
// folding a read role named reader as well makes it READER, and the second
// apply plans CREATE ROLE "reader" again.
func TestSchemaApplyOracleLowerCaseRoleConvergesE2E(t *testing.T) {
	const bare = "CREATE TABLE t1 (id NUMBER(10) PRIMARY KEY);\n"
	tests := []struct {
		name     string
		setup    []string
		declared string
		role     string
		table    string
		query    string
	}{
		{
			name:     "a role name with surrounding spaces",
			declared: `CREATE ROLE " PTAH_RF_{stamp} ";` + "\n" + bare + `GRANT SELECT ON t1 TO " PTAH_RF_{stamp} ";` + "\n",
			role:     " PTAH_RF_{stamp} ",
			table:    "T1",
			query:    "SELECT id FROM {account}.t1",
		},
		{
			name:     "a bare role name the schema creates",
			declared: "CREATE ROLE ptah_rf_{stamp};\n" + bare + "GRANT SELECT ON t1 TO ptah_rf_{stamp};\n",
			role:     "PTAH_RF_{stamp}",
			table:    "T1",
			query:    "SELECT id FROM {account}.t1",
		},
		{
			name:     "a quoted role name the schema creates",
			declared: `CREATE ROLE "ptah_rf_{stamp}";` + "\n" + bare + `GRANT SELECT ON t1 TO "ptah_rf_{stamp}";` + "\n",
			role:     "ptah_rf_{stamp}",
			table:    "T1",
			query:    "SELECT id FROM {account}.t1",
		},
		{
			name:     "a bare role name the schema does not create",
			setup:    []string{`CREATE ROLE "PTAH_RF_{stamp}"`},
			declared: bare + "GRANT SELECT ON t1 TO ptah_rf_{stamp};\n",
			role:     "PTAH_RF_{stamp}",
			table:    "T1",
			query:    "SELECT id FROM {account}.t1",
		},
		{
			name: "a quoted table name",
			declared: "CREATE ROLE PTAH_RF_{stamp};\n" + `CREATE TABLE "t_q" (id NUMBER(10) PRIMARY KEY);` + "\n" +
				`GRANT SELECT ON "t_q" TO PTAH_RF_{stamp};` + "\n",
			role:  "PTAH_RF_{stamp}",
			table: "t_q",
			query: `SELECT id FROM {account}."t_q"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			stamp := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000_000)
			declared := strings.ReplaceAll(test.declared, "{stamp}", stamp)
			stored := strings.ReplaceAll(test.role, "{stamp}", stamp)
			setup := make([]string, 0, len(test.setup))
			for _, statement := range test.setup {
				setup = append(setup, strings.ReplaceAll(statement, "{stamp}", stamp))
			}
			account := newOracleRoleFoldAccount(c, stamp, stored, setup...)

			for range 2 {
				applied := account.apply(c, declared, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			}
			again := account.apply(c, declared, "--dry-run")

			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			c.Assert(account.privileges(c, stored, test.table), qt.DeepEquals, []string{"SELECT"})
			member := account.member(c, stored)
			_, err := member.ExecContext(c.Context(), strings.ReplaceAll(test.query, "{account}", account.name))
			c.Assert(err, qt.IsNil)
		})
	}
}

// A grant the read reports for a role and a table created quoted is revoked
// by their quoted names once the schema stops declaring it, so the role holds
// nothing and a member is refused the table. Measured on Oracle 23.26.3 with
// the read's names left as stored, the second apply plans CREATE ROLE again
// and fails with ORA-01921, and with only the role spelled the REVOKE names
// PTAH.t_q and fails with ORA-00942.
func TestSchemaApplyOracleRevokesFromAQuotedRoleE2E(t *testing.T) {
	c := qt.New(t)
	stamp := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000_000)
	role := "ptah_rf_" + stamp
	account := newOracleRoleFoldAccount(c, stamp, role)
	table := `CREATE ROLE "` + role + `";` + "\n" + `CREATE TABLE "t_q" (id NUMBER(10) PRIMARY KEY);` + "\n"
	granted := account.apply(c, table+`GRANT SELECT ON "t_q" TO "`+role+`";`+"\n", "--auto-approve")

	c.Assert(granted.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", granted.Stdout, granted.Stderr))
	c.Assert(account.privileges(c, role, "t_q"), qt.DeepEquals, []string{"SELECT"})
	revoked := account.apply(c, table, "--auto-approve")

	c.Assert(revoked.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", revoked.Stdout, revoked.Stderr))
	c.Assert(account.privileges(c, role, "t_q"), qt.IsNil)
	again := account.apply(c, table, "--dry-run")

	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
	member := account.member(c, role)
	_, err := member.ExecContext(c.Context(), strings.ReplaceAll(`SELECT id FROM {account}."t_q"`, "{account}", account.name))
	c.Assert(err, qt.ErrorMatches, `(?s).*ORA-00942.*`)
}

// A role and a table created quoted, and a grant on the table to PUBLIC, are
// written by `db read` with the names that address them. Replayed after both
// objects are gone, the file creates the role stored as ptah_rq_<stamp> and the
// table stored as qt_q, grants SELECT to the role and to PUBLIC again, and the
// next plan is empty. A file that wrote the names bare would create PTAH_RQ_
// and QT_Q instead.
func TestDBReadOracleQuotedNamesReplayE2E(t *testing.T) {
	c := qt.New(t)
	stamp := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000_000)
	role := "ptah_rq_" + stamp
	account := newOracleRoleFoldAccount(c, stamp, role, `CREATE ROLE "`+role+`"`)
	for _, statement := range []string{
		`CREATE TABLE "qt_q" (id NUMBER(10) PRIMARY KEY)`,
		`GRANT SELECT ON "qt_q" TO "` + role + `"`,
		`GRANT SELECT ON "qt_q" TO PUBLIC`,
	} {
		_, err := account.owner.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	read := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: 3 * time.Minute},
		"db", "read", "--db-url", account.url)

	c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", read.Stdout, read.Stderr))
	c.Assert(read.Stdout, qt.Contains, `CREATE ROLE "`+role+`";`)
	c.Assert(read.Stdout, qt.Contains, "CREATE TABLE "+account.name+`."qt_q" (`)
	c.Assert(read.Stdout, qt.Contains, "GRANT SELECT ON "+account.name+`."qt_q" TO "PUBLIC";`)
	c.Assert(read.Stdout, qt.Contains, "GRANT SELECT ON "+account.name+`."qt_q" TO "`+role+`";`)
	execOracle(c.Context(), c, account.admin, "DROP TABLE "+account.name+`."qt_q" PURGE`)
	execOracle(c.Context(), c, account.admin, `DROP ROLE "`+role+`"`)
	c.Assert(account.roles(c, role), qt.IsNil)
	replayed := account.apply(c, read.Stdout, "--auto-approve")

	c.Assert(replayed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", replayed.Stdout, replayed.Stderr))
	c.Assert(account.roles(c, role), qt.DeepEquals, []string{role})
	c.Assert(account.privileges(c, role, "qt_q"), qt.DeepEquals, []string{"SELECT"})
	c.Assert(account.privileges(c, "PUBLIC", "qt_q"), qt.DeepEquals, []string{"SELECT"})
	again := account.apply(c, read.Stdout, "--dry-run")

	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
	member := account.member(c, role)
	_, err := member.ExecContext(c.Context(), strings.ReplaceAll(`SELECT id FROM {account}."qt_q"`, "{account}", account.name))
	c.Assert(err, qt.IsNil)
}

// A bare role name declared beside a role created quoted under the same
// letters is a different role: the plan creates it with the bare name, the
// server stores it as PTAH_RF_<stamp> and it holds SELECT, and the role stored
// as ptah_rf_<stamp> is left as it was and holds nothing. Matched by its
// letters alone, the declared role would be the existing one, the plan would
// create nothing, and the GRANT would go to it.
func TestSchemaApplyOracleBareRoleBesideAQuotedRoleE2E(t *testing.T) {
	c := qt.New(t)
	stamp := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000_000)
	lower := "ptah_rf_" + stamp
	upper := "PTAH_RF_" + stamp
	account := newOracleRoleFoldAccount(c, stamp, upper, `CREATE ROLE "`+lower+`"`)
	c.Cleanup(func() { dropOracleRole(context.Background(), c, account.admin, `"`+lower+`"`) })
	declared := "CREATE ROLE " + lower + ";\nCREATE TABLE t1 (id NUMBER(10) PRIMARY KEY);\nGRANT SELECT ON t1 TO " + lower + ";\n"
	planned := account.apply(c, declared, "--dry-run")

	c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
	c.Assert(planned.Stdout, qt.Contains, "CREATE ROLE "+lower+";")
	for range 2 {
		applied := account.apply(c, declared, "--auto-approve")

		c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	}
	again := account.apply(c, declared, "--dry-run")

	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
	c.Assert(account.roles(c, lower, upper), qt.DeepEquals, []string{lower, upper})
	c.Assert(account.privileges(c, upper, "T1"), qt.DeepEquals, []string{"SELECT"})
	c.Assert(account.privileges(c, lower, "T1"), qt.IsNil)
	member := account.member(c, upper)
	_, err := member.ExecContext(c.Context(), strings.ReplaceAll("SELECT id FROM {account}.t1", "{account}", account.name))
	c.Assert(err, qt.IsNil)
}

// oracleRoleFoldAccount is an account that may read the role catalog and
// create roles, and owns what an apply creates.
type oracleRoleFoldAccount struct {
	name      string
	url       string
	driverURL string
	owner     *sql.DB
	admin     *dbschema.DatabaseConnection
}

// newOracleRoleFoldAccount creates the account and runs each setup statement
// as the administrator. The account, the role stored as stored and the member
// user are dropped when the test ends.
func newOracleRoleFoldAccount(c *qt.C, stamp, stored string, setup ...string) oracleRoleFoldAccount {
	c.Helper()
	ctx := c.Context()
	adminURL := dbtarget.URL(c, dbtarget.OracleAdmin)
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	account := "PTAH_RF_A_" + stamp
	c.Cleanup(func() {
		dropOracleUser(context.Background(), c, admin, "PTAH_RF_M_"+stamp)
		dropOracleUser(context.Background(), c, admin, account)
		dropOracleRole(context.Background(), c, admin, `"`+stored+`"`)
	})
	createOracleUser(ctx, c, admin, account)
	execOracle(ctx, c, admin, "GRANT SELECT_CATALOG_ROLE, CREATE ROLE TO "+account)
	for _, statement := range setup {
		execOracle(ctx, c, admin, statement)
	}
	accountURL := oracleURLAs(c, adminURL, account)
	driverURL := oracleRevokeAllDriverURL(c, accountURL)
	owner, err := sql.Open("oracle", driverURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(owner.Close(), qt.IsNil) })
	return oracleRoleFoldAccount{name: account, url: accountURL, driverURL: driverURL, owner: owner, admin: admin}
}

// apply runs `ptah schema apply` as the account with the desired schema and
// the extra arguments.
func (a oracleRoleFoldAccount) apply(c *qt.C, desired string, args ...string) clirun.Result {
	c.Helper()
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte(desired), 0o600), qt.IsNil)
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: 3 * time.Minute},
		append([]string{"schema", "apply", "--db-url", a.url, "--schema-file", file}, args...)...)
}

// privileges is what the role stored as stored holds on the table stored as
// table, in order.
func (a oracleRoleFoldAccount) privileges(c *qt.C, stored, table string) []string {
	c.Helper()
	rows, err := a.owner.QueryContext(c.Context(),
		"SELECT privilege FROM all_tab_privs WHERE grantee = :1 AND table_schema = :2 AND table_name = :3 ORDER BY privilege",
		stored, a.name, table)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var held []string
	for rows.Next() {
		var privilege string
		c.Assert(rows.Scan(&privilege), qt.IsNil)
		held = append(held, privilege)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return held
}

// roles is each of the names a role is stored under on the server, in the
// order given, leaving out the ones no role has.
func (a oracleRoleFoldAccount) roles(c *qt.C, stored ...string) []string {
	c.Helper()
	var found []string
	for _, name := range stored {
		rows, err := a.owner.QueryContext(c.Context(), "SELECT role FROM dba_roles WHERE role = :1", name)
		c.Assert(err, qt.IsNil)
		for rows.Next() {
			var role string
			c.Assert(rows.Scan(&role), qt.IsNil)
			found = append(found, role)
		}
		c.Assert(rows.Err(), qt.IsNil)
		c.Assert(rows.Close(), qt.IsNil)
	}
	return found
}

// member is a session of a user the role stored as stored is granted to.
func (a oracleRoleFoldAccount) member(c *qt.C, stored string) *sql.DB {
	c.Helper()
	user := strings.Replace(a.name, "PTAH_RF_A_", "PTAH_RF_M_", 1)
	execOracle(c.Context(), c, a.admin, "CREATE USER "+user+" IDENTIFIED BY "+user)
	execOracle(c.Context(), c, a.admin, `GRANT CREATE SESSION, "`+stored+`" TO `+user)
	return openRevokeAllMember(c, "oracle", a.driverURL, url.UserPassword(user, user))
}
