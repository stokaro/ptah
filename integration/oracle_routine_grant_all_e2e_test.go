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

// A declared GRANT ALL on an Oracle function or procedure is held once the
// server granted it, for a role the schema declares: two applies leave the
// role what ALL gives on the routine, the next plan is empty, a member calls
// it, and `ptah db read` prints the grant.
func TestSchemaApplyOracleGrantAllOnARoutineE2E(t *testing.T) {
	tests := []struct {
		name      string
		directive string
		call      string
	}{
		{
			name:      "a function",
			directive: `//ptah:schema:function name="rt" returns="NUMBER" language="plsql" body="BEGIN RETURN 1; END;"`,
			call:      "SELECT {account}.rt() FROM dual",
		},
		{
			name:      "a procedure",
			directive: `//ptah:schema:procedure name="rt" language="plsql" body="BEGIN NULL; END;"`,
			call:      "BEGIN {account}.rt; END;",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			account := newOracleRoutineAccount(c)
			apply := account.applier(c, test.directive, "CREATE ROLE {role};\nGRANT ALL ON rt TO {role};\n")

			for range 2 {
				applied := apply("--auto-approve")
				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			}
			settled := apply("--dry-run")

			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			c.Assert(oracleRoutinePrivilegesOf(c, account.owner, account.role, account.name), qt.DeepEquals, []string{"DEBUG", "EXECUTE"})
			_, err := account.member.ExecContext(c.Context(), strings.ReplaceAll(test.call, "{account}", account.name))
			c.Assert(err, qt.IsNil)
			read := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: 3 * time.Minute}, "db", "read", "--db-url", account.url)
			c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", read.Stderr))
			c.Assert(read.Stdout, qt.Contains, "GRANT EXECUTE ON "+account.name+".RT TO "+account.role+";\n")
		})
	}
}

// A declared GRANT ALL on an Oracle function returning a nested-table
// collection compares with the function's list, though the catalog reports the
// function's return type as table: the role holds what ALL gives on a function
// after every apply, and a member reads the function's rows. Every plan
// replaces the function, because the read spells its return type table where
// the declaration spells RV_NUMS, so every apply drops it, creates it and
// grants back what the role held.
func TestSchemaApplyOracleGrantAllOnACollectionFunctionE2E(t *testing.T) {
	c := qt.New(t)
	account := newOracleRoutineAccount(c)
	_, err := account.owner.ExecContext(c.Context(), "CREATE TYPE rv_nums AS TABLE OF NUMBER")
	c.Assert(err, qt.IsNil)
	apply := account.applier(c,
		`//ptah:schema:function name="rt" returns="RV_NUMS" language="plsql" body="BEGIN RETURN RV_NUMS(1); END;"`,
		"CREATE ROLE {role};\nGRANT ALL ON rt TO {role};\n")

	for range 3 {
		applied := apply("--auto-approve")

		c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
		c.Assert(applied.Stdout, qt.Contains, "CREATE OR REPLACE FUNCTION")
		c.Assert(applied.Stdout, qt.Not(qt.Contains), "REVOKE")
		c.Assert(oracleRoutinePrivilegesOf(c, account.owner, account.role, account.name), qt.DeepEquals, []string{"DEBUG", "EXECUTE"})
	}
	var values int
	c.Assert(account.member.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM TABLE("+account.name+".rt())").Scan(&values), qt.IsNil)
	c.Assert(values, qt.Equals, 1)
}

// A body change to an Oracle routine with a declared grant drops the routine,
// creates it again and grants back every privilege a grantee held on it: the
// declared role holds what it held before, and so does the member, a user the
// schema does not manage. The next plan is empty, and the member calls the
// new body.
func TestSchemaApplyOracleKeepsTheGrantsOnAReplacedRoutineE2E(t *testing.T) {
	tests := []struct {
		name   string
		before string
		after  string
		grant  string
		want   []string
		call   string
	}{
		{
			name:   "ALL on a function",
			before: `//ptah:schema:function name="rt" returns="NUMBER" language="plsql" body="BEGIN RETURN 1; END;"`,
			after:  `//ptah:schema:function name="rt" returns="NUMBER" language="plsql" body="BEGIN RETURN 2; END;"`,
			grant:  "GRANT ALL ON rt TO {role};\n",
			want:   []string{"DEBUG", "EXECUTE"},
			call:   "SELECT {account}.rt() FROM dual",
		},
		{
			name:   "EXECUTE on a procedure",
			before: `//ptah:schema:procedure name="rt" language="plsql" body="BEGIN NULL; END;"`,
			after:  `//ptah:schema:procedure name="rt" language="plsql" body="BEGIN RETURN; END;"`,
			grant:  "GRANT EXECUTE ON rt TO {role};\n",
			want:   []string{"EXECUTE"},
			call:   "BEGIN {account}.rt; END;",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			account := newOracleRoutineAccount(c)
			statements := "CREATE ROLE {role};\n" + test.grant
			created := account.applier(c, test.before, statements)("--auto-approve")
			c.Assert(created.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", created.Stdout, created.Stderr))
			_, err := account.owner.ExecContext(c.Context(), "GRANT EXECUTE ON rt TO "+account.user)
			c.Assert(err, qt.IsNil)
			apply := account.applier(c, test.after, statements)

			replaced := apply("--auto-approve")
			settled := apply("--dry-run")

			c.Assert(replaced.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", replaced.Stdout, replaced.Stderr))
			c.Assert(replaced.Stdout, qt.Contains, "CREATE OR REPLACE ")
			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			c.Assert(oracleRoutinePrivilegesOf(c, account.owner, account.role, account.name), qt.DeepEquals, test.want)
			c.Assert(oracleRoutinePrivilegesOf(c, account.owner, account.user, account.name), qt.DeepEquals, []string{"EXECUTE"})
			_, err = account.member.ExecContext(c.Context(), strings.ReplaceAll(test.call, "{account}", account.name))
			c.Assert(err, qt.IsNil)
		})
	}
}

// A body change to an Oracle routine on which a user holds a privilege WITH
// GRANT OPTION is refused before any statement runs: the DROP would take the
// option, and Ptah writes no grant option on Oracle. The routine keeps its body
// and the user keeps the privilege.
func TestSchemaApplyOracleRefusesToReplaceARoutineAGrantOptionIsHeldOnE2E(t *testing.T) {
	c := qt.New(t)
	account := newOracleRoutineAccount(c)
	statements := "CREATE ROLE {role};\nGRANT EXECUTE ON rt TO {role};\n"
	created := account.applier(c, `//ptah:schema:procedure name="rt" language="plsql" body="BEGIN NULL; END;"`, statements)("--auto-approve")
	c.Assert(created.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", created.Stdout, created.Stderr))
	_, err := account.owner.ExecContext(c.Context(), "GRANT EXECUTE ON rt TO "+account.user+" WITH GRANT OPTION")
	c.Assert(err, qt.IsNil)

	refused := account.applier(c, `//ptah:schema:procedure name="rt" language="plsql" body="BEGIN RETURN; END;"`, statements)("--auto-approve")

	c.Assert(refused.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
	c.Assert(refused.Stderr, qt.Contains, "oracle: cannot replace procedure rt: grantee \""+account.user+"\" holds EXECUTE on it "+
		"WITH GRANT OPTION, and the DROP that replaces the routine takes the option, which Ptah cannot grant back; "+
		"revoke the option, or change the routine by hand with CREATE OR REPLACE, which keeps it")
	var body string
	c.Assert(account.owner.QueryRowContext(c.Context(),
		"SELECT LISTAGG(text) WITHIN GROUP (ORDER BY line) FROM user_source WHERE name = 'RT'").Scan(&body), qt.IsNil)
	c.Assert(body, qt.Contains, "BEGIN NULL; END;")
	c.Assert(oracleRoutinePrivilegesOf(c, account.owner, account.user, account.name), qt.DeepEquals, []string{"EXECUTE"})
}

// A plan that replaces an Oracle procedure for a new body and stops declaring
// one of the role's privileges on it writes no REVOKE, since the DROP takes
// the privilege, and gives back only the privileges the schema keeps. The role
// holds EXECUTE and not DEBUG, and the next plan is empty.
func TestSchemaApplyOracleKeepsARevokeOnAReplacedRoutineE2E(t *testing.T) {
	c := qt.New(t)
	account := newOracleRoutineAccount(c)
	created := account.applier(c, `//ptah:schema:procedure name="rt" language="plsql" body="BEGIN NULL; END;"`,
		"CREATE ROLE {role};\nGRANT EXECUTE ON rt TO {role};\nGRANT DEBUG ON rt TO {role};\n")("--auto-approve")
	c.Assert(created.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", created.Stdout, created.Stderr))
	c.Assert(oracleRoutinePrivilegesOf(c, account.owner, account.role, account.name), qt.DeepEquals, []string{"DEBUG", "EXECUTE"})
	apply := account.applier(c, `//ptah:schema:procedure name="rt" language="plsql" body="BEGIN RETURN; END;"`,
		"CREATE ROLE {role};\nGRANT EXECUTE ON rt TO {role};\n")

	replaced := apply("--auto-approve")
	settled := apply("--dry-run")

	c.Assert(replaced.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", replaced.Stdout, replaced.Stderr))
	c.Assert(replaced.Stdout, qt.Not(qt.Contains), "REVOKE")
	c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
	c.Assert(settled.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
	c.Assert(oracleRoutinePrivilegesOf(c, account.owner, account.role, account.name), qt.DeepEquals, []string{"EXECUTE"})
	var body string
	c.Assert(account.owner.QueryRowContext(c.Context(),
		"SELECT LISTAGG(text) WITHIN GROUP (ORDER BY line) FROM user_source WHERE name = 'RT'").Scan(&body), qt.IsNil)
	c.Assert(body, qt.Contains, "BEGIN RETURN; END;")
}

// A schema that stops declaring an Oracle table or procedure, and the grant on
// it, drops the object and plans no REVOKE: the DROP takes the role's
// privileges, and a REVOKE run after it answers ORA-00942 once the DROP has
// committed. The apply succeeds, the object and the privileges are gone, and
// the next plan is empty.
func TestSchemaApplyOracleDropsAGrantedObjectE2E(t *testing.T) {
	tests := []struct {
		name       string
		directive  string
		statements string
	}{
		{
			name:       "a table",
			statements: "CREATE TABLE rt (id NUMBER PRIMARY KEY);\nCREATE ROLE {role};\nGRANT SELECT ON rt TO {role};\n",
		},
		{
			name:       "a procedure",
			directive:  `//ptah:schema:procedure name="rt" language="plsql" body="BEGIN NULL; END;"`,
			statements: "CREATE ROLE {role};\nGRANT ALL ON rt TO {role};\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			account := newOracleRoutineAccount(c)
			created := account.applier(c, test.directive, test.statements)("--auto-approve")
			c.Assert(created.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", created.Stdout, created.Stderr))
			c.Assert(oracleRoutinePrivilegesOf(c, account.owner, account.role, account.name), qt.Not(qt.HasLen), 0)
			apply := account.applier(c, "", "CREATE ROLE {role};\n")

			dropped := apply("--auto-approve")
			settled := apply("--dry-run")

			c.Assert(dropped.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", dropped.Stdout, dropped.Stderr))
			c.Assert(dropped.Stdout, qt.Not(qt.Contains), "REVOKE")
			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			c.Assert(oracleObjectsNamedRT(c, account.owner), qt.Equals, 0)
			c.Assert(oracleRoutinePrivilegesOf(c, account.owner, account.role, account.name), qt.DeepEquals, make([]string, 0))
		})
	}
}

// A role the schema declares loses EXECUTE on an Oracle procedure the schema
// grants it nothing on: the apply revokes it, the next plan is empty, and a
// member's call is refused.
func TestSchemaApplyOracleRevokesAnUndeclaredRoutineGrantE2E(t *testing.T) {
	c := qt.New(t)
	account := newOracleRoutineAccount(c)
	apply := account.applier(c, `//ptah:schema:procedure name="rt" language="plsql" body="BEGIN NULL; END;"`, "CREATE ROLE {role};\n")
	created := apply("--auto-approve")
	c.Assert(created.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", created.Stdout, created.Stderr))
	_, err := account.owner.ExecContext(c.Context(), "GRANT EXECUTE ON rt TO "+account.role)
	c.Assert(err, qt.IsNil)

	applied := apply("--auto-approve")
	settled := apply("--dry-run")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stdout, qt.Contains, "REVOKE EXECUTE ON "+account.name+".RT FROM "+account.role+";\n")
	c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
	c.Assert(settled.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
	c.Assert(oracleRoutinePrivilegesOf(c, account.owner, account.role, account.name), qt.DeepEquals, make([]string, 0))
	_, err = account.member.ExecContext(c.Context(), "BEGIN "+account.name+".rt; END;")
	c.Assert(err, qt.ErrorMatches, `(?s).*PLS-00201.*`)
}

// oracleRoutineAccount is an Oracle account that may create routines, a role
// the account's schema declares, and a user holding the role.
type oracleRoutineAccount struct {
	// url is the address ptah connects with, as the account.
	url string
	// name is the account, which owns the schema.
	name string
	role string
	// user is the user holding the role.
	user string
	// owner is a session of the account, and member one of the user.
	owner  *sql.DB
	member *sql.DB
}

// newOracleRoutineAccount creates the account, the role and the user as the
// admin, and drops them when the test ends. The role is created here because
// the account may not create roles; the schema declares it all the same.
func newOracleRoutineAccount(c *qt.C) oracleRoutineAccount {
	c.Helper()
	ctx := c.Context()
	adminURL := dbtarget.URL(c, dbtarget.OracleAdmin)
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	stamp := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000_000)
	name, role, user := "PTAH_RG_"+stamp, "PTAH_RG_ROLE_"+stamp, "PTAH_RG_M_"+stamp
	c.Cleanup(func() {
		dropOracleUser(context.Background(), c, admin, user)
		dropOracleUser(context.Background(), c, admin, name)
		dropOracleRole(context.Background(), c, admin, role)
	})
	createOracleUser(ctx, c, admin, name)
	execOracle(ctx, c, admin, "GRANT SELECT_CATALOG_ROLE, CREATE PROCEDURE, CREATE TYPE TO "+name)
	execOracle(ctx, c, admin, "CREATE ROLE "+role)
	execOracle(ctx, c, admin, "CREATE USER "+user+" IDENTIFIED BY "+user)
	execOracle(ctx, c, admin, "GRANT CREATE SESSION, "+role+" TO "+user)
	accountURL := oracleURLAs(c, adminURL, name)
	driverURL := oracleRevokeAllDriverURL(c, accountURL)
	owner, err := sql.Open("oracle", driverURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(owner.Close(), qt.IsNil) })
	return oracleRoutineAccount{
		url: accountURL, name: name, role: role, user: user, owner: owner,
		member: openRevokeAllMember(c, "oracle", driverURL, url.UserPassword(user, user)),
	}
}

// applier writes a Go source holding directive and a SQL file holding
// statements, where {role} stands for the role, and returns a `ptah schema
// apply` of both with the given arguments.
func (a oracleRoutineAccount) applier(c *qt.C, directive, statements string) func(args ...string) clirun.Result {
	c.Helper()
	routines := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(routines, "routine.go"), []byte("package entities\n\n"+directive+"\ntype R struct{}\n"), 0o600), qt.IsNil)
	grants := filepath.Join(c.TempDir(), "grants.sql")
	c.Assert(os.WriteFile(grants, []byte(strings.ReplaceAll(statements, "{role}", a.role)), 0o600), qt.IsNil)
	return func(args ...string) clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: 3 * time.Minute},
			append([]string{"schema", "apply", "--db-url", a.url, "--root-dir", routines, "--schema-file", grants}, args...)...)
	}
}

// oracleObjectsNamedRT is how many objects the session's own schema holds
// under the name RT.
func oracleObjectsNamedRT(c *qt.C, db *sql.DB) int {
	c.Helper()
	var count int
	c.Assert(db.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM user_objects WHERE object_name = 'RT'").Scan(&count), qt.IsNil)
	return count
}

// oracleRoutinePrivilegesOf is what the catalog reports grantee holds on
// account's routine RT, one privilege per row, in order.
func oracleRoutinePrivilegesOf(c *qt.C, db *sql.DB, grantee, account string) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(),
		"SELECT privilege FROM all_tab_privs WHERE grantee = :1 AND table_schema = :2 AND table_name = 'RT' ORDER BY privilege", grantee, account)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	held := make([]string, 0)
	for rows.Next() {
		var privilege string
		c.Assert(rows.Scan(&privilege), qt.IsNil)
		held = append(held, privilege)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return held
}
