//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// oracleGrantOptionTable declares the table the tests grant on.
const oracleGrantOptionTable = "CREATE TABLE t48 (id NUMBER PRIMARY KEY);\n"

// applyOracleSchemaFile runs `ptah schema apply` as the account with a SQL
// schema file holding statements, where {user} stands for the account's user,
// and the given arguments.
func applyOracleSchemaFile(c *qt.C, account oracleRoutineAccount, statements string, args ...string) clirun.Result {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(strings.ReplaceAll(statements, "{user}", account.user)), 0o600), qt.IsNil)
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: 3 * time.Minute},
		append([]string{"schema", "apply", "--db-url", account.url, "--schema-file", path}, args...)...)
}

// oracleTablePrivilegesOf is what the catalog reports grantee holds on
// account's table T48, one `privilege grantable` per row, in order.
func oracleTablePrivilegesOf(c *qt.C, db *sql.DB, grantee, account string) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(),
		"SELECT privilege || ' ' || grantable FROM all_tab_privs WHERE grantee = :1 AND table_schema = :2 "+
			"AND table_name = 'T48' ORDER BY privilege", grantee, account)
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

// grantedT48 creates the table T48 through an apply and runs grant as the
// owner, where {user} stands for the account's user.
func grantedT48(c *qt.C, account oracleRoutineAccount, grant string) {
	c.Helper()
	created := applyOracleSchemaFile(c, account, oracleGrantOptionTable, "--auto-approve")
	c.Assert(created.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", created.Stdout, created.Stderr))
	_, err := account.owner.ExecContext(c.Context(), strings.ReplaceAll(grant, "{user}", account.user))
	c.Assert(err, qt.IsNil)
}

// Oracle has no statement that takes the grant option and leaves the
// privilege. A user holds SELECT WITH GRANT OPTION, and a schema that revokes
// ALL from the user and grants SELECT says the user keeps SELECT without the
// option: the plan is refused before any statement runs, where the plain
// REVOKE it would render takes SELECT from the user and from everyone the user
// granted it to. A declared REVOKE GRANT OPTION FOR is refused by name. The
// user holds SELECT with the option after both.
func TestSchemaApplyOracleRefusesToTakeOnlyAGrantOptionE2E(t *testing.T) {
	c := qt.New(t)
	account := newOracleRoutineAccount(c)
	grantedT48(c, account, "GRANT SELECT ON t48 TO {user} WITH GRANT OPTION")

	planned := applyOracleSchemaFile(c, account,
		oracleGrantOptionTable+"REVOKE ALL ON t48 FROM {user};\nGRANT SELECT ON t48 TO {user};\n", "--auto-approve")
	declared := applyOracleSchemaFile(c, account,
		oracleGrantOptionTable+"GRANT SELECT ON t48 TO {user} WITH GRANT OPTION;\nREVOKE GRANT OPTION FOR SELECT ON t48 FROM {user};\n",
		"--auto-approve")

	c.Assert(planned.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
	c.Assert(planned.Stderr, qt.Contains, "oracle: cannot take the grant option on SELECT ON T48 from \""+
		account.user+"\" and leave the privilege: Oracle has no statement for it")
	c.Assert(declared.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", declared.Stdout, declared.Stderr))
	c.Assert(declared.Stderr, qt.Contains, "REVOKE GRANT OPTION FOR SELECT ON TABLE t48 FROM "+account.user+
		": oracle has no statement that takes the grant option and leaves the privilege")
	c.Assert(oracleTablePrivilegesOf(c, account.owner, account.user, account.name), qt.DeepEquals, []string{"SELECT YES"})
}

// The control for the refusal: a user holding SELECT without the option is what
// the same schema declares, so the apply succeeds, the user keeps SELECT, and
// the next plan is empty.
func TestSchemaApplyOracleKeepsAPrivilegeHeldWithoutTheGrantOptionE2E(t *testing.T) {
	c := qt.New(t)
	account := newOracleRoutineAccount(c)
	grantedT48(c, account, "GRANT SELECT ON t48 TO {user}")
	statements := oracleGrantOptionTable + "REVOKE ALL ON t48 FROM {user};\nGRANT SELECT ON t48 TO {user};\n"

	applied := applyOracleSchemaFile(c, account, statements, "--auto-approve")
	settled := applyOracleSchemaFile(c, account, statements, "--dry-run")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(oracleTablePrivilegesOf(c, account.owner, account.user, account.name), qt.DeepEquals, []string{"SELECT NO"})
	c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
	c.Assert(settled.Stdout, qt.Contains, "Schema is synced")
}

// On Oracle a REVOKE on a table takes the grantee's own grant and nothing a
// grant to PUBLIC gives it: measured on Oracle 23.26, a user holding SELECT
// on t48, with SELECT on t48 granted to PUBLIC as well, reads t48 after
// REVOKE SELECT ON t48 FROM the user. A schema that revokes it is refused
// before any statement runs, naming the grant to PUBLIC and the user as a
// principal, since the read describes roles and the user is none of them; the
// user still holds its grant, and the revoke run by hand leaves the user
// reading the table.
func TestSchemaApplyOracleRefusesARevokeAGrantToPublicKeepsE2E(t *testing.T) {
	c := qt.New(t)
	account := newOracleRoutineAccount(c)
	grantedT48(c, account, "GRANT SELECT ON t48 TO {user}")
	_, err := account.owner.ExecContext(c.Context(), "GRANT SELECT ON t48 TO PUBLIC")
	c.Assert(err, qt.IsNil)

	applied := applyOracleSchemaFile(c, account, oracleGrantOptionTable+"REVOKE SELECT ON t48 FROM {user};\n", "--auto-approve")

	c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stderr, qt.Contains, `PUBLIC holds SELECT ON t48, which gives principal "`+account.user+
		`" the privilege on table t48, so a revoke from "`+account.user+`" cannot take it: revoke SELECT ON t48 from PUBLIC instead`)
	c.Assert(oracleTablePrivilegesOf(c, account.owner, account.user, account.name), qt.DeepEquals, []string{"SELECT NO"})
	_, err = account.owner.ExecContext(c.Context(), strings.ReplaceAll("REVOKE SELECT ON t48 FROM {user}", "{user}", account.user))
	c.Assert(err, qt.IsNil)
	var rows int
	c.Assert(account.member.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM "+account.name+".T48").Scan(&rows), qt.IsNil)
	c.Assert(oracleTablePrivilegesOf(c, account.owner, account.user, account.name), qt.HasLen, 0)
}

// The control for the refusal above: a schema that revokes SELECT on t48 from
// PUBLIC too leaves PUBLIC's grant nothing to keep, so the apply revokes both
// and the user reads t48 no more. The schema names the table t48 and the read
// names it T48, so the revoke from PUBLIC is found only under the name Oracle
// stores.
func TestSchemaApplyOracleRevokesFromTheUserAndFromPublicE2E(t *testing.T) {
	c := qt.New(t)
	account := newOracleRoutineAccount(c)
	grantedT48(c, account, "GRANT SELECT ON t48 TO {user}")
	_, err := account.owner.ExecContext(c.Context(), "GRANT SELECT ON t48 TO PUBLIC")
	c.Assert(err, qt.IsNil)

	applied := applyOracleSchemaFile(c, account,
		oracleGrantOptionTable+"REVOKE SELECT ON t48 FROM {user};\nREVOKE SELECT ON t48 FROM PUBLIC;\n", "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(oracleTablePrivilegesOf(c, account.owner, account.user, account.name), qt.HasLen, 0)
	c.Assert(oracleTablePrivilegesOf(c, account.owner, "PUBLIC", account.name), qt.HasLen, 0)
	var rows int
	err = account.member.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM "+account.name+".T48").Scan(&rows)
	c.Assert(err, qt.ErrorMatches, `(?s).*ORA-00942.*`)
}
