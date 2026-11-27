//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// An ordinary Oracle owner may replace its procedure without reading
// DBA_ROLES. The generated up and down keep the routine's object grants,
// including a user's grant option and the privilege held through a role.
func TestMigrationsOracleBodyReplacementKeepsPrivilegesWithoutCatalogAccessE2E(t *testing.T) {
	for _, test := range []struct {
		name      string
		schema    string
		attribute string
		routine   string
	}{
		{name: "unqualified HCL", routine: "rt"},
		{
			name: "qualified HCL", schema: "schema \"{owner}\" {}\n",
			attribute: "  schema = schema.{owner}\n", routine: "{owner}.rt",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			account := newOracleRoutineAccountWithPrivileges(c, "CONNECT, RESOURCE, CREATE PROCEDURE")
			var count int
			err := account.owner.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM dba_roles").Scan(&count)
			c.Assert(err, qt.ErrorMatches, `(?s).*ORA-(00942|01031).*`)
			dir := c.TempDir()
			writeRoutineMigration(c, dir, "1700000000_init.up.sql",
				"CREATE OR REPLACE PROCEDURE rt AUTHID DEFINER IS\nBEGIN NULL; END;\n")
			writeRoutineMigration(c, dir, "1700000000_init.down.sql", "DROP PROCEDURE rt;\n")
			options := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute, Env: environmentWithoutPtahVariables()}
			initial := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--db-url", account.url, "--migrations-dir", dir)
			c.Assert(initial.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", initial.Stdout, initial.Stderr))
			for _, statement := range []string{
				"GRANT EXECUTE ON rt TO " + account.role,
				"GRANT EXECUTE ON rt TO " + account.user + " WITH GRANT OPTION",
			} {
				_, err := account.owner.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil)
			}
			wantedGrants := []string{account.role + " EXECUTE NO", account.user + " EXECUTE YES"}
			slices.Sort(wantedGrants)
			assertOracleRoutineReplacement(c, account, "BEGIN NULL; END;", wantedGrants)
			declared := filepath.Join(c.TempDir(), "routine.hcl")
			document := test.schema + "procedure \"rt\" {\n" + test.attribute + `
  lang = "plsql"
  security = "DEFINER"
  volatility = "VOLATILE"
  as = "BEGIN DBMS_OUTPUT.PUT_LINE(2); END;"
}
`
			c.Assert(os.WriteFile(declared, []byte(strings.ReplaceAll(document, "{owner}", account.name)), 0o600), qt.IsNil)

			generated := clirun.Run(c, clirun.Ptah, options, "migrations", "generate", "--db-url", account.url,
				"--migrations-dir", dir, "--schema-file", declared, "--name", "replace_body")

			c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
			header := "CREATE OR REPLACE PROCEDURE " + strings.ReplaceAll(test.routine, "{owner}", account.name) + " AUTHID DEFINER IS\n"
			c.Assert(generatedStatements(c, dir, "replace_body", "up"), qt.Equals, header+"BEGIN DBMS_OUTPUT.PUT_LINE(2); END;")
			c.Assert(generatedStatements(c, dir, "replace_body", "down"), qt.Equals, header+"BEGIN NULL; END;")
			up := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--db-url", account.url, "--migrations-dir", dir)
			c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
			assertOracleRoutineReplacement(c, account, "BEGIN DBMS_OUTPUT.PUT_LINE(2); END;", wantedGrants)
			down := clirun.Run(c, clirun.Ptah, options, "migrations", "down", "--db-url", account.url,
				"--migrations-dir", dir, "--limit", "1", "--confirm")
			c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
			assertOracleRoutineReplacement(c, account, "BEGIN NULL; END;", wantedGrants)
		})
	}
}

// assertOracleRoutineReplacement checks both the catalog and a grantee's real
// call. Reading only the migration text cannot prove that Oracle kept access.
func assertOracleRoutineReplacement(c *qt.C, account oracleRoutineAccount, wantedBody string, wantedGrants []string) {
	c.Helper()
	c.Assert(removedOracleRoutinePrivileges(c, account.owner), qt.DeepEquals, wantedGrants)
	var body string
	c.Assert(account.owner.QueryRowContext(c.Context(),
		"SELECT LISTAGG(text) WITHIN GROUP (ORDER BY line) FROM user_source WHERE name = 'RT'").Scan(&body), qt.IsNil)
	c.Assert(body, qt.Contains, wantedBody)
	_, err := account.member.ExecContext(c.Context(), "BEGIN "+account.name+".rt; END;")
	c.Assert(err, qt.IsNil)
}
