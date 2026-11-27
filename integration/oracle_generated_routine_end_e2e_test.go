//go:build integration

package integration_test

import (
	"context"
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

// routinesHCL declares, in the schema account, the function RT and the
// procedure RP, as `ptah schema inspect` writes them.
func routinesHCL(account string) string {
	return `schema "` + account + `" {
}
function "RT" {
  schema = schema.` + account + `
  lang = "plsql"
  return = "number"
  params = "x in number"
  security = "DEFINER"
  volatility = "VOLATILE"
  as = "BEGIN\n  RETURN x * 2;\nEND;"
}
procedure "RP" {
  schema = schema.` + account + `
  lang = "plsql"
  security = "DEFINER"
  volatility = "VOLATILE"
  as = "BEGIN\n  NULL;\nEND;"
}
`
}

// generatedStatements is the body of the file `migrations generate` wrote for
// the migration named name in direction, up or down, without the comment
// lines its header is.
func generatedStatements(c *qt.C, dir, name, direction string) string {
	c.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*_"+name+"."+direction+".sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(paths, qt.HasLen, 1, qt.Commentf("the %s files in %s named %s", direction, dir, name))
	body, err := os.ReadFile(paths[0])
	c.Assert(err, qt.IsNil)
	var kept []string
	for line := range strings.SplitSeq(string(body), "\n") {
		if !strings.HasPrefix(line, "--") {
			kept = append(kept, line)
		}
	}
	return strings.TrimSpace(strings.Join(kept, "\n"))
}

// accountRoutineStatuses answers the functions and procedures an account
// owns, as type, name and status.
func accountRoutineStatuses(c *qt.C, ctx context.Context, admin *dbschema.DatabaseConnection, account string) []string {
	c.Helper()
	return accountNames(c, ctx, admin,
		"SELECT object_type || ' ' || object_name || ' ' || status FROM all_objects "+
			"WHERE owner = '"+account+"' AND object_type IN ('FUNCTION', 'PROCEDURE') ORDER BY object_type, object_name")
}

// TestMigrationsGenerateEndsAnOracleRoutineOnceE2E generates the migration
// that creates a function and a procedure, and the one that removes them,
// whose rollback creates them again from the read. Each body ends with one
// `END;`: a second semicolon after a PL/SQL block is not part of the block.
// Each file applies, and the account holds both routines VALID where a file
// creates them (stokaro/ptah#6755).
func TestMigrationsGenerateEndsAnOracleRoutineOnceE2E(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.OracleAdmin)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	account, accountURL := oracleScratchAccount(c, ctx, admin, "gen_end")
	dir := c.TempDir()
	declared := filepath.Join(c.TempDir(), "routines.hcl")
	c.Assert(os.WriteFile(declared, []byte(routinesHCL(account)), 0o600), qt.IsNil)
	empty := filepath.Join(c.TempDir(), "empty.hcl")
	c.Assert(os.WriteFile(empty, []byte(`schema "`+account+`" {
}
`), 0o600), qt.IsNil)
	options := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute, Env: environmentWithoutPtahVariables()}
	both := []string{"FUNCTION RT VALID", "PROCEDURE RP VALID"}

	created := clirun.Run(c, clirun.Ptah, options, "migrations", "generate", "--db-url", accountURL,
		"--migrations-dir", dir, "--schema-file", declared, "--name", "create_routines")
	c.Assert(created.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", created.Stdout, created.Stderr))
	c.Assert(generatedStatements(c, dir, "create_routines", "up"), qt.Equals,
		"CREATE OR REPLACE PROCEDURE "+account+".RP AUTHID DEFINER IS\n"+
			"BEGIN\n  NULL;\nEND;\n"+
			"CREATE OR REPLACE FUNCTION "+account+".RT(x in number) RETURN number AUTHID DEFINER IS\n"+
			"BEGIN\n  RETURN x * 2;\nEND;")
	up := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--db-url", accountURL, "--migrations-dir", dir)
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(accountRoutineStatuses(c, ctx, admin, account), qt.DeepEquals, both)

	removed := clirun.Run(c, clirun.Ptah, options, "migrations", "generate", "--db-url", accountURL,
		"--migrations-dir", dir, "--schema-file", empty, "--name", "drop_routines")
	c.Assert(removed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", removed.Stdout, removed.Stderr))
	c.Assert(generatedStatements(c, dir, "drop_routines", "down"), qt.Equals,
		"CREATE OR REPLACE FUNCTION RT(x in number) RETURN number AUTHID DEFINER IS\n"+
			"BEGIN\n  RETURN x * 2;\nEND;\n"+
			"CREATE OR REPLACE PROCEDURE RP AUTHID DEFINER IS\n"+
			"BEGIN\n  NULL;\nEND;")
	up = clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--db-url", accountURL, "--migrations-dir", dir,
		"--allow-destructive")
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(accountRoutineStatuses(c, ctx, admin, account), qt.HasLen, 0)

	down := clirun.Run(c, clirun.Ptah, options, "migrations", "down", "--db-url", accountURL, "--migrations-dir", dir,
		"--limit", "1", "--confirm")
	c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
	c.Assert(accountRoutineStatuses(c, ctx, admin, account), qt.DeepEquals, both)
}
