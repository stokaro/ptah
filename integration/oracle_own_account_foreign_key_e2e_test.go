//go:build integration

package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// ownAccountSpellings are the ways a declaration can name the account Ptah
// connects as, stored in upper case: Oracle takes each for that account.
var ownAccountSpellings = []struct {
	name  string
	spell func(account string) string
}{
	{name: "lower case", spell: strings.ToLower},
	{name: "upper case", spell: strings.ToUpper},
	{name: "quoted", spell: func(account string) string { return `"` + account + `"` }},
}

// ownAccountKey is a declaration of T2 whose key reads T1 of account, spelled
// as written.
func ownAccountKey(account string) string {
	return "CREATE TABLE T2 (ID NUMBER(10) PRIMARY KEY, T1_ID NUMBER(10), " +
		"CONSTRAINT T2_FK FOREIGN KEY (T1_ID) REFERENCES " + account + ".T1 (ID));\n"
}

// accountKeys answers the foreign keys account owns, each with the account and
// table it reads.
func accountKeys(c *qt.C, ctx context.Context, admin *dbschema.DatabaseConnection, account string) []string {
	c.Helper()
	return accountNames(c, ctx, admin,
		"SELECT k.constraint_name || ' ' || p.owner || '.' || p.table_name FROM all_constraints k "+
			"JOIN all_constraints p ON p.owner = k.r_owner AND p.constraint_name = k.r_constraint_name "+
			"WHERE k.owner = '"+account+"' AND k.constraint_type = 'R' ORDER BY k.constraint_name")
}

// ownedTables answers the tables account owns, leaving out the apply's own.
func ownedTables(c *qt.C, ctx context.Context, admin *dbschema.DatabaseConnection, account string) []string {
	c.Helper()
	return accountNames(c, ctx, admin,
		"SELECT table_name FROM all_tables WHERE owner = '"+account+"' AND "+ptahOwnTables+" ORDER BY table_name")
}

// TestSchemaApplyKeyIntoTheOwnAccountE2E declares T2 with a key that reads
// T1 of the account Ptah connects as, naming the account in each spelling
// Oracle takes for it. Declared without T1, the key reads a table the plan
// drops, so the apply is refused and T1 stays; planned, the drop fails with
// ORA-02449. Declared with T1, every spelling applies, the key reads the
// account's T1, and a second apply plans nothing.
func TestSchemaApplyKeyIntoTheOwnAccountE2E(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.OracleAdmin)
	for _, spelling := range ownAccountSpellings {
		t.Run(spelling.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
			account, accountURL := oracleScratchAccount(c, ctx, admin, "own_key")
			runAsAccount(c, ctx, admin, []string{"CREATE TABLE " + account + ".T1 (ID NUMBER(10) PRIMARY KEY)"})
			options := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute, Env: environmentWithoutPtahVariables()}
			undeclared := writeDeclaredSchema(c, ownAccountKey(spelling.spell(account)))
			declared := writeDeclaredSchema(c, "CREATE TABLE T1 (ID NUMBER(10) PRIMARY KEY);\n"+ownAccountKey(spelling.spell(account)))

			refused := clirun.Run(c, clirun.Ptah, options, "schema", "apply", "--db-url", accountURL,
				"--schema-file", undeclared, "--auto-approve")

			c.Assert(refused.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s", refused.Stdout))
			c.Assert(refused.Stderr, qt.Contains, `constraint "T2_FK" references unknown table`)
			c.Assert(refused.Stderr, qt.Contains, `"`+account+`" is the schema of the database compared`)
			c.Assert(ownedTables(c, ctx, admin, account), qt.DeepEquals, []string{"T1"})

			applied := clirun.Run(c, clirun.Ptah, options, "schema", "apply", "--db-url", accountURL,
				"--schema-file", declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(accountKeys(c, ctx, admin, account), qt.DeepEquals, []string{"T2_FK " + account + ".T1"})
			again := clirun.Run(c, clirun.Ptah, options, "schema", "apply", "--db-url", accountURL,
				"--schema-file", declared, "--dry-run")
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}
