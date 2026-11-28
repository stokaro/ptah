//go:build integration

package integration_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// ptah seed keeps its ledger, schema_seeds, where the migration history is
// kept: in the schema the URL names, or else in dbo. Left to the server, the
// ledger lands in the default schema of the first account that seeds, and an
// account with another default schema creates a second ledger and applies
// every seed again.

// sqlServerSeedLedgers lists every schema_seeds table the database holds, as
// schema.name in byte order.
const sqlServerSeedLedgers = `SELECT ISNULL(STRING_AGG(CONVERT(nvarchar(max), SCHEMA_NAME(schema_id) + N'.' + name), N',')
	WITHIN GROUP (ORDER BY SCHEMA_NAME(schema_id) COLLATE Latin1_General_BIN), N'')
	FROM sys.tables WHERE name = N'schema_seeds'`

// sqlServerSeededRows counts the rows the seed of sqlServerSeedFiles inserted.
const sqlServerSeededRows = `SELECT CONVERT(nvarchar(20), COUNT(*)) FROM dbo.seeded`

// sqlServerSeedFiles is one seed file that inserts one row, so a run that
// applies it again leaves an extra row.
var sqlServerSeedFiles = map[string]string{
	"001_one.all.sql": "INSERT INTO dbo.seeded VALUES (1);\n",
}

// sqlServerSeedTarget creates the table the seed fills in a scratch database,
// and returns the database's URL as sa and as an account whose default schema
// is app, with the seeds directory.
func sqlServerSeedTarget(c *qt.C) (dbURL, account, dir string) {
	c.Helper()
	dbURL = sqlServerRoleScratchDatabase(c)
	account = sqlServerAccount(c, dbURL, "app")
	_, err := connectDevDialect(c, dbURL).ExecContext(c.Context(), "CREATE TABLE dbo.seeded (id INT NOT NULL)")
	c.Assert(err, qt.IsNil)
	dir = c.TempDir()
	writeFiles(c, dir, sqlServerSeedFiles)
	return dbURL, account, dir
}

// TestSeedSQLServerKeepsOneLedgerForEveryAccountE2E seeds one database as sa
// and as an account whose default schema is app, in both orders. The second
// run finds the first run's record in dbo, and the seed is applied once. The
// account-first order is the one an unqualified ledger fails: the account
// creates it in app, and sa, which resolves the name in dbo, creates another.
func TestSeedSQLServerKeepsOneLedgerForEveryAccountE2E(t *testing.T) {
	tests := []struct {
		name  string
		order []string
	}{
		{name: "sa first", order: []string{"sa", "account"}},
		{name: "the account first", order: []string{"account", "sa"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbURL, account, dir := sqlServerSeedTarget(c)
			urls := map[string]string{"sa": dbURL, "account": account}

			for _, who := range test.order {
				got := clirun.Run(c, clirun.Ptah, clirun.Options{Timeout: time.Minute},
					"seed", "--db-url", urls[who], "--seeds-dir", dir, "--env", "dev")
				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("%s\nstdout:\n%s\nstderr:\n%s", who, got.Stdout, got.Stderr))
			}

			c.Assert(heldColumns(c, dbURL, sqlServerSeededRows), qt.Equals, "1")
			c.Assert(heldColumns(c, dbURL, sqlServerSeedLedgers), qt.Equals, "dbo.schema_seeds")
		})
	}
}

// TestSeedSQLServerRefusesALedgerInTheAccountsSchemaE2E seeds as an account
// whose default schema is app through a URL naming schema=app, which keeps the
// ledger in app, and seeds again without the parameter. That run would read
// the empty dbo and apply the seed again, so it is refused; the run that names
// app again finds its record.
func TestSeedSQLServerRefusesALedgerInTheAccountsSchemaE2E(t *testing.T) {
	c := qt.New(t)
	dbURL, account, dir := sqlServerSeedTarget(c)
	seed := func(target string) clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{Timeout: time.Minute},
			"seed", "--db-url", target, "--seeds-dir", dir, "--env", "dev")
	}

	kept := seed(account + "&schema=app")
	unnamed := seed(account)
	named := seed(account + "&schema=app")

	c.Assert(kept.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", kept.Stdout, kept.Stderr))
	c.Assert(unnamed.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", unnamed.Stdout, unnamed.Stderr))
	c.Assert(unnamed.Stderr, qt.Contains, `found seed ledger "schema_seeds" in schema "app", the default schema of the `+
		`connection's user, and none in schema "dbo", where this run keeps and reads it: add schema=app to the URL to `+
		`keep using the ledger where it is; move the table into schema "dbo"; or add schema=dbo to the URL to start a `+
		`new ledger in schema "dbo"`)
	c.Assert(named.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", named.Stdout, named.Stderr))
	c.Assert(named.Stdout, qt.Contains, "Database seed data is already up to date.")
	c.Assert(heldColumns(c, dbURL, sqlServerSeededRows), qt.Equals, "1")
	c.Assert(heldColumns(c, dbURL, sqlServerSeedLedgers), qt.Equals, "app.schema_seeds")
}

// TestSeedSQLServerStartsALedgerBesideAnotherToolsTableE2E seeds, as an
// account whose default schema is app, beside a table in app named like the
// ledger that holds none of its columns. It records no seed, so the run starts
// the ledger in dbo and applies the seed once.
func TestSeedSQLServerStartsALedgerBesideAnotherToolsTableE2E(t *testing.T) {
	c := qt.New(t)
	dbURL, account, dir := sqlServerSeedTarget(c)
	_, err := connectDevDialect(c, dbURL).ExecContext(c.Context(), "CREATE TABLE app.schema_seeds (id INT NOT NULL)")
	c.Assert(err, qt.IsNil)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Timeout: time.Minute},
		"seed", "--db-url", account, "--seeds-dir", dir, "--env", "dev")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(heldColumns(c, dbURL, sqlServerSeededRows), qt.Equals, "1")
	c.Assert(heldColumns(c, dbURL, sqlServerSeedLedgers), qt.Equals, "app.schema_seeds,dbo.schema_seeds")
}

// TestSeedSQLServerReadsTheLedgerInDboBesideOneInAppE2E keeps a ledger in app,
// through schema=app, and one in dbo, as sa: the second run applies the seed
// again, so dbo.seeded holds two rows. The account then seeds without a
// schema. dbo holds a ledger, so the run reads it and skips the seed.
func TestSeedSQLServerReadsTheLedgerInDboBesideOneInAppE2E(t *testing.T) {
	c := qt.New(t)
	dbURL, account, dir := sqlServerSeedTarget(c)
	seed := func(target string) clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{Timeout: time.Minute},
			"seed", "--db-url", target, "--seeds-dir", dir, "--env", "dev")
	}

	inApp := seed(account + "&schema=app")
	inDbo := seed(dbURL)
	unnamed := seed(account)

	c.Assert(inApp.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", inApp.Stdout, inApp.Stderr))
	c.Assert(inDbo.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", inDbo.Stdout, inDbo.Stderr))
	c.Assert(unnamed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", unnamed.Stdout, unnamed.Stderr))
	c.Assert(unnamed.Stdout, qt.Contains, "Database seed data is already up to date.")
	c.Assert(heldColumns(c, dbURL, sqlServerSeededRows), qt.Equals, "2")
}

// TestSeedSQLServerNamesTheSchemaForAUserOutsideDboE2E seeds as an account
// that may create tables in app alone. The ledger belongs in dbo, so the run
// that names no schema fails to create it and says how to keep it in app; the
// run that names app seeds.
func TestSeedSQLServerNamesTheSchemaForAUserOutsideDboE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := sqlServerRoleScratchDatabase(c)
	account := sqlServerLimitedAccount(c, dbURL, "app")
	_, err := connectDevDialect(c, dbURL).ExecContext(c.Context(), "CREATE TABLE app.limited (id INT NOT NULL)")
	c.Assert(err, qt.IsNil)
	dir := c.TempDir()
	writeFiles(c, dir, map[string]string{"001_one.all.sql": "INSERT INTO app.limited VALUES (1);\n"})
	seed := func(target string) clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{Timeout: time.Minute},
			"seed", "--db-url", target, "--seeds-dir", dir, "--env", "dev")
	}

	unnamed := seed(account)
	named := seed(account + "&schema=app")

	c.Assert(unnamed.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", unnamed.Stdout, unnamed.Stderr))
	c.Assert(unnamed.Stderr, qt.Contains, `create schema_seeds table in schema "dbo", where Ptah keeps its tables when `+
		`the URL names no schema; for a user that cannot create tables there, add schema=app to the URL: `)
	c.Assert(named.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", named.Stdout, named.Stderr))
	c.Assert(heldColumns(c, dbURL, `SELECT CONVERT(nvarchar(20), COUNT(*)) FROM app.limited`), qt.Equals, "1")
	c.Assert(heldColumns(c, dbURL, sqlServerSeedLedgers), qt.Equals, "app.schema_seeds")
}
