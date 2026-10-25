//go:build integration

package providercli_test

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// db verify runs each check through the provider's statement service: the
// provider proves the check reads and changes nothing before it runs it, and
// the check's verdict comes from what the provider's database holds.

// verifyReport is the part of db verify's JSON report a test reads.
type verifyReport struct {
	Verdict  string `json:"verdict"`
	Verified int    `json:"verified"`
	Failed   int    `json:"failed"`
	Errored  int    `json:"errored"`
	Results  []struct {
		Name   string `json:"name"`
		Status string `json:"status"`
		Error  string `json:"error"`
	} `json:"results"`
}

// verifyChecks applies the schema to a fresh fixture database, writes two
// rows into items through the test's own driver, and runs db verify with
// checks. The address it verifies refuses every statement but a read-only
// one, so a check that runs is one the host sent read-only.
func verifyChecks(c *qt.C, f devCLI, checks string) (verifyReport, string, int) {
	c.Helper()
	_, diagnostics, code := f.run(c, c.Context(), "apply", "--schema-file", f.schemaPath, "--auto-approve")
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	db, err := sql.Open("sqlite", f.databasePath)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(c.Context(), `INSERT INTO "items" ("id") VALUES (1), (2)`)
	c.Assert(err, qt.IsNil)
	c.Assert(db.Close(), qt.IsNil)
	path := filepath.Join(c.TempDir(), "checks.sql")
	c.Assert(os.WriteFile(path, []byte(checks), 0o600), qt.IsNil)

	out, diagnostics, code := executeProviderCLI(c.Context(), []string{"db", "verify", "--provider-config", f.manifestPath,
		"--db-url", f.address + "?fault=read-only-statements", "--checks", path, "--format", "json"})
	f.assertStopped(c)
	var report verifyReport
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil, qt.Commentf("%s\n%s", out, diagnostics))
	return report, diagnostics, code
}

func TestProviderVerifiesChecksThroughItsStatements(t *testing.T) {
	c := qt.New(t)
	report, diagnostics, code := verifyChecks(c, newDevCLI(c),
		"-- +ptah check name=two_items assert=\"SELECT count(*) = 2 FROM items\"\n"+
			"-- +ptah check name=no_values assert=\"SELECT count(*) = 0 FROM second\"\n")

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(report.Verdict, qt.Equals, "verified")
	c.Assert(report.Verified, qt.Equals, 2)
}

// A check that does not hold fails, one the database refuses errors with the
// server's words, and an engine that runs no statement refuses the check.
func TestProviderVerifiesChecksThroughItsStatements_FailurePath(t *testing.T) {
	t.Run("a check that does not hold", func(t *testing.T) {
		c := qt.New(t)
		report, diagnostics, code := verifyChecks(c, newDevCLI(c), "-- +ptah check name=three_items assert=\"SELECT count(*) = 3 FROM items\"\n")

		c.Assert(code, qt.Equals, 1, qt.Commentf("%s", diagnostics))
		c.Assert(report.Verdict, qt.Equals, "failed")
		c.Assert(report.Failed, qt.Equals, 1)
	})
	t.Run("a check the database refuses", func(t *testing.T) {
		c := qt.New(t)
		report, diagnostics, code := verifyChecks(c, newDevCLI(c), "-- +ptah check name=missing assert=\"SELECT count(*) = 0 FROM missing\"\n")

		c.Assert(code, qt.Equals, 2, qt.Commentf("%s", diagnostics))
		c.Assert(report.Errored, qt.Equals, 1)
		c.Assert(report.Results[0].Error, qt.Contains, "no such table: missing")
	})
	t.Run("an engine that runs no statement", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()
		spec := devdbSpec(c, dir)
		spec.Environment = append(spec.Environment, "FIXTURE_DEV_ENGINE=bare")
		f := devCLI{providerCLI: newProviderCLI(c, spec), dir: dir}
		path := filepath.Join(c.TempDir(), "checks.sql")
		c.Assert(os.WriteFile(path, []byte("-- +ptah check name=one assert=\"SELECT 1 = 1\"\n"), 0o600), qt.IsNil)

		_, diagnostics, code := executeProviderCLI(c.Context(), []string{"db", "verify", "--provider-config", f.manifestPath,
			"--db-url", f.address, "--checks", path})

		c.Assert(code, qt.Equals, 2)
		c.Assert(diagnostics, qt.Contains, "the experimental provider contract does not support statements the operator wrote")
	})
}

// statementCases is a migration test directory whose case migrates, writes a
// row with an exec step, and asserts on it with the provider's statements;
// assertion is the truth the case asserts.
func statementCases(c *qt.C, assertion string) string {
	c.Helper()
	dir := c.TempDir()
	cases := "test \"migrate\" \"records\" {\n" +
		"  migrate { to = \"0000000001\" }\n" +
		"  exec { sql = \"INSERT INTO records (id) VALUES (1)\" }\n" +
		"  assert { sql = \"" + assertion + "\" }\n" +
		"  catch {\n    sql   = \"SELECT * FROM missing\"\n    error = \"no such table\"\n  }\n" +
		"}\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "records.test.hcl"), []byte(cases), 0o600), qt.IsNil)
	return dir
}

func runStatementCases(c *qt.C, assertion string) (out, diagnostics string, code int) {
	c.Helper()
	f := newDevCLI(c)
	out, diagnostics, code = executeProviderCLI(c.Context(), []string{"migrations", "test", "--provider-config", f.manifestPath,
		"--dir", statementCases(c, assertion), "--migrations-dir", hashedMigrations(c), "--db-url", fixtureAddress("", filepath.Join(c.TempDir(), "scratch.db"))})
	f.assertStopped(c)
	return out, diagnostics, code
}

// A migration test's exec, assert and catch steps run on the provider's
// statements: the row the exec step wrote is the one the assertion counts,
// and the refusal the catch step expects is the server's.
func TestProviderRunsMigrationTestStepsThroughItsStatements(t *testing.T) {
	c := qt.New(t)
	out, diagnostics, code := runStatementCases(c, "SELECT count(*) = 1 FROM records")

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s\n%s", out, diagnostics))
	c.Assert(out, qt.Contains, "1 cases, 1 passed")
}

// An assertion that does not hold fails its case, after the exec step ran.
func TestProviderRunsMigrationTestStepsThroughItsStatements_FailurePath(t *testing.T) {
	c := qt.New(t)
	out, diagnostics, code := runStatementCases(c, "SELECT count(*) = 2 FROM records")

	c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("%s\n%s", out, diagnostics))
	c.Assert(out, qt.Contains, "exec ok")
	c.Assert(out, qt.Contains, `expected a true value, got "0"`)
	c.Assert(out, qt.Contains, "1 cases, 0 passed, 1 failed")
}

// An engine that runs no statement fails a step that expects an error: the
// refusal to run the step's SQL is not the error the author expected.
func TestProviderRunsMigrationTestStepsThroughItsStatements_NoStatements(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	spec := devdbSpec(c, dir)
	spec.Environment = append(spec.Environment, "FIXTURE_DEV_ENGINE=bare")
	f := devCLI{providerCLI: newProviderCLI(c, spec), dir: dir}
	cases := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(cases, "caught.test.hcl"), []byte("test \"migrate\" \"caught\" {\n  catch { sql = \"SELECT 1\" }\n}\n"), 0o600), qt.IsNil)

	out, diagnostics, code := executeProviderCLI(c.Context(), []string{"migrations", "test", "--provider-config", f.manifestPath,
		"--dir", cases, "--migrations-dir", hashedMigrations(c), "--db-url", f.address})

	c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("%s\n%s", out, diagnostics))
	c.Assert(out, qt.Contains, "the experimental provider contract does not support statements the operator wrote")
	c.Assert(out, qt.Contains, "1 cases, 0 passed, 1 failed")
}
