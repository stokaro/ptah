//go:build integration

package atlasschema_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/rolepassword"
)

// These tests hold `ptah schema apply --dry-run` to what a real run of the same plan
// answers on a server. The dry run comes first, so the plan a real run refuses is
// still the plan it was asked about, and the database is read back after both.

// runApply runs `ptah schema apply --json` with no approval flag and no input, so a
// refusal that came after the prompt could not pass for one that came ahead of it.
func runApply(c *qt.C, f *failureTarget, args ...string) (clirun.Result, failureDocument) {
	c.Helper()
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		append([]string{"schema", "apply", "--db-url", f.url, "--json"}, args...)...)
	var document failureDocument
	c.Assert(json.Unmarshal([]byte(result.Stdout), &document), qt.IsNil, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
	return result, document
}

// assertSameAnswer holds the dry run and the real run to one answer: the exit code, the
// outcome, the refusal and the words of the error.
func assertSameAnswer(c *qt.C, dry clirun.Result, dryDoc failureDocument, ran clirun.Result, ranDoc failureDocument) {
	c.Helper()
	comment := qt.Commentf("dry run:\n%s\n%s\nreal run:\n%s\n%s", dry.Stdout, dry.Stderr, ran.Stdout, ran.Stderr)
	c.Assert(dry.ExitCode, qt.Equals, ran.ExitCode, comment)
	c.Assert(dryDoc.Outcome, qt.Equals, ranDoc.Outcome, comment)
	c.Assert(dryDoc.Refusal, qt.DeepEquals, ranDoc.Refusal, comment)
	c.Assert(dryDoc.Error, qt.DeepEquals, ranDoc.Error, comment)
}

// TestSchemaApplyDryRunAnswersAsARealRunE2E covers what a real run refuses ahead of its
// first statement, computed and saved.
func TestSchemaApplyDryRunAnswersAsARealRunE2E(t *testing.T) {
	t.Run("a saved plan the transaction mode refuses", func(t *testing.T) {
		c := qt.New(t)
		f := newFailureTarget(c, dbtarget.PostgreSQL)
		accounts, planned := f.table("accounts"), f.table("planned")
		f.exec(c, "CREATE TABLE "+accounts+" (id INT PRIMARY KEY, email VARCHAR(100))")
		// The saved plan's fingerprint covers the table above.
		plan := f.savedPlan(c, "CREATE TABLE "+accounts+" (id INT PRIMARY KEY, email VARCHAR(100));\nCREATE TABLE "+planned+" (id INT PRIMARY KEY)",
			"CREATE INDEX CONCURRENTLY "+accounts+"_email ON "+accounts+" (email)")

		dry, dryDoc := runApply(c, f, "--plan", plan, "--dry-run")
		ran, ranDoc := runApply(c, f, "--plan", plan)

		assertSameAnswer(c, dry, dryDoc, ran, ranDoc)
		c.Assert(dry.ExitCode, qt.Equals, 2)
		c.Assert(dryDoc.Outcome, qt.Equals, atlasschema.ApplyOutcomeRefused)
		c.Assert(dryDoc.Refusal.Code, qt.Equals, atlasschema.RefusalTransactionPreflight)
		c.Assert(f.indexExists(c, accounts+"_email"), qt.IsFalse)

		passing, passingDoc := runApply(c, f, "--plan", plan, "--tx-mode", "none", "--dry-run")
		c.Assert(passing.ExitCode, qt.Equals, 0, qt.Commentf("%s", passing.Stderr))
		c.Assert(passingDoc.Outcome, qt.Equals, atlasschema.ApplyOutcomeDryRun)
		c.Assert(f.indexExists(c, accounts+"_email"), qt.IsFalse)
	})

	t.Run("a computed plan the transaction mode refuses", func(t *testing.T) {
		c := qt.New(t)
		f := newFailureTarget(c, dbtarget.PostgreSQL)
		mood, notes := f.table("mood"), f.table("notes")
		f.exec(c, "CREATE TYPE "+mood+" AS ENUM ('calm')", "CREATE TABLE "+notes+" (id INT PRIMARY KEY)")
		schema := filepath.Join(c.TempDir(), "schema.sql")
		c.Assert(os.WriteFile(schema, fmt.Appendf(nil,
			"CREATE TYPE %[1]s AS ENUM ('calm', 'angry');\nCREATE TABLE %[2]s (id INT PRIMARY KEY, feeling %[1]s DEFAULT 'angry');\n", mood, notes), 0o600), qt.IsNil)

		dry, dryDoc := runApply(c, f, "--schema-file", schema, "--dry-run")
		ran, ranDoc := runApply(c, f, "--schema-file", schema)

		assertSameAnswer(c, dry, dryDoc, ran, ranDoc)
		c.Assert(dry.ExitCode, qt.Equals, 2)
		c.Assert(dryDoc.Outcome, qt.Equals, atlasschema.ApplyOutcomeRefused)
		c.Assert(dryDoc.Refusal.Code, qt.Equals, atlasschema.RefusalTransactionPreflight)
	})

	t.Run("a rehearsal the dev database refuses", func(t *testing.T) {
		c := qt.New(t)
		f := newFailureTarget(c, dbtarget.PostgreSQL)
		dev := newFailureTarget(c, dbtarget.PostgreSQL)
		declared, notes := f.table("declared"), f.table("notes")
		root := c.TempDir()
		c.Assert(os.WriteFile(filepath.Join(root, "entities.go"), fmt.Appendf(nil, declaredRowsEntities, declared, declared, notes), 0o600), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(root, "rows.yaml"), fmt.Appendf(nil, "- id: 1\n  email: %[1]q\n- id: 2\n  email: %[1]q\n", failureRowValue), 0o600), qt.IsNil)

		dry, dryDoc := runApply(c, f, "--root-dir", root, "--dev-url", dev.url, "--dry-run")
		ran, ranDoc := runApply(c, f, "--root-dir", root, "--dev-url", dev.url)

		c.Assert(dry.ExitCode, qt.Equals, ran.ExitCode)
		c.Assert(dry.ExitCode, qt.Equals, 2)
		c.Assert(dryDoc.Outcome, qt.Equals, ranDoc.Outcome)
		c.Assert(dryDoc.Outcome, qt.Equals, atlasschema.ApplyOutcomeRefused)
		c.Assert(dryDoc.Refusal.Code, qt.Equals, atlasschema.RefusalSimulationFailed)
		c.Assert(ranDoc.Refusal.Code, qt.Equals, atlasschema.RefusalSimulationFailed)
		c.Assert(f.tableExists(c, notes), qt.IsFalse)
	})

	t.Run("a saved plan naming a password variable that is not set", func(t *testing.T) {
		c := qt.New(t)
		f := newFailureTarget(c, dbtarget.PostgreSQL)
		role := f.table("role")
		plan := f.savedPlan(c, "CREATE TABLE "+f.table("planned")+" (id INT PRIMARY KEY)",
			`CREATE ROLE "`+role+`" WITH LOGIN PASSWORD `+rolepassword.Reference(role))
		c.Cleanup(func() { _, _ = f.conn.ExecContext(context.Background(), `DROP ROLE IF EXISTS "`+role+`"`) })

		dry, dryDoc := runApply(c, f, "--plan", plan, "--dry-run")
		// A real run asks for this statement as it sends it, after the prompt.
		ran, ranDoc := runApply(c, f, "--plan", plan, "--auto-approve")

		assertSameAnswer(c, dry, dryDoc, ran, ranDoc)
		c.Assert(dry.ExitCode, qt.Equals, 2)
		c.Assert(dryDoc.Outcome, qt.Equals, atlasschema.ApplyOutcomeFailed)
		c.Assert(dryDoc.Error.Message, qt.Contains, "which is not set")
		c.Assert(compactRecord(c, dryDoc.Failure), qt.Equals, "")
		c.Assert(f.roleExists(c, role), qt.IsFalse)
	})
}

// A saved SQL plan has the same statement preflight on the compatibility
// surface's dry run as it has when the approved plan executes.
func TestCompatSchemaApplyDryRunChecksSavedRolePasswordE2E(t *testing.T) {
	c := qt.New(t)
	f := newFailureTarget(c, dbtarget.PostgreSQL)
	role := f.table("role")
	variable := rolepassword.Variable(role)
	plan := f.savedPlan(c, "CREATE TABLE "+f.table("planned")+" (id INT PRIMARY KEY)",
		`CREATE ROLE "`+role+`" WITH LOGIN PASSWORD `+rolepassword.Reference(role))
	c.Cleanup(func() { _, _ = f.conn.ExecContext(context.Background(), `DROP ROLE IF EXISTS "`+role+`"`) })
	args := []string{"schema", "apply", "--url", f.url, "--plan", "file://" + plan}
	run := func(extra ...string) clirun.Result {
		return clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, append(args, extra...)...)
	}

	dry := run("--dry-run")
	ran := run("--auto-approve")
	c.Assert(dry.ExitCode, qt.Equals, ran.ExitCode, qt.Commentf("dry: %s\nreal: %s", dry.Stderr, ran.Stderr))
	c.Assert(dry.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(dry.Stderr, qt.Contains, variable+", which is not set")
	c.Assert(ran.Stderr, qt.Contains, variable+", which is not set")
	c.Assert(f.roleExists(c, role), qt.IsFalse)

	passing := clirun.Run(c, clirun.Compat,
		clirun.Options{Dir: c.TempDir(), Env: append(os.Environ(), variable+"=a-password")},
		append(args, "--dry-run")...)
	c.Assert(passing.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", passing.Stdout, passing.Stderr))
	c.Assert(f.roleExists(c, role), qt.IsFalse)
}

// TestSchemaApplyDryRunOfADeclaredPasswordReferenceE2E is the computed plan's way to a
// password variable: a schema that names one holds no value, and every run reads it.
// The control sets the variable, and the dry run passes it with the role still absent.
func TestSchemaApplyDryRunOfADeclaredPasswordReferenceE2E(t *testing.T) {
	c := qt.New(t)
	f := newFailureTarget(c, dbtarget.PostgreSQL)
	role := f.table("role")
	variable := rolepassword.Variable(role)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, fmt.Appendf(nil, `CREATE ROLE "%s" WITH LOGIN PASSWORD %s;`+"\n", role, rolepassword.Reference(role)), 0o600), qt.IsNil)
	c.Cleanup(func() { _, _ = f.conn.ExecContext(context.Background(), `DROP ROLE IF EXISTS "`+role+`"`) })

	dry, dryDoc := runApply(c, f, "--schema-file", schema, "--dry-run")
	ran, ranDoc := runApply(c, f, "--schema-file", schema, "--auto-approve")

	assertSameAnswer(c, dry, dryDoc, ran, ranDoc)
	c.Assert(dry.ExitCode, qt.Equals, 2)
	c.Assert(dryDoc.Outcome, qt.Equals, atlasschema.ApplyOutcomeFailed)
	c.Assert(dryDoc.Error.Message, qt.Contains, variable+", which is not set")
	c.Assert(f.roleExists(c, role), qt.IsFalse)

	passing := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Env: append(os.Environ(), variable+"=a-password")},
		"schema", "apply", "--db-url", f.url, "--schema-file", schema, "--dry-run", "--json")

	c.Assert(passing.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", passing.Stdout, passing.Stderr))
	c.Assert(passing.Stdout, qt.Contains, `"outcome": "dry-run"`)
	c.Assert(f.roleExists(c, role), qt.IsFalse)
}

// TestSchemaApplyDryRunNamesAVariableALaterStatementNeedsE2E is the one place the two
// answers part. A real run without a transaction has sent the first statement when it
// reaches the second, so it reports unknown, and the table is there. A dry run has sent
// nothing, so it reports failed, with the same exit code and the same words.
func TestSchemaApplyDryRunNamesAVariableALaterStatementNeedsE2E(t *testing.T) {
	c := qt.New(t)
	f := newFailureTarget(c, dbtarget.PostgreSQL)
	first, role := f.table("first_unit"), f.table("role")
	plan := f.savedPlan(c, "CREATE TABLE "+f.table("planned")+" (id INT PRIMARY KEY)",
		"CREATE TABLE "+first+" (id INT PRIMARY KEY)",
		`CREATE ROLE "`+role+`" WITH LOGIN PASSWORD `+rolepassword.Reference(role))
	c.Cleanup(func() { _, _ = f.conn.ExecContext(context.Background(), `DROP ROLE IF EXISTS "`+role+`"`) })

	dry, dryDoc := runApply(c, f, "--plan", plan, "--tx-mode", "none", "--dry-run")

	c.Assert(dry.ExitCode, qt.Equals, 2, qt.Commentf("%s\n%s", dry.Stdout, dry.Stderr))
	c.Assert(dryDoc.Outcome, qt.Equals, atlasschema.ApplyOutcomeFailed)
	c.Assert(compactRecord(c, dryDoc.Failure), qt.Equals, "")
	c.Assert(f.tableExists(c, first), qt.IsFalse)

	ran, ranDoc := runApply(c, f, "--plan", plan, "--tx-mode", "none", "--auto-approve")

	c.Assert(ran.ExitCode, qt.Equals, dry.ExitCode)
	c.Assert(ranDoc.Outcome, qt.Equals, atlasschema.ApplyOutcomeUnknown)
	c.Assert(ranDoc.Error, qt.DeepEquals, dryDoc.Error)
	c.Assert(f.tableExists(c, first), qt.IsTrue)
}

// indexExists asks the server whether an index is there.
func (f *failureTarget) indexExists(c *qt.C, name string) bool {
	c.Helper()
	var count int
	c.Assert(f.conn.QueryRowContext(c.Context(), "SELECT count(*) FROM pg_indexes WHERE indexname = $1", name).Scan(&count), qt.IsNil)
	return count > 0
}

// roleExists asks the server whether a role is there.
func (f *failureTarget) roleExists(c *qt.C, name string) bool {
	c.Helper()
	var count int
	c.Assert(f.conn.QueryRowContext(c.Context(), "SELECT count(*) FROM pg_roles WHERE rolname = $1", name).Scan(&count), qt.IsNil)
	return count > 0
}
