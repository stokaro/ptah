//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
	"ptah.run/migration/migrator"
)

// The error-reasons fixture applies nothing of a unit that names the table
// "refused", and rolls back a commit of a transaction that created the table
// "uncommitted". Each result carries the fixture's own reason, and a message
// with a fake credential, which no host prints.
const (
	refusedUnitReason   = "the fixture refuses to create this table"
	refusedCommitReason = "the fixture refuses to commit this transaction"
)

// executionReasonsCLI selects the error-reasons fixture, started with the
// given environment.
func executionReasonsCLI(c *qt.C, environment ...string) providerCLI {
	c.Helper()
	binary := testutils.BuildFixtureProvider(c, "error-reasons")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	return newProviderCLI(c, providerhost.Spec{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture",
		Environment: environment})
}

// oneMigration writes a directory whose one migration creates table.
func oneMigration(c *qt.C, table string) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_"+table+".up.sql"), []byte(`CREATE TABLE "`+table+`" ("id" INTEGER);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_"+table+".down.sql"), []byte(`DROP TABLE "`+table+`";`), 0o600), qt.IsNil)
	return dir
}

// failedRun is what a failed `migrations up --json` printed: its standard
// error, the run document's error message, and the error the history records
// for the migration.
type failedRun struct {
	diagnostics, message, recorded string
}

// all is every text of the run.
func (r failedRun) all() string { return r.diagnostics + r.message + r.recorded }

// failedUp runs `migrations up --json` over dir, which must fail.
func failedUp(c *qt.C, f providerCLI, dir string) failedRun {
	c.Helper()
	out, diagnostics, code := executeProviderCLI(c.Context(), migrationArguments(f, "up", "--migrations-dir", dir, "--tx-mode", "file", "--json"))
	c.Assert(code, qt.Equals, 2, qt.Commentf("%s\n%s", diagnostics, out))
	f.assertStopped(c)
	var result migrator.RunResult
	c.Assert(json.Unmarshal([]byte(out), &result), qt.IsNil, qt.Commentf("%s", out))
	c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomeFailed)
	c.Assert(result.Error, qt.IsNotNil)
	c.Assert(result.Status.DirtyRevision, qt.IsNotNil, qt.Commentf("%s", out))
	return failedRun{diagnostics: diagnostics, message: result.Error.Message, recorded: result.Status.DirtyRevision.Error}
}

// tableHeld reports whether the fixture's database holds table.
func tableHeld(c *qt.C, f providerCLI, table string) bool {
	c.Helper()
	connection, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(f.databasePath))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(connection.Close(), qt.IsNil) })
	return providerTablePresent(c, connection, table)
}

// A provider that lists errors.reasons says why a migration's unit applied
// nothing and why its commit rolled back, and the host prints each reason
// after the outcome, on standard error and in the run document. The history
// records the unit's reason with the failed migration. The results' messages
// stay out of all of them.
func TestProviderExecutionReasons_HappyPath(t *testing.T) {
	t.Run("a unit that applied nothing", func(t *testing.T) {
		c := qt.New(t)
		f := executionReasonsCLI(c)
		c.Assert(checkedFeatures(c, f), qt.Contains, provider.ErrorReasonsFeature)

		run := failedUp(c, f, oneMigration(c, "refused"))

		want := "provider execution: NOT_APPLIED (0 units confirmed applied): " + refusedUnitReason
		c.Assert(run.diagnostics, qt.Contains, want)
		c.Assert(run.message, qt.Contains, want)
		c.Assert(run.recorded, qt.Contains, want)
		c.Assert(run.all(), qt.Not(qt.Contains), fixtureCredential)
		c.Assert(tableHeld(c, f, "refused"), qt.IsFalse)
	})
	t.Run("a commit that rolled back", func(t *testing.T) {
		c := qt.New(t)
		f := executionReasonsCLI(c)

		run := failedUp(c, f, oneMigration(c, "uncommitted"))

		want := "provider transaction outcome is ROLLED_BACK: " + refusedCommitReason
		c.Assert(run.diagnostics, qt.Contains, want)
		c.Assert(run.message, qt.Contains, want)
		// The revision keeps the mark it was given before the body ran, so it
		// records no error of the commit's (G107).
		c.Assert(run.recorded, qt.Equals, "statement execution outcome is unknown after process interruption")
		c.Assert(run.all(), qt.Not(qt.Contains), fixtureCredential)
		c.Assert(tableHeld(c, f, "uncommitted"), qt.IsFalse)
	})
}

// A provider that does not list errors.reasons reaches the host as the
// outcome alone, though it sends the same reasons: the SDK server removes
// them, and the host would not print them.
func TestProviderExecutionReasons_FailurePath(t *testing.T) {
	t.Run("a unit that applied nothing", func(t *testing.T) {
		c := qt.New(t)
		f := executionReasonsCLI(c, "PTAH_FIXTURE_OMITTED="+provider.ErrorReasonsFeature)
		c.Assert(checkedFeatures(c, f), qt.Not(qt.Contains), provider.ErrorReasonsFeature)

		run := failedUp(c, f, oneMigration(c, "refused"))

		c.Assert(run.diagnostics, qt.Contains, "provider execution: NOT_APPLIED (0 units confirmed applied)")
		c.Assert(run.all(), qt.Not(qt.Contains), refusedUnitReason)
		c.Assert(run.all(), qt.Not(qt.Contains), fixtureCredential)
	})
	t.Run("a commit that rolled back", func(t *testing.T) {
		c := qt.New(t)
		f := executionReasonsCLI(c, "PTAH_FIXTURE_OMITTED="+provider.ErrorReasonsFeature)

		run := failedUp(c, f, oneMigration(c, "uncommitted"))

		c.Assert(run.diagnostics, qt.Contains, "provider transaction outcome is ROLLED_BACK")
		c.Assert(run.all(), qt.Not(qt.Contains), refusedCommitReason)
		c.Assert(run.all(), qt.Not(qt.Contains), fixtureCredential)
	})
}
