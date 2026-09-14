package migrateup_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/migrateup"
	"ptah.run/migration/migrator"
)

// runUpSplit keeps the two streams apart, which is the property the document
// depends on: it is standard output, and everything written for a person is
// not.
func runUpSplit(args ...string) (stdout, stderr string, err error) {
	cmd := migrateup.NewMigrateUpCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// TestMigrateUpJSONReportsWhatTheRunApplied covers the document a caller reads
// instead of parsing a summary written for a person.
func TestMigrateUpJSONReportsWhatTheRunApplied(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeUpMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "evidence.db")

	out, human, err := runUpSplit("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--json")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", human))
	result := decodeRunResult(c, out)
	c.Assert(result.ContractVersion, qt.Equals, migrator.RunContractVersion)
	c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomeApplied)
	c.Assert(result.Direction, qt.Equals, migrator.MigrationDirectionUp)
	c.Assert(result.Planned, qt.DeepEquals, []int64{1, 2})
	c.Assert(result.Applied, qt.DeepEquals, []int64{1, 2})
	c.Assert(result.Error, qt.Equals, "")
	c.Assert(result.Status, qt.IsNotNil)
	c.Assert(result.Status.CurrentVersion, qt.Equals, int64(2))
	c.Assert(out, qt.Not(qt.Contains), "=== MIGRATE UP ===",
		qt.Commentf("the summary written for a person would not parse as the document"))
	c.Assert(human, qt.Contains, "=== MIGRATE UP ===",
		qt.Commentf("and it is still written, on the other stream"))
}

// TestMigrateUpJSONReportsAFailedRun is why the document is written on both
// paths: the caller that most needs to know what the database now holds is the
// one whose run stopped.
func TestMigrateUpJSONReportsAFailedRun(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeUpMigrations(t)
	broken := "CREATE TABLE payments (id INTEGER PRIMARY KEY);\nNOT SQL;\n"
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "0000000003_broken.up.sql"), []byte(broken), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "0000000003_broken.down.sql"),
		[]byte("DROP TABLE payments;\n"), 0o600), qt.IsNil)
	dbPath := filepath.Join(t.TempDir(), "evidence-failure.db")

	out, _, err := runUpSplit("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--json")

	c.Assert(err, qt.IsNotNil, qt.Commentf("a failed run still fails"))
	result := decodeRunResult(c, out)
	c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomeFailed)
	c.Assert(result.Planned, qt.DeepEquals, []int64{1, 2, 3})
	c.Assert(result.Applied, qt.DeepEquals, []int64{1, 2})
	c.Assert(result.Error, qt.Not(qt.Equals), "")
	c.Assert(result.Status, qt.IsNotNil)
	c.Assert(result.Status.DirtyRevision, qt.IsNotNil)
	c.Assert(result.Status.DirtyRevision.Version, qt.Equals, int64(3))
}

// TestMigrateUpJSONReportsADryRun keeps a preview from reading as work.
func TestMigrateUpJSONReportsADryRun(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeUpMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "evidence-dry-run.db")

	out, human, err := runUpSplit("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--json", "--dry-run")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", human))
	result := decodeRunResult(c, out)
	c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomeDryRun)
	c.Assert(result.Applied, qt.IsNil)
	c.Assert(result.Status.CurrentVersion, qt.Equals, int64(0))
}

func decodeRunResult(c *qt.C, out string) migrator.RunResult {
	c.Helper()
	var result migrator.RunResult
	c.Assert(json.Unmarshal([]byte(out), &result), qt.IsNil, qt.Commentf("output was:\n%s", out))
	return result
}
