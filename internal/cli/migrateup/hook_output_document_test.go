package migrateup_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// A pre-flight hook's output is written for a person, so under --json it goes
// to standard error and the document stays the only thing on standard output.
func TestMigrateUpJSONKeepsHookOutputOffTheDocument(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := runUpSplit("--db-url", "sqlite://"+filepath.Join(t.TempDir(), "app.db"),
		"--migrations-dir", writeUpMigrations(t), "--json", "--pre-up-hook", "echo hook-said-this")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	c.Assert(decodeOnly[migrator.RunResult](c, stdout).Outcome, qt.Equals, migrator.RunOutcomeApplied)
	c.Assert(stderr, qt.Contains, "hook-said-this")
}
