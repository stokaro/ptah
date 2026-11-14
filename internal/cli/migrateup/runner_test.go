package migrateup_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/migrateup"
)

// A runner Ptah does not model is refused before the source is read and the
// database is opened, from the flag or from ptah.yaml: applying a directory as
// a tool runs it that Ptah cannot run is not something to fall back from. The
// directory here does not exist, so a refusal that came after the source
// would name the directory instead.
func TestMigrateUpCommand_RunnerRefusesAnUnknownTool(t *testing.T) {
	tests := []struct {
		name   string
		config string
		flags  []string
	}{
		{name: "the flag", flags: []string{"--runner", "goose"}},
		{name: "ptah.yaml", config: "migration:\n  runner: goose\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := filepath.Join(c.TempDir(), "missing")
			config := filepath.Join(c.TempDir(), "ptah.yaml")
			c.Assert(os.WriteFile(config, []byte(test.config), 0o600), qt.IsNil)
			database := filepath.Join(c.TempDir(), "ptah.db")
			cmd := migrateup.NewMigrateUpCommand()
			cmd.SetArgs(append([]string{"--config", config, "--migrations-dir", dir,
				"--db-url", atlasurl.SQLiteURLFromPath(database)}, test.flags...))

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, `invalid runner "goose": expected ptah or golang-migrate`)
			_, statErr := os.Stat(database)
			c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
		})
	}
}

// The runner is resolved before the command's early returns, so a malformed
// value is refused on an invocation that names neither a database nor a
// directory, and not only on one that reaches the run.
func TestMigrateUpCommand_RunnerIsRefusedBeforeTheEarlyReturns(t *testing.T) {
	c := qt.New(t)
	config := filepath.Join(c.TempDir(), "ptah.yaml")
	c.Assert(os.WriteFile(config, nil, 0o600), qt.IsNil)
	cmd := migrateup.NewMigrateUpCommand()
	cmd.SetArgs([]string{"--config", config, "--runner", "goose"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `invalid runner "goose": expected ptah or golang-migrate`)
}
