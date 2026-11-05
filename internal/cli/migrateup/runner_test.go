package migrateup_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/migrateup"
)

// A runner Ptah does not model is refused before the database is opened,
// from the flag or from ptah.yaml: applying a directory as a tool runs it
// that Ptah cannot run is not something to fall back from.
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
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte("CREATE TABLE t (id int);\n"), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("DROP TABLE t;\n"), 0o600), qt.IsNil)
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
