package migraterepair_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/migraterepair"
	"ptah.run/internal/cli/migrateup"
)

// `ptah migrations repair` reads ptah.yaml under its flags, as `migrations up`
// and `migrations down` do, and resolves the runner the same way: --runner,
// then PTAH_RUNNER, then `migration.runner`. The e2e tests under integration/
// drive PTAH_RUNNER, in a process whose environment they own.

// appliedProjectConfig applies one migration to a SQLite database and writes
// a ptah.yaml that names the database and the directory. It returns the path
// of the ptah.yaml.
func appliedProjectConfig(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("DROP TABLE widgets;\n"), 0o600),
		qt.IsNil)
	dbURL := atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "repair.db"))
	up := migrateup.NewMigrateUpCommand()
	var out bytes.Buffer
	up.SetOut(&out)
	up.SetErr(&out)
	up.SetArgs([]string{"--db-url", dbURL, "--migrations-dir", dir})
	c.Assert(up.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
	configPath := filepath.Join(c.TempDir(), "ptah.yaml")
	config := "url: " + dbURL + "\nmigration:\n  dir: " + dir + "\n"
	c.Assert(os.WriteFile(configPath, []byte(config), 0o600), qt.IsNil)
	return configPath
}

func runRepair(args ...string) (string, error) {
	cmd := migraterepair.NewMigrateRepairCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// The database and the directory come from ptah.yaml, so the repair reaches
// the revision and reads it: the applied migration is not dirty, and a plain
// repair of it is refused for that reason.
func TestMigrateRepairCommandReadsTheProjectConfig_FailurePath(t *testing.T) {
	c := qt.New(t)
	configPath := appliedProjectConfig(c)

	_, err := runRepair("--config", configPath, "--version", "1")

	c.Assert(err, qt.ErrorMatches, `migration 1 is not dirty; rerun with --force to rewrite it`)
}

// The control: the forced rewrite the refusal names runs against the same
// database, read from the same ptah.yaml.
func TestMigrateRepairCommandReadsTheProjectConfig_HappyPath(t *testing.T) {
	c := qt.New(t)
	configPath := appliedProjectConfig(c)

	out, err := runRepair("--config", configPath, "--version", "1", "--force")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Updated migration 1 revision metadata; no migration SQL ran")
}

// A runner that names no tool is refused before anything else is read, from
// the flag or from ptah.yaml, so it cannot stay unread because the invocation
// lacks a database or a version.
func TestMigrateRepairCommandRefusesAnUnknownRunner_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		config string
		args   []string
	}{
		{name: "named by the flag", args: []string{"--runner", "goose"}},
		{name: "named in ptah.yaml", config: "migration:\n  runner: goose\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			configPath := filepath.Join(c.TempDir(), "ptah.yaml")
			c.Assert(os.WriteFile(configPath, []byte(test.config), 0o600), qt.IsNil)

			_, err := runRepair(append([]string{"--config", configPath}, test.args...)...)

			c.Assert(err, qt.ErrorMatches, `invalid runner "goose": expected ptah or golang-migrate`)
		})
	}
}
