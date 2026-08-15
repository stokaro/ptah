package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

func TestCompatDirectCommandReportsIgnoredAtlasConstructs(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url     = "sqlite://`+filepath.Join(dir, "inspect.db")+`"
  project = "ignored"
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"schema", "inspect", "--env", "local"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(
		stderr.String(),
		qt.Equals,
		"warning: atlas.hcl attribute \"project\" at atlas.hcl:3 is ignored for Atlas compatibility and has no effect\n",
	)
	c.Assert(stdout.String(), qt.Not(qt.Contains), "warning: atlas.hcl")
}

func TestCompatAdapterCommandReportsIgnoredAtlasConstructs(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	c.Assert(os.Mkdir("migrations", 0o700), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  project = "ignored"
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"migrate", "hash", "--env", "local"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(
		stderr.String(),
		qt.Equals,
		"warning: atlas.hcl attribute \"project\" at atlas.hcl:2 is ignored for Atlas compatibility and has no effect\n",
	)
	c.Assert(stdout.String(), qt.Not(qt.Contains), "warning: atlas.hcl")
}

func TestCompatConvertedIntegrityCommandReportsIgnoredAtlasConstructs(t *testing.T) {
	c := qt.New(t)
	configPath, migrationsDir := writeIntegrityProject(c, "goose")
	c.Assert(os.WriteFile(configPath, []byte(`env "local" {
  project = "ignored"
  migration {
    dir    = "file://migrations"
    format = goose
  }
}
`), 0o600), qt.IsNil)

	stdout, stderr, err := runCompatExit(
		"migrate", "hash", "--config", "file://"+configPath, "--env", "local",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(
		stderr,
		qt.Equals,
		"warning: atlas.hcl attribute \"project\" at "+configPath+":2 is ignored for Atlas compatibility and has no effect\n",
	)
	c.Assert(sumEntryNames(c, migrationsDir), qt.DeepEquals, sqlSuffixCoveredSet)
}

func TestCompatConvertedMigrateNewReportsIgnoredAtlasConstructs(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  project = "ignored"
  migration {
    dir    = "file://migrations"
    format = goose
  }
}
`), 0o600), qt.IsNil)

	stdout, stderr, err := runCompatExit("migrate", "new", "add_users", "--env", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(
		stderr,
		qt.Equals,
		"warning: atlas.hcl attribute \"project\" at atlas.hcl:2 is ignored for Atlas compatibility and has no effect\n",
	)
	created, err := filepath.Glob(filepath.Join("migrations", "*_add_users.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(created, qt.HasLen, 1)
}
