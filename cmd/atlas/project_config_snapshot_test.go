package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
)

func TestCompatCommandMigrateDownUsesPtahSafetySnapshot(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	projectDir := filepath.Join(dir, "project")
	migrationsDir := filepath.Join(projectDir, "migrations")
	dbPath := filepath.Join(dir, "snapshot.db")
	writeMigrateDownFixture(c, migrationsDir, dbPath)
	c.Assert(os.WriteFile("ptah.yaml", []byte(`env:
  local:
    migration:
      pre_down_hook: "echo snapshot-hook; exit 8"
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "project.hcl"), []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetIn(strings.NewReader("YES\n"))
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--config", filepath.Join(projectDir, "project.hcl"),
		"--env", "local",
		"--to-version", "1",
	})

	err := cmd.Execute()

	c.Assert(
		err,
		qt.ErrorMatches,
		"(?s).*down pre-flight custom command hook failed: exit status 8.*snapshot-hook.*",
	)
	c.Assert(sqliteTableCount(c, dbPath, "down_fmt_audit"), qt.Equals, 1)
}

func TestCompatCommandMigrateDownPreservesPtahPathBase(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	projectDir := filepath.Join(dir, "project")
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "ptah-path.db")
	writeMigrateDownFixture(c, migrationsDir, dbPath)
	c.Assert(os.MkdirAll(projectDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile("ptah.yaml", []byte(`env:
  local:
    migration:
      dir: ./migrations
      pre_down_hook: "echo ptah-path-hook; exit 8"
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "project.hcl"), []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetIn(strings.NewReader("YES\n"))
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--config", filepath.Join(projectDir, "project.hcl"),
		"--env", "local",
		"--to-version", "1",
	})

	err := cmd.Execute()

	c.Assert(
		err,
		qt.ErrorMatches,
		"(?s).*down pre-flight custom command hook failed: exit status 8.*ptah-path-hook.*",
	)
	c.Assert(sqliteTableCount(c, dbPath, "down_fmt_audit"), qt.Equals, 1)
}

func TestCompatCommandMigrateDownEnvironmentOverridesSafetySnapshot(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "environment.db")
	writeMigrateDownFixture(c, migrationsDir, dbPath)
	c.Assert(os.WriteFile("ptah.yaml", []byte(`env:
  local:
    migration:
      pre_down_hook: "echo snapshot-hook; exit 8"
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("project.hcl", []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)
	t.Setenv("PTAH_PRE_DOWN_HOOK", "echo environment-hook; exit 9")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetIn(strings.NewReader("YES\n"))
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--config", "project.hcl",
		"--env", "local",
		"--to-version", "1",
	})

	err := cmd.Execute()

	c.Assert(
		err,
		qt.ErrorMatches,
		"(?s).*down pre-flight custom command hook failed: exit status 9.*environment-hook.*",
	)
	c.Assert(sqliteTableCount(c, dbPath, "down_fmt_audit"), qt.Equals, 1)
}

func TestCompatCommandMigrateDownNativeDirectoryEnvironmentOverridesProjectConfig(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "environment-migrations")
	dbPath := filepath.Join(dir, "native-directory-environment.db")
	writeMigrateDownFixture(c, migrationsDir, dbPath)
	c.Assert(os.WriteFile("ptah.yaml", []byte(`env:
  local:
    migration:
      pre_down_hook: "echo native-directory-hook; exit 8"
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("project.hcl", []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  migration {
    dir = "file://missing-config-migrations"
  }
}
`), 0o600), qt.IsNil)
	t.Setenv("PTAH_MIGRATIONS_DIR", migrationsDir)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetIn(strings.NewReader("YES\n"))
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--config", "project.hcl",
		"--env", "local",
		"--to-version", "1",
	})

	err := cmd.Execute()

	c.Assert(
		err,
		qt.ErrorMatches,
		"(?s).*down pre-flight custom command hook failed: exit status 8.*native-directory-hook.*",
	)
	c.Assert(sqliteTableCount(c, dbPath, "down_fmt_audit"), qt.Equals, 1)
}

func TestCompatCommandProjectConfigDefersToDirectoryEnvironment(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	configDir := filepath.Join(dir, "config-migrations")
	environmentDir := filepath.Join(dir, "environment-migrations")
	c.Assert(os.MkdirAll(configDir, 0o755), qt.IsNil)
	c.Assert(os.MkdirAll(environmentDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(configDir, "1_config.sql"),
		[]byte("CREATE TABLE config_table (id int);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(environmentDir, "1_environment.sql"),
		[]byte("CREATE TABLE environment_one (id int);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(environmentDir, "2_environment.sql"),
		[]byte("CREATE TABLE environment_two (id int);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile("project.hcl", []byte(`env "local" {
  migration {
    dir = "file://config-migrations"
  }
}
`), 0o600), qt.IsNil)
	t.Setenv("PTAH_DIR", "file://"+environmentDir)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "hash",
		"--config", "project.hcl",
		"--env", "local",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "2 migration file(s) hashed")
}
