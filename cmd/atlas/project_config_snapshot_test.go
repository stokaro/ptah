package atlas_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
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

func TestCompatCommandMigrateDownUsesProjectDevURLForShadowVerification(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "project-dev.db")
	writeMigrateDownFixture(c, migrationsDir, dbPath)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "2_add_audit.down.sql"),
		[]byte("DROP TABLE no_such_table;\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile("ptah.yaml", []byte(`env:
  local:
    dev: "sqlite://`+filepath.Join(dir, "shadow.db")+`"
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("project.hcl", []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--config", "project.hcl",
		"--env", "local",
		"--to-version", "1",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `(?s)rollback verification failed: .*no_such_table.*`)
	c.Assert(sqliteTableCount(c, dbPath, "down_fmt_audit"), qt.Equals, 1)
}

func TestCompatCommandMigrateDownExplicitDirectoryOverridesUnsupportedProjectPaths(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "explicit-migrations")
	dbPath := filepath.Join(dir, "explicit-directory.db")
	writeMigrateDownFixture(c, migrationsDir, dbPath)
	c.Assert(os.WriteFile("project.hcl", []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  schema {
    src = ["postgres://unused/schema"]
  }
  migration {
    dir = "atlas://remote/migrations"
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
		"--config", "project.hcl",
		"--env", "local",
		"--dir", "file://" + migrationsDir,
		"--to-version", "1",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(sqliteTableCount(c, dbPath, "down_fmt_audit"), qt.Equals, 0)
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
  schema {
    src = ["postgres://unused/schema"]
  }
  migration {
    dir = "atlas://remote/migrations"
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
	c.Assert(out.String(), qt.Equals, "")
	_, err = os.Stat(filepath.Join(environmentDir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
}

func TestCompatCommandProjectSelectionDoesNotLeakAcrossRootReuse(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "1_init.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate",
		"--config", "file://missing-first.hcl",
		"--env", "first",
		"--var", "name=first",
		"--help",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	out.Reset()
	cmd.SetArgs([]string{
		"migrate", "hash",
		"--dir", "file://" + migrationsDir,
	})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Equals, "")
	_, err = os.Stat(filepath.Join(migrationsDir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
}

func TestCompatCommandProjectSelectionDoesNotLeakAfterArgumentValidationFailure(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "1_init.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate",
		"--config", "file://missing-first.hcl",
		"--env", "first",
		"--var", "name=first",
		"status", "unexpected",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unexpected positional arguments \["unexpected"\]`)
	out.Reset()
	cmd.SetArgs([]string{
		"migrate", "hash",
		"--dir", "file://" + migrationsDir,
	})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Equals, "")
	_, err = os.Stat(filepath.Join(migrationsDir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
}

func TestCompatCommandProjectSelectionDoesNotLeakAfterFlagGroupValidationFailure(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--config", "file://missing-first.hcl",
		"--env", "first",
		"--var", "name=first",
		"--file", "schema.hcl",
		"--to", "schema.sql",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `if any flags in the group \[file to\] are set none of the others can be; \[file to\] were all set`)
	out.Reset()
	cmd.SetArgs([]string{
		"schema", "inspect",
		"--url", "sqlite://" + filepath.Join(dir, "target.db"),
	})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
}

func TestCompatCommandProjectSelectionDoesNotLeakAfterCompletion(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"__complete",
		"schema", "inspect",
		"--config", "file://missing-first.hcl",
		"--env", "first",
		"--var", "name=first",
		"",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	out.Reset()
	cmd.SetArgs([]string{
		"schema", "inspect",
		"--url", "sqlite://" + filepath.Join(dir, "target.db"),
	})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
}

func TestCompatCommandProjectEnvironmentRemainsEffectiveAcrossRootReuse(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "environment-migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "1_init.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile("project.hcl", []byte(`env "local" {
  migration {
    dir = "file://environment-migrations"
  }
}
`), 0o600), qt.IsNil)
	t.Setenv("PTAH_CONFIG", "file://project.hcl")
	t.Setenv("PTAH_ENV", "local")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "hash"})

	firstErr := cmd.Execute()

	c.Assert(firstErr, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Equals, "")
	_, statErr := os.Stat(filepath.Join(migrationsDir, "atlas.sum"))
	c.Assert(statErr, qt.IsNil)
	out.Reset()
	cmd.SetArgs([]string{"migrate", "hash"})

	secondErr := cmd.Execute()

	c.Assert(secondErr, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Equals, "")
}

func TestCompatMigrateHashHelpUsesUpdatedRootWriter(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(dir, "1_init.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var firstOut bytes.Buffer
	cmd.SetOut(&firstOut)
	cmd.SetErr(&firstOut)
	cmd.SetArgs([]string{"migrate", "hash", "--dir", "file://" + dir})

	firstErr := cmd.Execute()

	c.Assert(firstErr, qt.IsNil, qt.Commentf("%s", firstOut.String()))
	c.Assert(firstOut.String(), qt.Equals, "")
	var secondOut bytes.Buffer
	cmd.SetOut(&secondOut)
	cmd.SetErr(&secondOut)
	cmd.SetArgs([]string{"migrate", "hash", "--help"})

	secondErr := cmd.Execute()

	c.Assert(secondErr, qt.IsNil, qt.Commentf("%s", secondOut.String()))
	c.Assert(firstOut.String(), qt.Equals, "")
	c.Assert(secondOut.String(), qt.Contains, "Usage:\n  atlas migrate hash [flags]")
}

func TestCompatCommandProjectSelectionDoesNotLeakAfterRootHelp(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "1_init.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile("second.hcl", []byte(`env "local" {
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)
	t.Setenv("PTAH_CONFIG", "file://second.hcl")
	t.Setenv("PTAH_ENV", "local")

	cmd := atlas.NewCompatCommand("atlas")
	migrate, _, err := cmd.Find([]string{"migrate"})
	c.Assert(err, qt.IsNil)
	c.Assert(migrate.PersistentFlags().Set("config", "file://missing-first.hcl"), qt.IsNil)
	c.Assert(migrate.PersistentFlags().Set("env", "first"), qt.IsNil)
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--help"})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil)
	out.Reset()
	cmd.SetArgs([]string{"migrate", "hash"})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Equals, "")
	_, err = os.Stat(filepath.Join(migrationsDir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
}

func TestCompatCommandProjectSelectionDoesNotLeakAfterRootArgumentFailure(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "1_init.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile("second.hcl", []byte(`env "local" {
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)
	t.Setenv("PTAH_CONFIG", "file://second.hcl")
	t.Setenv("PTAH_ENV", "local")

	cmd := atlas.NewCompatCommand("atlas")
	migrate, _, err := cmd.Find([]string{"migrate"})
	c.Assert(err, qt.IsNil)
	c.Assert(migrate.PersistentFlags().Set("config", "file://missing-first.hcl"), qt.IsNil)
	c.Assert(migrate.PersistentFlags().Set("env", "first"), qt.IsNil)
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"unexpected"})

	err = cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unknown command "unexpected" for "atlas"`)
	out.Reset()
	cmd.SetArgs([]string{"migrate", "hash"})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Equals, "")
	_, err = os.Stat(filepath.Join(migrationsDir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
}

func TestCompatCommandDirectExecutionRefreshesContextAcrossRootReuse(t *testing.T) {
	c := qt.New(t)
	dbURL := "sqlite://" + filepath.Join(t.TempDir(), "target.db")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "inspect", "--url", dbURL})

	err := cmd.ExecuteContext(context.Background())

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	out.Reset()
	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()
	cmd.SetArgs([]string{"schema", "inspect", "--url", dbURL})

	err = cmd.ExecuteContext(canceledContext)

	c.Assert(err, qt.ErrorMatches, `connect to --url: failed to ping database: context canceled`)
}
