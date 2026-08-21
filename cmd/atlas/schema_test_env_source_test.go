package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// `schema test -u env://src` was refused by name while `--to env://src`
// resolved, because the verb's flag list is built at registration, where no
// environment exists yet, so its mapper could only ever refuse the scheme
// (stokaro/ptah#1761). These tests hold the resolution and the refusals that
// stay.

// writeSchemaTestEnvProject writes an atlas.hcl whose single env names a
// schema file, that file, and a case directory asserting the schema is really
// applied: the INSERT fails unless the table the env names exists.
func writeSchemaTestEnvProject(c *qt.C) string {
	baseDir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(baseDir, "atlas.hcl"), []byte(`env "dev" {
  src = "file://desired.sql"
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(baseDir, "desired.sql"), []byte(`
CREATE TABLE env_users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);
`), 0o600), qt.IsNil)
	cases := filepath.Join(baseDir, "tests")
	c.Assert(os.MkdirAll(cases, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(cases, "case.yaml"), []byte(`cases:
  - name: the env's schema is applied
    steps:
      - apply_schema: true
      - exec: "INSERT INTO env_users (id, name) VALUES (1, 'a')"
`), 0o600), qt.IsNil)
	return baseDir
}

// runSchemaTest runs the compat verb with the given source arguments and
// returns its combined output and error.
func runSchemaTest(c *qt.C, baseDir string, args ...string) (string, error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(append([]string{
		"schema", "test",
		"--config", "file://" + filepath.Join(baseDir, "atlas.hcl"),
	}, append(args, filepath.Join(baseDir, "tests"))...))

	err := cmd.Execute()
	c.Logf("schema test output:\n%s", out.String())
	return out.String(), err
}

func TestSchemaTestEnvSrcSourceResolves(t *testing.T) {
	c := qt.New(t)

	baseDir := writeSchemaTestEnvProject(c)

	out, err := runSchemaTest(c, baseDir, "--env", "dev", "-u", "env://src")

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}

// TestSchemaTestEnvSrcAppliesTheSchemaTheEnvNames separates resolution from a
// source the verb might have found on its own: a second schema file sits
// beside the one the env names, and the case only passes against the named
// one, because the other declares a different table.
func TestSchemaTestEnvSrcAppliesTheSchemaTheEnvNames(t *testing.T) {
	c := qt.New(t)

	baseDir := writeSchemaTestEnvProject(c)
	other := filepath.Join(baseDir, "other.sql")
	c.Assert(os.WriteFile(other, []byte("CREATE TABLE not_named (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	named, namedErr := runSchemaTest(c, baseDir, "--env", "dev", "-u", "env://src")
	_, otherErr := runSchemaTest(c, baseDir, "--env", "dev", "-u", "file://"+other)

	c.Assert(namedErr, qt.IsNil)
	c.Assert(named, qt.Contains, "1 cases, 1 passed, 0 failed")
	c.Assert(otherErr, qt.IsNotNil)
}

// TestSchemaTestEnvSrcMatchesTheDesiredStateFlags is the parity check the
// issue asks for: -u must answer an env:// reference the way --to does, so a
// run that names no env resolves on both or on neither.
func TestSchemaTestEnvSrcMatchesTheDesiredStateFlags(t *testing.T) {
	c := qt.New(t)

	baseDir := writeSchemaTestEnvProject(c)

	out, err := runSchemaTest(c, baseDir, "-u", "env://src")

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "1 cases, 1 passed, 0 failed")
}

// TestSchemaTestUnknownEnvAttributeIsStillNamed is the negative control: the
// expansion did not swallow the attribute allowlist that tells a user with a
// typo what they can write.
func TestSchemaTestUnknownEnvAttributeIsStillNamed(t *testing.T) {
	c := qt.New(t)

	baseDir := writeSchemaTestEnvProject(c)

	_, err := runSchemaTest(c, baseDir, "--env", "dev", "-u", "env://bogus")

	c.Assert(err, qt.ErrorMatches, `.*unsupported env:// attribute "bogus".*`)
}

// TestSchemaTestPlainSourceIgnoresTheEnvironment is the other negative
// control: expansion keys on the env:// scheme, so a plain source still wins
// over the one the environment names.
func TestSchemaTestPlainSourceIgnoresTheEnvironment(t *testing.T) {
	c := qt.New(t)

	baseDir := writeSchemaTestEnvProject(c)

	_, err := runSchemaTest(c, baseDir, "--env", "dev", "-u", "sqlite://"+filepath.Join(c.TempDir(), "live.db"))

	c.Assert(err, qt.IsNotNil)
}

// TestSchemaTestEnvSrcWithoutAProjectIsStillRefused pins the boundary: the
// adapter selects a project only when -c or --env names one, so a bare run has
// no environment to read the attribute out of and keeps the refusal. Without
// this, resolving unconditionally would look correct on every test above.
func TestSchemaTestEnvSrcWithoutAProjectIsStillRefused(t *testing.T) {
	c := qt.New(t)

	baseDir := writeSchemaTestEnvProject(c)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "test", "-u", "env://src", filepath.Join(baseDir, "tests")})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `.*does not support env reference desired-state sources.*`)
}

// TestMigrateTestEnvMigrationDirIsRefusedAndRedundant records the half that
// stays refused, and why: the selected env's own migration directory is
// replayed without the flag, so --dir env://migration asks for what the run
// already does. The same reasoning settled --exclude in stokaro/ptah#1697.
func TestMigrateTestEnvMigrationDirIsRefusedAndRedundant(t *testing.T) {
	c := qt.New(t)

	baseDir := writeMigrateTestEnvProject(c)

	refused := runMigrateTest(c, baseDir, "--dir", "env://migration")
	withoutFlag := runMigrateTest(c, baseDir)

	c.Assert(refused, qt.ErrorMatches, `.*only local file:// migration directories are supported`)
	c.Assert(withoutFlag, qt.IsNil)
}

// writeMigrateTestEnvProject writes an env carrying a migration directory and
// a case that only passes if that directory was replayed: the INSERT needs the
// table the migration creates.
func writeMigrateTestEnvProject(c *qt.C) string {
	baseDir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(baseDir, "atlas.hcl"), []byte(`env "dev" {
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)
	migrations := filepath.Join(baseDir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrations, "20260101000000_init.sql"),
		[]byte("CREATE TABLE env_migrated (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	cases := filepath.Join(baseDir, "tests")
	c.Assert(os.MkdirAll(cases, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(cases, "case.yaml"), []byte(`cases:
  - name: the env's migration directory is replayed
    steps:
      - migrate_to: latest
      - exec: "INSERT INTO env_migrated (id) VALUES (1)"
`), 0o600), qt.IsNil)
	return baseDir
}

// runMigrateTest runs the compat verb against the project's case directory and
// returns its error.
//
// Each call gets its own dev database file. A shared `sqlite://dev?mode=memory`
// outlives one run inside the test binary, so the second replay would insert
// over the first run's row and fail on the primary key -- a collision that
// reads exactly like the flag having done nothing.
func runMigrateTest(c *qt.C, baseDir string, args ...string) error {
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(append([]string{
		"migrate", "test",
		"--config", "file://" + filepath.Join(baseDir, "atlas.hcl"),
		"--env", "dev",
		"--dev-url", "sqlite://" + filepath.Join(c.TempDir(), "dev.db"),
	}, append(args, filepath.Join(baseDir, "tests"))...))

	err := cmd.Execute()
	c.Logf("migrate test output:\n%s", out.String())
	return err
}

// TestSchemaTestEnvSrcNamingSeveralSourcesIsRefused holds the one place this
// verb parts company with --to, which merges a list-valued src. -u forwards to
// a single native --root-dir, so the second source has nowhere to go, and
// picking one would test a schema the environment does not describe.
func TestSchemaTestEnvSrcNamingSeveralSourcesIsRefused(t *testing.T) {
	c := qt.New(t)

	baseDir := writeSchemaTestEnvProject(c)
	second := filepath.Join(baseDir, "second.sql")
	c.Assert(os.WriteFile(second, []byte("CREATE TABLE env_second (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(baseDir, "atlas.hcl"), []byte(`env "dev" {
  src = ["file://desired.sql", "file://second.sql"]
}
`), 0o600), qt.IsNil)

	_, err := runSchemaTest(c, baseDir, "--env", "dev", "-u", "env://src")

	c.Assert(err, qt.ErrorMatches, `.*names 2 desired-state sources, and this flag forwards one.*`)
}
