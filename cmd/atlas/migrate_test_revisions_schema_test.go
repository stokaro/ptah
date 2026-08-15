package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// writeMigrateTestRevisionsFixture writes a hashed one-migration Atlas directory plus a
// test case that proves WHERE the revision table landed.
//
// The case attaches a second SQLite database under the name the run is asked to
// put revisions in, so "a named schema" is expressible on the ephemeral SQLite
// database the test runner provisions, with no server involved. It then asserts
// both halves: the revision row is in the named schema, and the connection's
// default schema has no revision table at all. Asserting only the first half
// would pass on an implementation that wrote revisions to both.
func writeMigrateTestRevisionsFixture(c *qt.C) (migrationsDir, casesDir string) {
	c.Helper()
	root := c.TempDir()
	migrationsDir = filepath.Join(root, "migrations")
	casesDir = filepath.Join(root, "tests")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.MkdirAll(casesDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "20240101000001_init.sql"),
		[]byte("CREATE TABLE rs_users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)

	// Slashed: this path is interpolated into a double-quoted YAML scalar,
	// where a Windows separator makes \U an escape sequence and the whole
	// test-case file fails to parse. SQLite reads either separator.
	attached := filepath.ToSlash(filepath.Join(root, "attached.db"))
	cases := `cases:
  - name: revisions land in the named schema
    steps:
      - name: attach the schema
        exec: "ATTACH DATABASE '` + attached + `' AS alt"
      - name: migrate to latest
        migrate_to: latest
      - name: the revision row is in alt
        assert:
          query: "SELECT count(*) FROM alt.schema_migrations"
          scalar: "1"
      - name: the default schema has no revision table
        assert:
          query: "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
          scalar: "0"
`
	c.Assert(os.WriteFile(filepath.Join(casesDir, "revisions.yaml"), []byte(cases), 0o600), qt.IsNil)
	return migrationsDir, casesDir
}

// runMigrateTestRevisionsFixture runs `migrate test` over the fixture above with
// whatever flags the caller adds between the directory and the cases path.
func runMigrateTestRevisionsFixture(c *qt.C, flags ...string) (stdout string, err error) {
	c.Helper()
	migrationsDir, casesDir := writeMigrateTestRevisionsFixture(c)
	args := append([]string{"migrate", "test", "--dir", "file://" + migrationsDir}, flags...)
	args = append(args, casesDir)
	stdout, _, err = runCompatStreams(c, args...)
	return stdout, err
}

// The two tests below are a discriminating pair rather than a single assertion:
// the same fixture must PASS with --revisions-schema and FAIL without it.
// Without the pair, a test runner that ignored the flag and wrote revisions
// everywhere would look correct.

// TestCompatCommand_MigrateTestRevisionsSchema_HappyPath is the half the flag
// exists for. With the mapping reverted -- `migrate test` passing an empty
// revisions schema to the runner, so revisions land in the connection default
// -- this one reddens with "migration tests failed" while the failure path below
// stays green, which is the direction that says the flag is what moved them.
func TestCompatCommand_MigrateTestRevisionsSchema_HappyPath(t *testing.T) {
	c := qt.New(t)

	stdout, err := runMigrateTestRevisionsFixture(c, "--revisions-schema", "alt")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "1 cases, 1 passed, 0 failed")
}

// TestCompatCommand_MigrateTestRevisionsSchema_FailurePath is the half that
// keeps the happy path from being vacuous: the same case, run without the flag,
// has to find no revision row in the attached schema and fail.
func TestCompatCommand_MigrateTestRevisionsSchema_FailurePath(t *testing.T) {
	c := qt.New(t)

	stdout, err := runMigrateTestRevisionsFixture(c)

	c.Assert(err, qt.ErrorMatches, "migration tests failed")
	c.Assert(stdout, qt.Contains, "1 cases, 0 passed, 1 failed")
}
