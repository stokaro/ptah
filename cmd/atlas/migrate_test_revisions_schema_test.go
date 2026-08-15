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

	attached := filepath.Join(root, "attached.db")
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

// TestCompatCommand_MigrateTestRevisionsSchema is a discriminating pair rather
// than a single assertion: the same fixture must PASS with the flag and FAIL
// without it. Without the pair, a test runner that ignored --revisions-schema
// and wrote revisions everywhere would look correct.
//
// Reverted, the passing row does not merely fail its assertion — the run stops
// with `unknown flag: --revisions-schema`.
func TestCompatCommand_MigrateTestRevisionsSchema(t *testing.T) {
	tests := []struct {
		name     string
		flagArgs func() []string
		wantOut  string
		wantErr  func(c *qt.C, err error)
	}{
		{
			name:     "with the flag the revision table moves",
			flagArgs: func() []string { return []string{"--revisions-schema", "alt"} },
			wantOut:  "1 cases, 1 passed, 0 failed",
			wantErr: func(c *qt.C, err error) {
				c.Assert(err, qt.IsNil)
			},
		},
		{
			name:     "without the flag the same case fails",
			flagArgs: func() []string { return nil },
			wantOut:  "1 cases, 0 passed, 1 failed",
			wantErr: func(c *qt.C, err error) {
				c.Assert(err, qt.ErrorMatches, "migration tests failed")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			migrationsDir, casesDir := writeMigrateTestRevisionsFixture(c)
			args := append([]string{
				"migrate", "test",
				"--dir", "file://" + migrationsDir,
			}, test.flagArgs()...)
			args = append(args, casesDir)

			stdout, _, err := runCompatStreams(c, args...)

			test.wantErr(c, err)
			c.Assert(stdout, qt.Contains, test.wantOut)
		})
	}
}
