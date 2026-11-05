//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
)

// An Atlas-format directory runs the files its atlas.sum covers, the
// top-level ones spelled `.sql`, and no others. These tests hold every verb
// that reads such a directory, on both binaries, to one warning naming how
// many files that leaves out. SQLite is compiled in, so no server is needed.

// uncoveredAtlasDir writes an Atlas-format directory holding one covered
// migration, one in a subdirectory and one spelled `.SQL`, hashed as Atlas
// hashes it, and returns its path.
func uncoveredAtlasDir(c *qt.C) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(filepath.Join(dir, "sub"), 0o755), qt.IsNil)
	for name, sql := range map[string]string{
		"1_a.sql":     "CREATE TABLE a (id integer);\n",
		"sub/2_b.sql": "CREATE TABLE b (id integer);\n",
		"3_c.SQL":     "CREATE TABLE c (id integer);\n",
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, filepath.FromSlash(name)), []byte(sql), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

// uncoveredFilesWarning is the warning for the two files the directory holds
// outside atlas.sum, in the listing words of a run.
func uncoveredFilesWarning(listing string) string {
	return "warning: 2 SQL files are not covered by atlas.sum and will not run;" +
		" Atlas migrations are top-level files named *.sql; " + listing
}

// TestVerbsNameTheFilesAtlasSumLeavesOutE2E runs each verb on the directory:
// each exits 0 and writes the warning once. `migrations up`, `down` and
// `status` log through a logger of their own, so their listing names the
// flag beside the variable.
func TestVerbsNameTheFilesAtlasSumLeavesOutE2E(t *testing.T) {
	tests := []struct {
		name    string
		target  clirun.Target
		args    []string
		listing string
	}{
		{
			name: "migrations up", target: clirun.Ptah,
			args:    []string{"migrations", "up", "--migrations-dir", "{dir}", "--db-url", "{db}"},
			listing: "PTAH_LOG_LEVEL=debug or --log-level debug lists them",
		},
		{
			name: "migrations status", target: clirun.Ptah,
			args:    []string{"migrations", "status", "--migrations-dir", "{dir}", "--db-url", "{db}"},
			listing: "PTAH_LOG_LEVEL=debug or --log-level debug lists them",
		},
		{
			name: "migrations lint", target: clirun.Ptah,
			args:    []string{"migrations", "lint", "--dir", "{dir}", "--dev-url", "sqlite://file?mode=memory"},
			listing: "PTAH_LOG_LEVEL=debug lists them",
		},
		{
			name: "migrations checkpoint", target: clirun.Ptah,
			args: []string{
				"migrations", "checkpoint", "--migrations-dir", "{dir}", "--dir-format", "atlas", "--dry-run",
				"--shadow-db", "{db}",
			},
			listing: "PTAH_LOG_LEVEL=debug lists them",
		},
		{
			name: "migrations ls --verify-sum", target: clirun.Ptah,
			args:    []string{"migrations", "ls", "--migrations-dir", "{dir}", "--verify-sum"},
			listing: "PTAH_LOG_LEVEL=debug lists them",
		},
		{
			name: "ptah-compat migrate status", target: clirun.Compat,
			args:    []string{"migrate", "status", "--dir", "file://{dir}", "--url", "{db}"},
			listing: "PTAH_LOG_LEVEL=debug lists them",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := uncoveredAtlasDir(c)
			replacer := strings.NewReplacer(
				"{dir}", filepath.ToSlash(dir), "{db}", "sqlite://"+filepath.ToSlash(filepath.Join(c.TempDir(), "run.db")),
			)
			args := make([]string, 0, len(test.args))
			for _, arg := range test.args {
				args = append(args, replacer.Replace(arg))
			}

			got := clirun.Run(c, test.target, clirun.Options{Dir: c.TempDir()}, args...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(warningLines(got.Stderr), qt.DeepEquals, []string{uncoveredFilesWarning(test.listing)})
		})
	}
}

// TestMigrationsUpRunsOnlyTheCoveredFileE2E is what the warning says, read
// back from the database: `migrations up` creates the covered migration's
// table and neither of the others.
func TestMigrationsUpRunsOnlyTheCoveredFileE2E(t *testing.T) {
	c := qt.New(t)
	dir := uncoveredAtlasDir(c)
	db := filepath.Join(c.TempDir(), "run.db")

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"migrations", "up", "--migrations-dir", dir, "--db-url", "sqlite://"+filepath.ToSlash(db))

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(sqliteTableNames(c, "sqlite://"+filepath.ToSlash(db)), qt.DeepEquals, []string{"a", "schema_migrations", "schema_migrations_log"})
}

// TestMigrationsUpSaysNothingWhereNothingIsLeftOutE2E is the control: a Ptah
// directory with no atlas.sum runs its nested migration too, so nothing is
// left out and nothing is said.
func TestMigrationsUpSaysNothingWhereNothingIsLeftOutE2E(t *testing.T) {
	c := qt.New(t)
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(filepath.Join(dir, "sub"), 0o755), qt.IsNil)
	for name, sql := range map[string]string{
		"0000000001_a.up.sql":       "CREATE TABLE a (id integer);\n",
		"0000000001_a.down.sql":     "DROP TABLE a;\n",
		"sub/0000000002_b.up.sql":   "CREATE TABLE b (id integer);\n",
		"sub/0000000002_b.down.sql": "DROP TABLE b;\n",
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, filepath.FromSlash(name)), []byte(sql), 0o600), qt.IsNil)
	}
	db := filepath.Join(c.TempDir(), "run.db")

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"migrations", "up", "--migrations-dir", dir, "--db-url", "sqlite://"+filepath.ToSlash(db))

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(warningLines(got.Stderr), qt.HasLen, 0)
	c.Assert(sqliteTableNames(c, "sqlite://"+filepath.ToSlash(db)), qt.DeepEquals, []string{"a", "b", "schema_migrations", "schema_migrations_log"})
}
