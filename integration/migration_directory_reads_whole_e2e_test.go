//go:build integration

package integration_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// These tests drive the shipped binary over SQLite and a migration directory
// that a run either reads whole or refuses, naming the file. A refusal comes
// before anything runs, so the database holds no table afterwards, the
// revision table included.

// readsWholeDirectory writes files into the migrations directory under a new
// working directory and returns the working directory.
func readsWholeDirectory(c *qt.C, files map[string]string) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.Mkdir(filepath.Join(dir, "migrations"), 0o750), qt.IsNil)
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, "migrations", name), []byte(body), 0o600), qt.IsNil)
	}
	return dir
}

// readsWholePtah runs `ptah migrations <verb>` in dir against app.db there.
func readsWholePtah(c *qt.C, dir, verb string, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
		append([]string{"migrations", verb, "--db-url", "sqlite://app.db", "--migrations-dir", "migrations"}, args...)...)
}

// refusedByLint names the files `migrations lint --format json` reports under
// MF105P, and returns the exit code.
func refusedByLint(c *qt.C, dir string) ([]string, int) {
	c.Helper()
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
		"migrations", "lint", "--dir", "migrations", "--format", "json")
	var report struct {
		Findings []struct {
			Rule string `json:"rule"`
			File string `json:"file"`
		} `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(result.Stdout), &report), qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	files := make([]string, 0)
	for _, finding := range report.Findings {
		if finding.Rule == "MF105P" {
			files = append(files, finding.File)
		}
	}
	return files, result.ExitCode
}

const (
	// repeatableRefusal is the refusal of R__r.sql in a directory read in the
	// Atlas format, over the revision table that records numbers.
	repeatableRefusal = `Atlas migration R__r.sql has version "R", which a revision table that records numbers cannot keep; ` +
		`use the Atlas revision table (--revision-format atlas), or Ptah's file names, under which R__<name>.sql is a ` +
		`repeatable migration`
	// detectionRefusal is the refusal of v2_b.sql beside 1_a.sql, which format
	// detection reads as Atlas migrations.
	detectionRefusal = `migration files format detection does not read: v2_b.sql. The directory reads as Atlas migrations, ` +
		`and Atlas runs every SQL file at the top level of the directory, so Ptah refuses the directory rather than ` +
		`leave them unapplied; read it with --dir-format atlas, or move them out of the directory`
	// ptahRefusal is the refusal of notes.sql beside a Ptah pair.
	ptahRefusal = `migration files the Ptah format does not read: notes.sql. Ptah reads a migration directory whole, so ` +
		`it refuses the directory rather than leave them unapplied; name each NNNNNNNNNN_<name>.up.sql, ` +
		`NNNNNNNNNN_<name>.down.sql or R__<name>.sql, or move it out of the directory`
)

// In a directory read in the Atlas format, R__r.sql is the one-time version
// R, which the Ptah revision table cannot record. Without the refusal, up
// records R under the key above every numbered file, 2, reports the run
// partial and exits 0; status then reads R as pending and 2 as a migration
// with no file, and the next up exits 2. Lint reads the directory as the
// Atlas format does and finds no file the migrator leaves out.
func TestMigrationDirectoryReadsWhole_AnAtlasRepeatableOverTheNumericTable_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := readsWholeDirectory(c, map[string]string{
		"1_a.sql":  "CREATE TABLE ta (id INTEGER);\n",
		"R__r.sql": "CREATE TABLE tr (id INTEGER);\n",
	})

	up := readsWholePtah(c, dir, "up")
	status := readsWholePtah(c, dir, "status")
	refused, lintExit := refusedByLint(c, dir)

	c.Assert(up.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(up.Stderr, qt.Contains, repeatableRefusal)
	c.Assert(status.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", status.Stdout, status.Stderr))
	c.Assert(status.Stderr, qt.Contains, repeatableRefusal)
	c.Assert(sqliteTables(c, filepath.Join(dir, "app.db")), qt.HasLen, 0)
	c.Assert(lintExit, qt.Equals, 0)
	c.Assert(refused, qt.HasLen, 0)
}

// The Atlas revision table records the version R, so the same directory
// applies there, R__r.sql last.
func TestMigrationDirectoryReadsWhole_AnAtlasRepeatableOverTheAtlasTable_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := readsWholeDirectory(c, map[string]string{
		"1_a.sql":  "CREATE TABLE ta (id INTEGER);\n",
		"R__r.sql": "CREATE TABLE tr (id INTEGER);\n",
	})

	up := readsWholePtah(c, dir, "up", "--revision-format", "atlas")

	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"1", "R"})
}

// A file the reading leaves out at the top of the directory refuses every
// command that reads the directory to run it, and lint reports the file
// under MF105P at error severity, so it exits 1. Without the refusal, up
// applies the files the reading selected, exits 0 and never runs the file,
// status counts no pending migration, and lint warns under MF103 and exits 0.
func TestMigrationDirectoryReadsWhole_AFileTheReadingLeavesOut_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		files   map[string]string
		refusal string
		unread  string
	}{
		{
			name: "a version format detection does not read",
			files: map[string]string{
				"1_a.sql":  "CREATE TABLE ta (id INTEGER);\n",
				"v2_b.sql": "CREATE TABLE tb (id INTEGER);\n",
			},
			refusal: detectionRefusal,
			unread:  "migrations/v2_b.sql",
		},
		{
			name: "a file beside a Ptah pair",
			files: map[string]string{
				"0000000001_a.up.sql":   "CREATE TABLE ta (id INTEGER);\n",
				"0000000001_a.down.sql": "DROP TABLE ta;\n",
				"notes.sql":             "CREATE TABLE notes (id INTEGER);\n",
			},
			refusal: ptahRefusal,
			unread:  "migrations/notes.sql",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := readsWholeDirectory(c, test.files)

			up := readsWholePtah(c, dir, "up")
			status := readsWholePtah(c, dir, "status")
			refused, lintExit := refusedByLint(c, dir)

			c.Assert(up.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
			c.Assert(up.Stderr, qt.Contains, test.refusal)
			c.Assert(status.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", status.Stdout, status.Stderr))
			c.Assert(status.Stderr, qt.Contains, test.refusal)
			c.Assert(sqliteTables(c, filepath.Join(dir, "app.db")), qt.HasLen, 0)
			c.Assert(lintExit, qt.Equals, 1)
			c.Assert(refused, qt.DeepEquals, []string{test.unread})
		})
	}
}

// Read in the Atlas format by name, v2_b.sql is the version v2, and the
// Atlas revision table records it.
func TestMigrationDirectoryReadsWhole_AVersionTheAtlasFormatReads_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := readsWholeDirectory(c, map[string]string{
		"1_a.sql":  "CREATE TABLE ta (id INTEGER);\n",
		"v2_b.sql": "CREATE TABLE tb (id INTEGER);\n",
	})

	up := readsWholePtah(c, dir, "up", "--dir-format", "atlas", "--revision-format", "atlas")

	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"1", "v2"})
}
