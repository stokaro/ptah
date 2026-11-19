//go:build integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// These tests drive the shipped binary over SQLite and a migration directory
// that a run either reads whole or refuses, naming the file. A refusal comes
// before anything runs, so the database holds no table afterwards, the
// revision table included. Lint reports each such file under MF104P at error
// severity, with a dev database or without one.

// readsWholeDirectory writes files into the migrations directory under a new
// working directory and returns the working directory. A name may hold a
// subdirectory.
func readsWholeDirectory(c *qt.C, files map[string]string) string {
	c.Helper()
	dir := c.TempDir()
	for name, body := range files {
		target := filepath.Join(dir, "migrations", filepath.FromSlash(name))
		c.Assert(os.MkdirAll(filepath.Dir(target), 0o750), qt.IsNil)
		c.Assert(os.WriteFile(target, []byte(body), 0o600), qt.IsNil)
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
// MF104P, with the further args given, and returns the exit code.
func refusedByLint(c *qt.C, dir string, args ...string) ([]string, int) {
	c.Helper()
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
		append([]string{"migrations", "lint", "--dir", "migrations", "--format", "json"}, args...)...)
	var report struct {
		Findings []struct {
			Rule string `json:"rule"`
			File string `json:"file"`
		} `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(result.Stdout), &report), qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	files := make([]string, 0)
	for _, finding := range report.Findings {
		if finding.Rule == "MF104P" {
			files = append(files, finding.File)
		}
	}
	return files, result.ExitCode
}

// devLint is the argument that gives lint a dev database to replay on.
var devLint = []string{"--dev-url", "sqlite://dev.db"}

const (
	// repeatableRefusal is the refusal of R__r.sql in a directory read in the
	// Atlas format, over the revision table that records numbers.
	repeatableRefusal = `Atlas migration R__r.sql has version "R", which a revision table that records numbers cannot keep; ` +
		`use the Atlas revision table (--revision-format atlas), or Ptah's file names, under which R__<name>.sql is a ` +
		`repeatable migration; a directory of repeatable migrations alone is read in Ptah's format with --dir-format ptah`
	// detectionRefusal is the refusal of one file format detection, which
	// reads the directory as Atlas migrations, does not read.
	detectionRefusal = `migration file format detection does not read: %s. The directory reads as Atlas migrations, ` +
		`and Atlas runs every SQL file at the top level of the directory, so Ptah refuses the directory rather than ` +
		`leave this file unapplied; read the directory with --dir-format atlas, or move the file out of the directory`
	// ptahRefusal is the refusal of one file beside a Ptah pair.
	ptahRefusal = `migration file the Ptah format does not read: %s. Ptah reads a migration directory whole, so it ` +
		`refuses the directory rather than leave this file unapplied; name it NNNNNNNNNN_<name>.up.sql, ` +
		`NNNNNNNNNN_<name>.down.sql or R__<name>.sql, with .up, .down and .sql in lower case, or move it out of the directory`
)

// ptahPair is a Ptah pair with the further files given.
func ptahPair(extra map[string]string) map[string]string {
	files := map[string]string{
		"0000000001_a.up.sql":   "CREATE TABLE ta (id INTEGER);\n",
		"0000000001_a.down.sql": "DROP TABLE ta;\n",
	}
	maps.Copy(files, extra)
	return files
}

// In a directory read in the Atlas format, R__r.sql is the one-time version
// R, which the Ptah revision table cannot record. Without the refusal, up
// records R under the key above every numbered file, 2, reports the run
// partial and exits 0; status then reads R as pending and 2 as a migration
// with no file, and the next up exits 2. Lint reports the file, with a dev
// database or without one.
func TestMigrationDirectoryReadsWhole_AnAtlasRepeatableOverTheNumericTable_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := readsWholeDirectory(c, map[string]string{
		"1_a.sql":  "CREATE TABLE ta (id INTEGER);\n",
		"R__r.sql": "CREATE TABLE tr (id INTEGER);\n",
	})

	up := readsWholePtah(c, dir, "up")
	status := readsWholePtah(c, dir, "status")
	refused, lintExit := refusedByLint(c, dir)
	devRefused, devLintExit := refusedByLint(c, dir, devLint...)

	c.Assert(up.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(up.Stderr, qt.Contains, repeatableRefusal)
	c.Assert(status.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", status.Stdout, status.Stderr))
	c.Assert(status.Stderr, qt.Contains, repeatableRefusal)
	c.Assert(sqliteTables(c, filepath.Join(dir, "app.db")), qt.HasLen, 0)
	c.Assert(lintExit, qt.Equals, 1)
	c.Assert(refused, qt.DeepEquals, []string{"migrations/R__r.sql"})
	c.Assert(devLintExit, qt.Equals, 1)
	c.Assert(devRefused, qt.DeepEquals, []string{"migrations/R__r.sql"})
}

// The Atlas revision table records the version R, so the same directory
// applies there, R__r.sql last, and lint told the table reports nothing.
func TestMigrationDirectoryReadsWhole_AnAtlasRepeatableOverTheAtlasTable_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := readsWholeDirectory(c, map[string]string{
		"1_a.sql":  "CREATE TABLE ta (id INTEGER);\n",
		"R__r.sql": "CREATE TABLE tr (id INTEGER);\n",
	})

	up := readsWholePtah(c, dir, "up", "--revision-format", "atlas")
	refused, lintExit := refusedByLint(c, dir, "--revision-format", "atlas")
	devRefused, devLintExit := refusedByLint(c, dir, append([]string{"--revision-format", "atlas"}, devLint...)...)

	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"1", "R"})
	c.Assert(lintExit, qt.Equals, 0)
	c.Assert(refused, qt.HasLen, 0)
	c.Assert(devLintExit, qt.Equals, 0)
	c.Assert(devRefused, qt.HasLen, 0)
}

// A file the reading leaves out at the top of the directory refuses every
// command that reads the directory to run it, and lint reports the file
// under MF104P at error severity, so it exits 1, with a dev database or
// without one. Without the refusal, up applies the files the reading
// selected, exits 0 and never runs the file, status counts no pending
// migration, and lint warns under MF103 and exits 0. A Ptah name is
// case-sensitive and needs a name after its version.
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
			unread:  "v2_b.sql",
		},
		{
			name: "an uppercase extension format detection does not read",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE ta (id INTEGER);\n",
				"3_c.SQL": "CREATE TABLE tc (id INTEGER);\n",
			},
			refusal: detectionRefusal,
			unread:  "3_c.SQL",
		},
		{
			name:    "a file beside a Ptah pair",
			files:   ptahPair(map[string]string{"notes.sql": "CREATE TABLE notes (id INTEGER);\n"}),
			refusal: ptahRefusal,
			unread:  "notes.sql",
		},
		{
			name:    "an uppercase direction beside a Ptah pair",
			files:   ptahPair(map[string]string{"0000000002_b.UP.sql": "CREATE TABLE tb (id INTEGER);\n"}),
			refusal: ptahRefusal,
			unread:  "0000000002_b.UP.sql",
		},
		{
			name:    "a version with no name beside a Ptah pair",
			files:   ptahPair(map[string]string{"0000000002.up.sql": "CREATE TABLE tb (id INTEGER);\n"}),
			refusal: ptahRefusal,
			unread:  "0000000002.up.sql",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := readsWholeDirectory(c, test.files)

			up := readsWholePtah(c, dir, "up")
			status := readsWholePtah(c, dir, "status")
			refused, lintExit := refusedByLint(c, dir)
			devRefused, devLintExit := refusedByLint(c, dir, devLint...)

			refusal := fmt.Sprintf(test.refusal, test.unread)
			c.Assert(up.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
			c.Assert(up.Stderr, qt.Contains, refusal)
			c.Assert(status.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", status.Stdout, status.Stderr))
			c.Assert(status.Stderr, qt.Contains, refusal)
			c.Assert(sqliteTables(c, filepath.Join(dir, "app.db")), qt.HasLen, 0)
			c.Assert(lintExit, qt.Equals, 1)
			c.Assert(refused, qt.DeepEquals, []string{"migrations/" + test.unread})
			c.Assert(devLintExit, qt.Equals, 1)
			c.Assert(devRefused, qt.DeepEquals, []string{"migrations/" + test.unread})
		})
	}
}

// A file at the top of the directory that is not SQL, and a SQL file one
// level down, which belongs to another directory, refuse nothing: the pair
// applies and lint reports no file.
func TestMigrationDirectoryReadsWhole_FilesOfNoMigration_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := readsWholeDirectory(c, ptahPair(map[string]string{
		"README.md":         "# Migrations\n",
		"shared/notes.sql":  "CREATE TABLE notes (id INTEGER);\n",
		"shared/README.txt": "templates\n",
	}))

	up := readsWholePtah(c, dir, "up")
	refused, lintExit := refusedByLint(c, dir)

	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(sqliteTables(c, filepath.Join(dir, "app.db")), qt.Contains, "ta")
	c.Assert(lintExit, qt.Equals, 0)
	c.Assert(refused, qt.HasLen, 0)
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

// ptah-compat reads a directory atlas.sum covers in the Atlas format, which
// runs every SQL file at the top level, so notes.sql beside a Ptah pair is a
// pending file there rather than a refusal.
func TestMigrationDirectoryReadsWhole_TheCompatibilitySurfaceReadsEveryFile_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := readsWholeDirectory(c, ptahPair(map[string]string{"notes.sql": "CREATE TABLE notes (id INTEGER);\n"}))
	compat := clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}

	hash := clirun.Run(c, clirun.Compat, compat, "migrate", "hash", "--dir", "file://migrations")
	status := clirun.Run(c, clirun.Compat, compat, "migrate", "status", "--dir", "file://migrations", "--url", "sqlite://app.db")

	c.Assert(hash.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", hash.Stdout, hash.Stderr))
	c.Assert(status.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", status.Stdout, status.Stderr))
	c.Assert(status.Stdout, qt.Contains, "Pending Files:   2")
}
