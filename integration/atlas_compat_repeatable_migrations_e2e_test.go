//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/exeext"
)

// The directory of these tests holds two versioned files and R__view.sql in
// the Atlas layout. The community binary reads the third as the one-time
// version R; PTAH_ATLAS_REPEATABLE_MIGRATIONS reads it as a repeatable
// migration whose record sits beside atlas_schema_revisions.

const (
	repeatableInitSQL = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);\n"
	repeatableMoreSQL = "CREATE TABLE u (id INTEGER PRIMARY KEY);\n"
	// repeatableViewSQL and repeatableViewChangedSQL differ in the columns
	// of the view, which is what the database is read back for.
	repeatableViewSQL        = "DROP VIEW IF EXISTS v;\nCREATE VIEW v AS SELECT id, name FROM t;\n"
	repeatableViewChangedSQL = "DROP VIEW IF EXISTS v;\nCREATE VIEW v AS SELECT id FROM t;\n"
)

// repeatableEngine is one engine the directory is applied to.
type repeatableEngine struct {
	name string
	url  func(c *qt.C) string
	// revisions and record are the tables an Atlas apply writes on it, as
	// SQL names them; quotedRevisions is the revision table as a refusal
	// quotes it.
	revisions       string
	quotedRevisions string
	record          string
	// recordTables lists the record table when it exists.
	recordTables string
}

var repeatableEngines = []repeatableEngine{
	{
		name: "sqlite",
		// The authority form, which the community binary reads too; the
		// opaque form atlasurl.SQLiteURLFromPath writes is Ptah's alone.
		url: func(c *qt.C) string {
			return "sqlite://" + filepath.ToSlash(filepath.Join(c.TempDir(), "app.db"))
		},
		revisions:       "atlas_schema_revisions",
		quotedRevisions: `"atlas_schema_revisions"`,
		record:          "atlas_schema_revisions_repeatable",
		recordTables:    "SELECT name FROM sqlite_master WHERE name = 'atlas_schema_revisions_repeatable'",
	},
	{
		name:            "postgres",
		url:             func(c *qt.C) string { return postgresScratchDevURL(c, "ptah_compat_repeatable") },
		revisions:       "atlas_schema_revisions.atlas_schema_revisions",
		quotedRevisions: `"atlas_schema_revisions"."atlas_schema_revisions"`,
		record:          "atlas_schema_revisions.atlas_schema_revisions_repeatable",
		recordTables: "SELECT table_name FROM information_schema.tables " +
			"WHERE table_name = 'atlas_schema_revisions_repeatable'",
	},
}

// repeatableEnabled is the environment that turns the reading on.
var repeatableEnabled = []string{"PTAH_ATLAS_REPEATABLE_MIGRATIONS=1"}

// writeRepeatableDir writes the directory with view as the body of
// R__view.sql into dir and hashes it with binary.
func writeRepeatableDir(c *qt.C, binary, dir, view string) {
	c.Helper()
	writeHashedDir(c, binary, dir, map[string]string{
		"1_init.sql":  repeatableInitSQL,
		"2_more.sql":  repeatableMoreSQL,
		"R__view.sql": view,
	})
}

// writeHashedDir writes files into dir/migrations, removes every other .sql
// file there, and hashes the directory with binary.
func writeHashedDir(c *qt.C, binary, dir string, files map[string]string) {
	c.Helper()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
	existing, err := filepath.Glob(filepath.Join(migrations, "*.sql"))
	c.Assert(err, qt.IsNil)
	for _, path := range existing {
		c.Assert(os.Remove(path), qt.IsNil)
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(migrations, name), []byte(body), 0o600), qt.IsNil)
	}
	hashed := runFormatTemplateVerb(c, binary, dir, nil, "migrate", "hash", "--dir", "file://migrations")
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", hashed.Stderr))
}

// repeatableNote is the line migrate diff and migrate lint print on stderr
// while the variable is set over the directory.
func repeatableNote(verb string) string {
	return "note: PTAH_ATLAS_REPEATABLE_MIGRATIONS is set, but migrate " + verb +
		" reads R__view.sql as the one-time version R; only migrate apply and migrate status read them as " +
		"repeatable migrations\n"
}

// applyRepeatableDir runs migrate apply of the directory in dir on url.
func applyRepeatableDir(c *qt.C, binary, dir, url string, env []string, args ...string) clirun.Result {
	c.Helper()
	return runFormatTemplateVerb(c, binary, dir, env,
		append([]string{"migrate", "apply", "--dir", "file://migrations", "--url", url}, args...)...)
}

// repeatableStrings reads the one text column query answers, row by row.
func repeatableStrings(c *qt.C, url, query string) []string {
	c.Helper()
	conn := connectDevDialect(c, url)
	rows, err := conn.QueryContext(c.Context(), query)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", query))
	defer rows.Close()
	values := make([]string, 0)
	for rows.Next() {
		var value string
		c.Assert(rows.Scan(&value), qt.IsNil)
		values = append(values, value)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return values
}

// viewColumns is the columns view v has on url.
func viewColumns(c *qt.C, url string) []string {
	c.Helper()
	conn := connectDevDialect(c, url)
	rows, err := conn.QueryContext(c.Context(), "SELECT * FROM v")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	columns, err := rows.Columns()
	c.Assert(err, qt.IsNil)
	c.Assert(rows.Err(), qt.IsNil)
	return columns
}

// repeatableDigest is the digest the record keeps for body, computed here
// from the bytes.
func repeatableDigest(body string) string {
	sum := sha256.Sum256([]byte(body))
	return "h1:" + base64.StdEncoding.EncodeToString(sum[:])
}

// applyDurations matches the durations the apply report prints, which no two
// runs share.
var applyDurations = regexp.MustCompile(`[0-9.]+(ns|µs|ms|s)`)

// repeatableOffReport is the default apply report both binaries print for the
// directory, durations aside.
const repeatableOffReport = `Migrating to version R (3 migrations in total):

  -- migrating version 1
    -> CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);
  -- ok (D)

  -- migrating version 2
    -> CREATE TABLE u (id INTEGER PRIMARY KEY);
  -- ok (D)

  -- migrating version R
    -> DROP VIEW IF EXISTS v;
    -> CREATE VIEW v AS SELECT id, name FROM t;
  -- ok (D)

  -------------------------
  -- D
  -- 3 migrations
  -- 4 sql statements
`

// repeatableOffStatus is the default status both binaries print after it.
const repeatableOffStatus = `Migration Status: OK
  -- Current Version: R
  -- Next Version:    Already at latest version
  -- Executed Files:  3
  -- Pending Files:   0
`

// TestCompatRepeatableFileWithoutTheVariableIsVersionRE2E is the shipped
// compat binary with the variable unset: R__view.sql is the version R, the
// reports are the community binary's, and no record table is created. It is
// also the control for the refusal of a record the reading ignores: with no
// record table the run pays one catalog lookup and reports what the community
// binary reports.
func TestCompatRepeatableFileWithoutTheVariableIsVersionRE2E(t *testing.T) {
	for _, engine := range repeatableEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			binary := clirun.Build(c, clirun.Compat)
			dir := c.TempDir()
			url := engine.url(c)
			writeRepeatableDir(c, binary, dir, repeatableViewSQL)

			applied := applyRepeatableDir(c, binary, dir, url, nil)
			status := runFormatTemplateVerb(c, binary, dir, nil,
				"migrate", "status", "--dir", "file://migrations", "--url", url)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))
			c.Assert(applyDurations.ReplaceAllString(applied.Stdout, "D"), qt.Equals, repeatableOffReport)
			c.Assert(status.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", status.Stderr))
			c.Assert(status.Stdout, qt.Equals, repeatableOffStatus)
			c.Assert(repeatableStrings(c, url, "SELECT version FROM "+engine.revisions+" ORDER BY version"),
				qt.DeepEquals, []string{"1", "2", "R"})
			c.Assert(repeatableStrings(c, url,
				"SELECT CAST(COUNT(*) AS TEXT) FROM "+engine.revisions+" WHERE version = 'R'"),
				qt.DeepEquals, []string{"1"})
			c.Assert(repeatableStrings(c, url, engine.recordTables), qt.HasLen, 0)
		})
	}
}

// TestReferenceRepeatableFileIsVersionRE2E takes the same measurement against
// the pinned community binary: its apply report and its status report are the
// two texts the compat test above compares with.
func TestReferenceRepeatableFileIsVersionRE2E(t *testing.T) {
	for _, engine := range repeatableEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			binary := requireEngineReference(t)
			dir := c.TempDir()
			url := engine.url(c)
			writeRepeatableDir(c, binary, dir, repeatableViewSQL)

			applied := applyRepeatableDir(c, binary, dir, url, nil)
			status := runFormatTemplateVerb(c, binary, dir, nil,
				"migrate", "status", "--dir", "file://migrations", "--url", url)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))
			c.Assert(applyDurations.ReplaceAllString(applied.Stdout, "D"), qt.Equals, repeatableOffReport)
			c.Assert(status.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", status.Stderr))
			c.Assert(status.Stdout, qt.Equals, repeatableOffStatus)
			c.Assert(repeatableStrings(c, url, "SELECT version FROM "+engine.revisions+" ORDER BY version"),
				qt.DeepEquals, []string{"1", "2", "R"})
		})
	}
}

// TestCompatRepeatableMigrationsRunOnChangeE2E is the shipped compat binary
// with the variable set: R__view.sql runs after the versioned files, is left
// alone while it is unchanged, and runs again once its content changes. The
// record and the view are read back from the database.
func TestCompatRepeatableMigrationsRunOnChangeE2E(t *testing.T) {
	for _, engine := range repeatableEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			binary := clirun.Build(c, clirun.Compat)
			dir := c.TempDir()
			url := engine.url(c)
			recordQuery := "SELECT identity || ' ' || checksum || ' ' || state FROM " + engine.record
			writeRepeatableDir(c, binary, dir, repeatableViewSQL)

			first := applyRepeatableDir(c, binary, dir, url, repeatableEnabled)
			firstColumns := viewColumns(c, url)
			unchanged := applyRepeatableDir(c, binary, dir, url, repeatableEnabled)
			unchangedRecord := repeatableStrings(c, url, recordQuery)
			writeRepeatableDir(c, binary, dir, repeatableViewChangedSQL)
			changed := applyRepeatableDir(c, binary, dir, url, repeatableEnabled)

			c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", first.Stderr))
			c.Assert(applyDurations.ReplaceAllString(first.Stdout, "D"), qt.Matches,
				`(?s)Migrating to version 2 \(2 migrations in total\):.*-- 2 migrations\n  -- 2 sql statements\n\n`+
					`Repeatable Migrations: 1\n  view new `+regexp.QuoteMeta(repeatableDigest(repeatableViewSQL))+` applied\n`)
			c.Assert(firstColumns, qt.DeepEquals, []string{"id", "name"})
			c.Assert(unchanged.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", unchanged.Stderr))
			c.Assert(unchanged.Stdout, qt.Equals, "No migration files to execute\n")
			c.Assert(unchangedRecord, qt.DeepEquals, []string{"view " + repeatableDigest(repeatableViewSQL) + " applied"})
			c.Assert(changed.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", changed.Stderr))
			c.Assert(changed.Stdout, qt.Equals, "No migration files to execute\n\nRepeatable Migrations: 1\n  view changed "+
				repeatableDigest(repeatableViewChangedSQL)+" applied\n")
			c.Assert(viewColumns(c, url), qt.DeepEquals, []string{"id"})
			c.Assert(repeatableStrings(c, url, recordQuery), qt.DeepEquals,
				[]string{"view " + repeatableDigest(repeatableViewChangedSQL) + " applied"})
			c.Assert(repeatableStrings(c, url, "SELECT version FROM "+engine.revisions+" ORDER BY version"),
				qt.DeepEquals, []string{"1", "2"})
		})
	}
}

// TestCompatRepeatableMigrationsHashMatchesTheAtlasReadingE2E pins atlas.sum:
// the variable does not change what migrate hash writes.
func TestCompatRepeatableMigrationsHashMatchesTheAtlasReadingE2E(t *testing.T) {
	c := qt.New(t)
	binary := clirun.Build(c, clirun.Compat)
	off := c.TempDir()
	on := c.TempDir()
	writeRepeatableDir(c, binary, off, repeatableViewSQL)
	writeRepeatableDir(c, binary, on, repeatableViewSQL)

	rehashed := runFormatTemplateVerb(c, binary, on, repeatableEnabled, "migrate", "hash", "--dir", "file://migrations")

	c.Assert(rehashed.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", rehashed.Stderr))
	offSum, err := os.ReadFile(filepath.Join(off, "migrations", "atlas.sum"))
	c.Assert(err, qt.IsNil)
	onSum, err := os.ReadFile(filepath.Join(on, "migrations", "atlas.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(onSum), qt.Equals, string(offSum))
}

// TestCompatRepeatableMigrationsRefuseE2E covers what the variable refuses
// before anything changes.
func TestCompatRepeatableMigrationsRefuseE2E(t *testing.T) {
	t.Run("a revision table that records version R", func(t *testing.T) {
		for _, engine := range repeatableEngines {
			t.Run(engine.name, func(t *testing.T) {
				c := qt.New(t)
				binary := clirun.Build(c, clirun.Compat)
				dir := c.TempDir()
				url := engine.url(c)
				writeRepeatableDir(c, binary, dir, repeatableViewSQL)
				off := applyRepeatableDir(c, binary, dir, url, nil)
				c.Assert(off.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", off.Stderr))

				got := applyRepeatableDir(c, binary, dir, url, repeatableEnabled)

				c.Assert(got.ExitCode, qt.Equals, 1)
				c.Assert(got.Stderr, qt.Matches, `(?s)Error: .*the revision table `+regexp.QuoteMeta(engine.quotedRevisions)+
					` records version R \(description "_view"\), .*R__view.sql .*`+
					`delete the row with version 'R' from `+regexp.QuoteMeta(engine.quotedRevisions)+` .*`)
				c.Assert(repeatableStrings(c, url, "SELECT version FROM "+engine.revisions+" ORDER BY version"),
					qt.DeepEquals, []string{"1", "2", "R"})
			})
		}
	})

	// The statement the repeatable page gives for each engine clears the
	// refusal above.
	t.Run("the documented recovery", func(t *testing.T) {
		for _, engine := range repeatableEngines {
			t.Run(engine.name, func(t *testing.T) {
				c := qt.New(t)
				binary := clirun.Build(c, clirun.Compat)
				dir := c.TempDir()
				url := engine.url(c)
				writeRepeatableDir(c, binary, dir, repeatableViewSQL)
				off := applyRepeatableDir(c, binary, dir, url, nil)
				c.Assert(off.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", off.Stderr))
				_, err := connectDevDialect(c, url).ExecContext(c.Context(),
					"DELETE FROM "+engine.revisions+" WHERE version = 'R'")
				c.Assert(err, qt.IsNil)

				got := applyRepeatableDir(c, binary, dir, url, repeatableEnabled)

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
				c.Assert(repeatableStrings(c, url, "SELECT version FROM "+engine.revisions+" ORDER BY version"),
					qt.DeepEquals, []string{"1", "2"})
				c.Assert(repeatableStrings(c, url, "SELECT identity || ' ' || state FROM "+engine.record),
					qt.DeepEquals, []string{"view applied"})
			})
		}
	})

	t.Run("a record the Atlas reading would ignore", func(t *testing.T) {
		for _, engine := range repeatableEngines {
			t.Run(engine.name, func(t *testing.T) {
				c := qt.New(t)
				binary := clirun.Build(c, clirun.Compat)
				dir := c.TempDir()
				url := engine.url(c)
				writeRepeatableDir(c, binary, dir, repeatableViewSQL)
				on := applyRepeatableDir(c, binary, dir, url, repeatableEnabled)
				c.Assert(on.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", on.Stderr))
				writeRepeatableDir(c, binary, dir, repeatableViewChangedSQL)

				applied := applyRepeatableDir(c, binary, dir, url, nil)
				status := runFormatTemplateVerb(c, binary, dir, nil,
					"migrate", "status", "--dir", "file://migrations", "--url", url)

				want := `(?s)Error: .*this database records repeatable migrations view in .*` +
					`which this run does not read: .*set PTAH_ATLAS_REPEATABLE_MIGRATIONS=1\n`
				c.Assert(applied.ExitCode, qt.Equals, 1)
				c.Assert(applied.Stdout, qt.Equals, "")
				c.Assert(applied.Stderr, qt.Matches, want)
				c.Assert(status.ExitCode, qt.Equals, 1)
				c.Assert(status.Stdout, qt.Equals, "")
				c.Assert(status.Stderr, qt.Matches, want)
				c.Assert(repeatableStrings(c, url, "SELECT version FROM "+engine.revisions+" ORDER BY version"),
					qt.DeepEquals, []string{"1", "2"})
				c.Assert(viewColumns(c, url), qt.DeepEquals, []string{"id", "name"})
			})
		}
	})

	t.Run("a malformed value on every verb that reads it", func(t *testing.T) {
		for _, verb := range [][]string{
			{"migrate", "apply", "--dir", "file://migrations"},
			{"migrate", "status", "--dir", "file://migrations"},
			{"migrate", "down", "--dir", "file://migrations", "--dev-url", "sqlite://dev?mode=memory"},
		} {
			t.Run(verb[1], func(t *testing.T) {
				c := qt.New(t)
				binary := clirun.Build(c, clirun.Compat)
				dir := c.TempDir()
				dbPath := filepath.Join(dir, "app.db")
				writeRepeatableDir(c, binary, dir, repeatableViewSQL)

				got := runFormatTemplateVerb(c, binary, dir, []string{"PTAH_ATLAS_REPEATABLE_MIGRATIONS=yes"},
					append(verb, "--url", atlasurl.SQLiteURLFromPath(dbPath))...)

				c.Assert(got.ExitCode, qt.Equals, 1)
				c.Assert(got.Stderr, qt.Contains, `invalid boolean value "yes" for PTAH_ATLAS_REPEATABLE_MIGRATIONS`)
				_, statErr := os.Stat(dbPath)
				c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
			})
		}
	})

	t.Run("strict compatibility", func(t *testing.T) {
		c := qt.New(t)
		binary := clirun.Build(c, clirun.Compat)
		dir := c.TempDir()
		dbPath := filepath.Join(dir, "app.db")
		writeRepeatableDir(c, binary, dir, repeatableViewSQL)

		got := applyRepeatableDir(c, binary, dir, atlasurl.SQLiteURLFromPath(dbPath),
			append([]string{"PTAH_ATLAS_STRICT_COMPAT=1"}, repeatableEnabled...))

		c.Assert(got.ExitCode, qt.Equals, 1)
		c.Assert(got.Stderr, qt.Contains, "PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_ATLAS_REPEATABLE_MIGRATIONS")
		_, statErr := os.Stat(dbPath)
		c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
	})

	t.Run("a rollback over a directory that holds an R__ file", func(t *testing.T) {
		c := qt.New(t)
		binary := clirun.Build(c, clirun.Compat)
		dir := c.TempDir()
		url := atlasurl.SQLiteURLFromPath(filepath.Join(dir, "app.db"))
		writeRollbackDir(c, binary, dir, repeatableViewFile)
		applied := applyRepeatableDir(c, binary, dir, url, repeatableEnabled)
		c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))

		got := runFormatTemplateVerb(c, binary, dir, repeatableEnabled,
			"migrate", "down", "--dir", "file://migrations", "--url", url, "--dev-url", "sqlite://dev?mode=memory")

		c.Assert(got.ExitCode, qt.Equals, 1)
		c.Assert(got.Stderr, qt.Contains,
			"PTAH_ATLAS_REPEATABLE_MIGRATIONS is set and the directory holds R__view.sql, which migrate down reads")
		c.Assert(repeatableStrings(c, url, "SELECT version FROM atlas_schema_revisions ORDER BY version"),
			qt.DeepEquals, []string{"1", "2"})
		c.Assert(repeatableStrings(c, url, "SELECT identity FROM atlas_schema_revisions_repeatable"),
			qt.DeepEquals, []string{"view"})
		c.Assert(viewColumns(c, url), qt.DeepEquals, []string{"id", "name"})
	})

	for _, tc := range []struct {
		name string
		env  []string
	}{
		{name: "a rollback over a record, variable unset"},
		{name: "a rollback over a record, variable set", env: repeatableEnabled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			binary := clirun.Build(c, clirun.Compat)
			dir := c.TempDir()
			url := atlasurl.SQLiteURLFromPath(filepath.Join(dir, "app.db"))
			writeRollbackDir(c, binary, dir, repeatableViewFile)
			applied := applyRepeatableDir(c, binary, dir, url, repeatableEnabled)
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))
			writeRollbackDir(c, binary, dir, nil)

			got := runFormatTemplateVerb(c, binary, dir, tc.env,
				"migrate", "down", "--dir", "file://migrations", "--url", url, "--dev-url", "sqlite://dev?mode=memory")

			c.Assert(got.ExitCode, qt.Equals, 1)
			c.Assert(got.Stderr, qt.Contains, "cannot roll back: this database records repeatable migrations view")
			c.Assert(repeatableStrings(c, url, "SELECT version FROM atlas_schema_revisions ORDER BY version"),
				qt.DeepEquals, []string{"1", "2"})
			c.Assert(repeatableStrings(c, url, "SELECT identity FROM atlas_schema_revisions_repeatable"),
				qt.DeepEquals, []string{"view"})
		})
	}
}

// repeatableViewFile is R__view.sql, for a directory that holds it.
var repeatableViewFile = map[string]string{"R__view.sql": repeatableViewSQL}

// writeRollbackDir writes a directory every version of which has a down file,
// with the files of extra beside them, and hashes it with binary.
func writeRollbackDir(c *qt.C, binary, dir string, extra map[string]string) {
	c.Helper()
	files := map[string]string{
		"1_init.sql":      repeatableInitSQL,
		"1_init.down.sql": "DROP TABLE t;\n",
		"2_more.sql":      repeatableMoreSQL,
		"2_more.down.sql": "DROP TABLE u;\n",
	}
	maps.Copy(files, extra)
	writeHashedDir(c, binary, dir, files)
}

// downPaths are the two paths `migrate down` takes: the forward to the native
// rollback, and the --format report.
var downPaths = []struct {
	name string
	args []string
}{
	{name: "forwarded"},
	{name: "format", args: []string{"--format", "{{ json . }}"}},
}

// TestCompatRepeatableMigrationsDownRefusesBeforeConnectingE2E covers the
// refusal of a directory that holds an R__ file on both paths of `migrate
// down`: it comes from the directory the verb read, before the verb connects,
// so the SQLite file the URL names is never created.
func TestCompatRepeatableMigrationsDownRefusesBeforeConnectingE2E(t *testing.T) {
	for _, path := range downPaths {
		t.Run(path.name, func(t *testing.T) {
			c := qt.New(t)
			binary := clirun.Build(c, clirun.Compat)
			dir := c.TempDir()
			dbPath := filepath.Join(dir, "app.db")
			writeRollbackDir(c, binary, dir, repeatableViewFile)

			got := runFormatTemplateVerb(c, binary, dir, repeatableEnabled, append([]string{
				"migrate", "down", "--dir", "file://migrations", "--url", atlasurl.SQLiteURLFromPath(dbPath),
				"--dev-url", "sqlite://dev?mode=memory",
			}, path.args...)...)

			c.Assert(got.ExitCode, qt.Equals, 1)
			c.Assert(got.Stderr, qt.Contains,
				"PTAH_ATLAS_REPEATABLE_MIGRATIONS is set and the directory holds R__view.sql, which migrate down reads")
			_, statErr := os.Stat(dbPath)
			c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
		})
	}
}

// countingProgram appends one line to the file its first argument names and
// prints its second argument: a data "external" program whose runs a test
// counts.
const countingProgram = `package main

import (
	"fmt"
	"os"
)

func main() {
	file, err := os.OpenFile(os.Args[1], os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		os.Exit(2)
	}
	if _, err := fmt.Fprintln(file, "evaluated"); err != nil {
		os.Exit(2)
	}
	if err := file.Close(); err != nil {
		os.Exit(2)
	}
	fmt.Print(os.Args[2])
}
`

// buildCountingProgram compiles countingProgram and returns its path.
func buildCountingProgram(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module counting\n\ngo 1.22\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "main.go"), []byte(countingProgram), 0o600), qt.IsNil)
	binary := filepath.Join(dir, "counting"+exeext.Suffix)
	build := exec.CommandContext(c.Context(), "go", "build", "-o", binary, ".")
	build.Dir = dir
	build.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=")
	output, err := build.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	return binary
}

// TestCompatRepeatableMigrationsDownEvaluatesTheProjectOnceE2E pins one
// evaluation of atlas.hcl per `migrate down` with the variable set, on both
// paths: the data "external" program that supplies the URL runs once, so a
// program with side effects is not run twice to look for R__ files. With no
// amount the run reverts the last applied migration, so version 1 remains,
// which says the run reached the database the program named.
func TestCompatRepeatableMigrationsDownEvaluatesTheProjectOnceE2E(t *testing.T) {
	for _, path := range downPaths {
		t.Run(path.name, func(t *testing.T) {
			c := qt.New(t)
			binary := clirun.Build(c, clirun.Compat)
			program := buildCountingProgram(c)
			dir := c.TempDir()
			count := filepath.Join(dir, "evaluations.txt")
			url := "sqlite://" + filepath.ToSlash(filepath.Join(dir, "app.db"))
			writeRollbackDir(c, binary, dir, nil)
			project := fmt.Sprintf(`data "external" "url" {
  program = [%s, %s, %s]
}

env "local" {
  url = data.external.url
  dev = "sqlite://dev?mode=memory"
  migration {
    dir = "file://migrations"
  }
}
`, strconv.Quote(program), strconv.Quote(count), strconv.Quote(url))
			c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)
			applied := runFormatTemplateVerb(c, binary, dir, repeatableEnabled, "migrate", "apply", "--env", "local")
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))
			c.Assert(os.Remove(count), qt.IsNil)

			got := runFormatTemplateVerb(c, binary, dir, repeatableEnabled,
				append([]string{"migrate", "down", "--env", "local"}, path.args...)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
			evaluations, err := os.ReadFile(count)
			c.Assert(err, qt.IsNil)
			c.Assert(string(evaluations), qt.Equals, "evaluated\n")
			c.Assert(repeatableStrings(c, url, "SELECT version FROM atlas_schema_revisions ORDER BY version"),
				qt.DeepEquals, []string{"1"})
		})
	}
}

// TestCompatRepeatableMigrationsRollbackControlE2E is the control for the two
// rollback refusals: with the variable set, a directory with no R__ file over
// a database with no record rolls back. It runs the invocation the refusals
// run, with no amount, so it reverts the last applied migration and leaves
// version 1.
func TestCompatRepeatableMigrationsRollbackControlE2E(t *testing.T) {
	c := qt.New(t)
	binary := clirun.Build(c, clirun.Compat)
	dir := c.TempDir()
	url := atlasurl.SQLiteURLFromPath(filepath.Join(dir, "app.db"))
	writeRollbackDir(c, binary, dir, nil)
	applied := applyRepeatableDir(c, binary, dir, url, repeatableEnabled)
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))

	got := runFormatTemplateVerb(c, binary, dir, repeatableEnabled,
		"migrate", "down", "--dir", "file://migrations", "--url", url, "--dev-url", "sqlite://dev?mode=memory")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(repeatableStrings(c, url, "SELECT version FROM atlas_schema_revisions ORDER BY version"),
		qt.DeepEquals, []string{"1"})
}

// TestCompatRepeatableMigrationsNoteE2E covers the line migrate diff and
// migrate lint print while the variable is set over a directory that holds an
// R__ file, and its absence without the variable.
func TestCompatRepeatableMigrationsNoteE2E(t *testing.T) {
	for _, tc := range []struct {
		name           string
		env            []string
		wantDiffStderr string
		wantLintStderr string
	}{
		{
			name:           "variable set",
			env:            repeatableEnabled,
			wantDiffStderr: repeatableNote("diff"),
			wantLintStderr: repeatableNote("lint"),
		},
		{name: "variable unset"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			binary := clirun.Build(c, clirun.Compat)
			dir := c.TempDir()
			writeRepeatableDir(c, binary, dir, repeatableViewSQL)
			schema := repeatableInitSQL + repeatableMoreSQL + "CREATE VIEW v AS SELECT id, name FROM t;\n"
			c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(schema), 0o600), qt.IsNil)

			diff := runFormatTemplateVerb(c, binary, dir, tc.env, "migrate", "diff", "--dir", "file://migrations",
				"--to", "file://schema.sql", "--dev-url", "sqlite://dev?mode=memory")
			lint := runFormatTemplateVerb(c, binary, dir, tc.env, "migrate", "lint", "--dir", "file://migrations",
				"--dev-url", "sqlite://dev?mode=memory", "--latest", "1")

			c.Assert(diff.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", diff.Stderr))
			c.Assert(diff.Stderr, qt.Equals, tc.wantDiffStderr)
			c.Assert(lint.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", lint.Stderr))
			c.Assert(lint.Stderr, qt.Equals, tc.wantLintStderr)
			c.Assert(lint.Stdout, qt.Contains, "-- analyzing version R\n")
		})
	}
}

// TestCompatRepeatableMigrationsStrictControlE2E is the control for the strict
// refusal: an explicit false is accepted under strict compatibility, so the
// refusal above is the enabled value and not the variable's presence.
func TestCompatRepeatableMigrationsStrictControlE2E(t *testing.T) {
	c := qt.New(t)
	binary := clirun.Build(c, clirun.Compat)
	dir := c.TempDir()
	url := atlasurl.SQLiteURLFromPath(filepath.Join(dir, "app.db"))
	writeRepeatableDir(c, binary, dir, repeatableViewSQL)

	got := applyRepeatableDir(c, binary, dir, url,
		[]string{"PTAH_ATLAS_STRICT_COMPAT=1", "PTAH_ATLAS_REPEATABLE_MIGRATIONS=0"})

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(repeatableStrings(c, url, "SELECT version FROM atlas_schema_revisions ORDER BY version"),
		qt.DeepEquals, []string{"1", "2", "R"})
}
