//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/base64"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
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
	// revisions and record are the tables an Atlas apply writes on it.
	revisions string
	record    string
}

var repeatableEngines = []repeatableEngine{
	{
		name: "sqlite",
		// The authority form, which the community binary reads too; the
		// opaque form atlasurl.SQLiteURLFromPath writes is Ptah's alone.
		url: func(c *qt.C) string {
			return "sqlite://" + filepath.ToSlash(filepath.Join(c.TempDir(), "app.db"))
		},
		revisions: "atlas_schema_revisions",
		record:    "atlas_schema_revisions_repeatable",
	},
	{
		name:      "postgres",
		url:       func(c *qt.C) string { return postgresScratchDevURL(c, "ptah_compat_repeatable") },
		revisions: "atlas_schema_revisions.atlas_schema_revisions",
		record:    "atlas_schema_revisions.atlas_schema_revisions_repeatable",
	},
}

// repeatableEnabled is the environment that turns the reading on.
var repeatableEnabled = []string{"PTAH_ATLAS_REPEATABLE_MIGRATIONS=1"}

// writeRepeatableDir writes the directory with view as the body of
// R__view.sql into dir and hashes it with binary.
func writeRepeatableDir(c *qt.C, binary, dir, view string) {
	c.Helper()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
	for name, body := range map[string]string{
		"1_init.sql":  repeatableInitSQL,
		"2_more.sql":  repeatableMoreSQL,
		"R__view.sql": view,
	} {
		c.Assert(os.WriteFile(filepath.Join(migrations, name), []byte(body), 0o600), qt.IsNil)
	}
	hashed := runFormatTemplateVerb(c, binary, dir, nil, "migrate", "hash", "--dir", "file://migrations")
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", hashed.Stderr))
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
// reports are the community binary's, and no record table is created.
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
			c.Assert(status.Stdout, qt.Equals, repeatableOffStatus)
			c.Assert(repeatableStrings(c, url, "SELECT version FROM "+engine.revisions+" ORDER BY version"),
				qt.DeepEquals, []string{"1", "2", "R"})
			c.Assert(repeatableStrings(c, url,
				"SELECT CAST(COUNT(*) AS TEXT) FROM "+engine.revisions+" WHERE version = 'R'"),
				qt.DeepEquals, []string{"1"})
		})
	}
}

// TestReferenceRepeatableFileIsVersionRE2E takes the same measurement against
// the pinned community binary.
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
				c.Assert(got.Stderr, qt.Matches, `(?s)Error: .*records version R \(description "view"\), .*R__view.sql .*`+
					`delete the row with version 'R' from .*`)
				c.Assert(repeatableStrings(c, url, "SELECT version FROM "+engine.revisions+" ORDER BY version"),
					qt.DeepEquals, []string{"1", "2", "R"})
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

	t.Run("a rollback", func(t *testing.T) {
		c := qt.New(t)
		binary := clirun.Build(c, clirun.Compat)
		dir := c.TempDir()
		url := atlasurl.SQLiteURLFromPath(filepath.Join(dir, "app.db"))
		writeRepeatableDir(c, binary, dir, repeatableViewSQL)
		applied := applyRepeatableDir(c, binary, dir, url, repeatableEnabled)
		c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))

		got := runFormatTemplateVerb(c, binary, dir, repeatableEnabled,
			"migrate", "down", "--dir", "file://migrations", "--url", url, "--dev-url", "sqlite://dev?mode=memory")

		c.Assert(got.ExitCode, qt.Equals, 1)
		c.Assert(got.Stderr, qt.Contains, "PTAH_ATLAS_REPEATABLE_MIGRATIONS is set, and migrate down does not read")
		c.Assert(repeatableStrings(c, url, "SELECT version FROM atlas_schema_revisions ORDER BY version"),
			qt.DeepEquals, []string{"1", "2"})
	})
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
