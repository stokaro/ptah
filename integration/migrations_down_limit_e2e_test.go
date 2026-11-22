//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/atlascompat"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/migration/migrationfile"
)

// limitHistoryFormat is a --format template that names the versions only.
const limitHistoryFormat = "{{ .Current }}->{{ .Target }}"

// `ptah-compat migrate down` with no amount reverts the last applied migration,
// and `migrate down 2` reverts up to two, as https://atlasgo.io/versioned/down
// documents; an amount above the history reverts what is applied. The native
// variables PTAH_TARGET and PTAH_LIMIT are not read on this surface. Each row
// applies versions 1, 2 and 3, which create a, b and c, and reads back which
// tables and revision rows are left.
func TestAtlasMigrateDownAmountE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name          string
		args          []string
		env           []string
		wantStdout    string
		wantTables    []string
		wantRevisions []string
	}{
		{
			name:          "no amount reverts the last applied migration",
			wantStdout:    "Target version: 2\n",
			wantTables:    []string{"a", "b"},
			wantRevisions: []string{"1", "2"},
		},
		{
			name:          "an amount of one",
			args:          []string{"1"},
			wantStdout:    "Target version: 2\n",
			wantTables:    []string{"a", "b"},
			wantRevisions: []string{"1", "2"},
		},
		{
			name:          "an amount of two",
			args:          []string{"2"},
			wantStdout:    "Target version: 1\n",
			wantTables:    []string{"a"},
			wantRevisions: []string{"1"},
		},
		{
			name:          "an amount above the history",
			args:          []string{"9"},
			wantStdout:    "Target version: 0\n",
			wantTables:    make([]string, 0),
			wantRevisions: make([]string, 0),
		},
		{
			name:          "a version names the stop",
			args:          []string{"--to-version", "1"},
			wantStdout:    "Target version: 1\n",
			wantTables:    []string{"a"},
			wantRevisions: []string{"1"},
		},
		{
			// PTAH_TO_VERSION is this surface's variable. PTAH_TARGET belongs
			// to the native command the run forwards to, which does not read it.
			name:          "PTAH_TARGET is not read",
			env:           []string{"PTAH_TARGET=1"},
			wantStdout:    "Target version: 2\n",
			wantTables:    []string{"a", "b"},
			wantRevisions: []string{"1", "2"},
		},
		{
			name:          "PTAH_TARGET is not read beside an amount",
			args:          []string{"1"},
			env:           []string{"PTAH_TARGET=1"},
			wantStdout:    "Target version: 2\n",
			wantTables:    []string{"a", "b"},
			wantRevisions: []string{"1", "2"},
		},
		{
			name:          "the report does not read PTAH_TARGET",
			args:          []string{"--format", limitHistoryFormat},
			env:           []string{"PTAH_TARGET=1"},
			wantStdout:    "3->2",
			wantTables:    []string{"a", "b"},
			wantRevisions: []string{"1", "2"},
		},
		{
			// The adapter decides the count, so the native command does not
			// read PTAH_LIMIT, and a value it would refuse has no effect.
			name:          "a malformed PTAH_LIMIT is not read",
			args:          []string{"--to-version", "1"},
			env:           []string{"PTAH_LIMIT=abc"},
			wantStdout:    "Target version: 1\n",
			wantTables:    []string{"a"},
			wantRevisions: []string{"1"},
		},
		{
			name:          "the report with no amount",
			args:          []string{"--format", limitHistoryFormat},
			wantStdout:    "3->2",
			wantTables:    []string{"a", "b"},
			wantRevisions: []string{"1", "2"},
		},
		{
			name:          "the report with an amount of two",
			args:          []string{"2", "--format", limitHistoryFormat},
			wantStdout:    "3->1",
			wantTables:    []string{"a"},
			wantRevisions: []string{"1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, targetURL := appliedLimitHistory(c)
			opts := clirun.Options{Dir: dir, Env: append(environmentWithoutPtahVariables(), test.env...)}

			got := clirun.Run(c, clirun.Compat, opts,
				append([]string{"migrate", "down", "--url", targetURL, "--dir", "file://migrations"}, test.args...)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, test.wantStdout)
			c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, test.wantTables)
			c.Assert(limitHistoryRevisions(c, targetURL), qt.DeepEquals, test.wantRevisions)
		})
	}
}

// TestAtlasMigrateDownAmountE2E_PTAHTargetIsNotReadBesideATag forwards --to-tag
// with PTAH_TARGET set. The native command refuses a --target its variable set
// beside a typed --to-tag, so this is the run on which PTAH_TARGET would
// change the outcome. The native command the run forwards to does not read the
// variable, and the tag decides.
func TestAtlasMigrateDownAmountE2E_PTAHTargetIsNotReadBesideATag(t *testing.T) {
	c := qt.New(t)
	dir, targetURL := appliedLimitHistory(c)
	tagged := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
		"migrations", "tag", "v1", "--db-url", targetURL, "--version", "2", "--revision-format", "atlas")
	c.Assert(tagged.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", tagged.Stdout, tagged.Stderr))
	opts := clirun.Options{Dir: dir, Env: append(environmentWithoutPtahVariables(), "PTAH_TARGET=1")}

	got := clirun.Run(c, clirun.Compat, opts,
		"migrate", "down", "--url", targetURL, "--dir", "file://migrations", "--to-tag", "v1")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stdout, qt.Contains, "Target version: 2\n")
	c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, []string{"a", "b"})
	c.Assert(limitHistoryRevisions(c, targetURL), qt.DeepEquals, []string{"1", "2"})
}

// An amount that is not a positive integer, an amount beside a version or a
// tag, a version or tag that names nothing, and the native --limit are refused
// before anything is reverted, on the forwarded run and on the --format run
// alike.
func TestAtlasMigrateDownAmountE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		env        []string
		wantStderr string
	}{
		{name: "zero", args: []string{"0"}, wantStderr: "Error: cannot revert '0' migration files\n"},
		{name: "negative", args: []string{"--", "-1"}, wantStderr: "Error: cannot revert '-1' migration files\n"},
		{
			name:       "not a number",
			args:       []string{"two"},
			wantStderr: `Error: invalid amount argument "two" (invalid syntax). Omit the argument or pass a valid integer instead` + "\n",
		},
		{name: "two amounts", args: []string{"1", "2"}, wantStderr: "Error: accepts at most 1 arg(s), received 2\n"},
		{
			name:       "an amount beside a version",
			args:       []string{"2", "--to-version", "1"},
			wantStderr: "Error: the amount argument and --to-version both name where to stop; pass one\n",
		},
		{
			name:       "an amount beside PTAH_TO_VERSION",
			args:       []string{"2"},
			env:        []string{"PTAH_TO_VERSION=1"},
			wantStderr: "Error: the amount argument and --to-version both name where to stop; pass one\n",
		},
		{
			name:       "an amount beside a tag",
			args:       []string{"1", "--to-tag", "v1"},
			wantStderr: "Error: the amount argument and --to-tag both name where to stop; pass one\n",
		},
		{
			name:       "an empty version",
			args:       []string{"--to-version", ""},
			wantStderr: "Error: --to-version is empty: give it a version, or leave it out to revert the last applied migration\n",
		},
		{
			name:       "an empty tag",
			args:       []string{"--to-tag", ""},
			wantStderr: "Error: --to-tag is empty: give it a tag, or leave it out to revert the last applied migration\n",
		},
		{
			name:       "the native limit",
			args:       []string{"--limit", "2"},
			wantStderr: "Error: atlas migrate down does not accept native Ptah flag --limit\n",
		},
		{
			name:       "the native limit beside an amount",
			args:       []string{"2", "--limit", "1"},
			wantStderr: "Error: atlas migrate down does not accept native Ptah flag --limit\n",
		},
		{
			name:       "the report with the native limit",
			args:       []string{"--limit", "2", "--format", limitHistoryFormat},
			wantStderr: "Error: atlas migrate down does not accept native Ptah flag --limit\n",
		},
		{
			name:       "the report with zero",
			args:       []string{"0", "--format", limitHistoryFormat},
			wantStderr: "Error: cannot revert '0' migration files\n",
		},
		{
			name:       "the report with an amount beside a version",
			args:       []string{"2", "--to-version", "1", "--format", limitHistoryFormat},
			wantStderr: "Error: the amount argument and --to-version both name where to stop; pass one\n",
		},
		{
			name:       "the report with an empty tag",
			args:       []string{"--to-tag", "", "--format", limitHistoryFormat},
			wantStderr: "Error: --to-tag is empty: give it a tag, or leave it out to revert the last applied migration\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, targetURL := appliedLimitHistory(c)
			opts := clirun.Options{Dir: dir, Env: append(environmentWithoutPtahVariables(), test.env...)}

			got := clirun.Run(c, clirun.Compat, opts,
				append([]string{"migrate", "down", "--url", targetURL, "--dir", "file://migrations"}, test.args...)...)

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, test.wantStderr)
			c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, []string{"a", "b", "c"})
			c.Assert(limitHistoryRevisions(c, targetURL), qt.DeepEquals, []string{"1", "2", "3"})
		})
	}
}

// `ptah migrations down --limit` is the native form of the amount: it reverts
// the last N applied migrations, and its variable PTAH_LIMIT does the same.
func TestMigrationsDownLimitE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name          string
		args          []string
		env           []string
		wantTables    []string
		wantRevisions []string
	}{
		{name: "a limit of one", args: []string{"--limit", "1"}, wantTables: []string{"a", "b"}, wantRevisions: []string{"1", "2"}},
		{name: "a limit of two", args: []string{"--limit", "2"}, wantTables: []string{"a"}, wantRevisions: []string{"1"}},
		{name: "a limit above the history", args: []string{"--limit", "9"}, wantTables: make([]string, 0), wantRevisions: make([]string, 0)},
		{name: "PTAH_LIMIT", env: []string{"PTAH_LIMIT=1"}, wantTables: []string{"a", "b"}, wantRevisions: []string{"1", "2"}},
		{
			name:          "a typed limit over PTAH_TARGET",
			args:          []string{"--limit", "1"},
			env:           []string{"PTAH_TARGET=1"},
			wantTables:    []string{"a", "b"},
			wantRevisions: []string{"1", "2"},
		},
		{
			// No migration carries the tag, so a run that reads it is refused.
			name:          "a typed limit over PTAH_TO_TAG",
			args:          []string{"--limit", "1"},
			env:           []string{"PTAH_TO_TAG=v1"},
			wantTables:    []string{"a", "b"},
			wantRevisions: []string{"1", "2"},
		},
		{
			name:          "a typed target over PTAH_LIMIT",
			args:          []string{"--target", "1"},
			env:           []string{"PTAH_LIMIT=1"},
			wantTables:    []string{"a"},
			wantRevisions: []string{"1"},
		},
		{
			name:          "a typed target of the whole history over PTAH_LIMIT",
			args:          []string{"--target", "0"},
			env:           []string{"PTAH_LIMIT=1"},
			wantTables:    make([]string, 0),
			wantRevisions: make([]string, 0),
		},
		{
			name:          "a limit of zero leaves PTAH_TARGET in force",
			args:          []string{"--limit", "0"},
			env:           []string{"PTAH_TARGET=1"},
			wantTables:    []string{"a"},
			wantRevisions: []string{"1"},
		},
		{
			name:          "an empty tag leaves the limit in force",
			args:          []string{"--limit", "1", "--to-tag", ""},
			wantTables:    []string{"a", "b"},
			wantRevisions: []string{"1", "2"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, targetURL := appliedLimitHistory(c)
			opts := clirun.Options{Dir: dir, Env: append(environmentWithoutPtahVariables(), test.env...)}

			got := clirun.Run(c, clirun.Ptah, opts, append(nativeLimitDown(targetURL), test.args...)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, test.wantTables)
			c.Assert(limitHistoryRevisions(c, targetURL), qt.DeepEquals, test.wantRevisions)
		})
	}
}

// --limit beside --target or --to-tag is refused before the command connects,
// a --target of 0 included, since 0 is the version the whole history rolls
// back to.
func TestMigrationsDownLimitE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		env        []string
		wantStderr string
	}{
		{
			name:       "a limit beside a target",
			args:       []string{"--limit", "1", "--target", "1"},
			wantStderr: "error: --limit and --target both name where to stop; pass one\n",
		},
		{
			name:       "a limit beside the target of the whole history",
			args:       []string{"--limit", "1", "--target", "0"},
			wantStderr: "error: --limit and --target both name where to stop; pass one\n",
		},
		{
			name:       "a limit beside a tag",
			args:       []string{"--limit", "1", "--to-tag", "v1"},
			wantStderr: "error: --limit and --to-tag both name where to stop; pass one\n",
		},
		{
			name:       "PTAH_LIMIT beside PTAH_TARGET",
			env:        []string{"PTAH_LIMIT=1", "PTAH_TARGET=1"},
			wantStderr: "error: PTAH_LIMIT and PTAH_TARGET both name where to stop; unset one\n",
		},
		{
			name:       "PTAH_LIMIT beside PTAH_TO_TAG",
			env:        []string{"PTAH_LIMIT=1", "PTAH_TO_TAG=v1"},
			wantStderr: "error: PTAH_LIMIT and PTAH_TO_TAG both name where to stop; unset one\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, targetURL := appliedLimitHistory(c)
			opts := clirun.Options{Dir: dir, Env: append(environmentWithoutPtahVariables(), test.env...)}

			got := clirun.Run(c, clirun.Ptah, opts, append(nativeLimitDown(targetURL), test.args...)...)

			c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, test.wantStderr)
			c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, []string{"a", "b", "c"})
			c.Assert(limitHistoryRevisions(c, targetURL), qt.DeepEquals, []string{"1", "2", "3"})
		})
	}
}

// nativeLimitDown is `ptah migrations down` against the history
// [appliedLimitHistory] wrote, read the way `migrate apply` wrote it.
func nativeLimitDown(targetURL string) []string {
	return []string{
		"migrations", "down", "--db-url", targetURL, "--migrations-dir", "migrations",
		"--dir-format", "atlas", "--revision-format", "atlas", "--confirm",
	}
}

// appliedLimitHistory writes an Atlas directory whose versions 1, 2 and 3
// create a, b and c and carry the down bodies that drop them, applies it with
// `ptah-compat migrate apply` to a database file, and returns the directory
// that holds both and the database URL.
func appliedLimitHistory(c *qt.C) (dir, targetURL string) {
	c.Helper()
	dir, targetURL = pendingLimitHistory(c)
	applied := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
		"migrate", "apply", "--url", targetURL, "--dir", "file://migrations")
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, []string{"a", "b", "c"})
	return dir, targetURL
}

// pendingLimitHistory writes the directory [appliedLimitHistory] applies, with
// its atlas.sum, and names an empty SQLite database beside it.
func pendingLimitHistory(c *qt.C) (dir, targetURL string) {
	c.Helper()
	dir = c.TempDir()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrations, 0o750), qt.IsNil)
	for name, table := range map[string]string{"1_a.sql": "a", "2_b.sql": "b", "3_c.sql": "c"} {
		body := "-- atlas:txtar\n\n-- migration.sql --\nCREATE TABLE " + table + " (id INTEGER PRIMARY KEY);\n\n" +
			"-- down.sql --\nDROP TABLE " + table + ";\n"
		c.Assert(os.WriteFile(filepath.Join(migrations, name), []byte(body), 0o600), qt.IsNil)
	}
	sum, err := atlascompat.ComputeSum(os.DirFS(migrations), migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, atlascompat.AtlasSumFileName), sum.Bytes(), 0o600), qt.IsNil)

	return dir, atlasurl.SQLiteURLFromPath(filepath.Join(dir, "target.db"))
}

// limitHistoryTables reads back which of the history's tables the target holds.
func limitHistoryTables(c *qt.C, targetURL string) []string {
	c.Helper()
	return limitHistoryColumn(c, targetURL,
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('a', 'b', 'c') ORDER BY name")
}

// limitHistoryRevisions reads back the versions the revision table records.
func limitHistoryRevisions(c *qt.C, targetURL string) []string {
	c.Helper()
	return limitHistoryColumn(c, targetURL,
		"SELECT version FROM atlas_schema_revisions ORDER BY CAST(version AS INTEGER)")
}

func limitHistoryColumn(c *qt.C, targetURL, query string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), query)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	values := make([]string, 0, 3)
	for rows.Next() {
		var value string
		c.Assert(rows.Scan(&value), qt.IsNil)
		values = append(values, value)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return values
}
