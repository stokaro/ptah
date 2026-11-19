//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// An exclude selector keeps parts of a SQLite table out of scope while the
// plan changes the table. SQLite changes most of a table by writing a new one
// from the declaration and copying the rows, and the declaration holds none of
// the parts out of scope, so the rebuild carries each one from its live
// definition. These tests apply through both binaries, read the table back
// from sqlite_schema with its rows, and plan again with nothing to do.
//
// internal/planner/dialects/sqlite/out_of_scope_test.go holds the carry to the
// planner without a database.

const (
	// sqliteKeyedLive holds a named primary key, a UNIQUE, a CHECK and an
	// index, and sqliteKeyedDesired declares the primary key alone. A plan
	// changes the constraints the declaration leaves out by writing a new
	// table.
	sqliteKeyedLive = "CREATE TABLE kinds (id integer NOT NULL, code text, CONSTRAINT kinds_pk PRIMARY KEY (id), " +
		"CONSTRAINT uq_code UNIQUE (code), CONSTRAINT code_len CHECK (length(code) > 0));\n" +
		"CREATE INDEX idx_code ON kinds (code);\nINSERT INTO kinds VALUES (1, 'a'), (2, 'b');\n"
	sqliteKeyedDesired = "CREATE TABLE kinds (id integer NOT NULL, code text, CONSTRAINT kinds_pk PRIMARY KEY (id));\n"
	// sqliteKeyedKept is what sqlite_schema holds for kinds while no plan has
	// written the table anew, without its index.
	sqliteKeyedKept = "CREATE TABLE kinds (id integer NOT NULL, code text, CONSTRAINT kinds_pk PRIMARY KEY (id), " +
		"CONSTRAINT uq_code UNIQUE (code), CONSTRAINT code_len CHECK (length(code) > 0))"

	// sqliteDetailedLive holds a column with values and a default, an index
	// and two triggers beside the columns sqliteDetailedDesired declares, so
	// a plan writes the table anew to drop what the declaration leaves out.
	sqliteDetailedLive = "CREATE TABLE kinds (id integer PRIMARY KEY, name text, extra text DEFAULT 'x', " +
		"CONSTRAINT name_len CHECK (length(name) > 0));\n" +
		"CREATE INDEX idx_name ON kinds (name);\n" +
		"CREATE TRIGGER trg_name AFTER DELETE ON kinds BEGIN SELECT name FROM kinds WHERE id = OLD.id; END;\n" +
		"CREATE TRIGGER trg_extra AFTER DELETE ON kinds BEGIN SELECT extra FROM kinds WHERE id = OLD.id; END;\n" +
		"INSERT INTO kinds (id, name, extra) VALUES (1, 'a', 'one'), (2, 'b', 'two');\n"
	sqliteDetailedDesired = "CREATE TABLE kinds (id integer PRIMARY KEY, name text);\n"
	// sqliteDetailedKept is what sqlite_schema holds for kinds while no plan
	// has written the table anew.
	sqliteDetailedKept = "CREATE TRIGGER trg_extra AFTER DELETE ON kinds BEGIN SELECT extra FROM kinds WHERE id = OLD.id; END;" +
		"CREATE TRIGGER trg_name AFTER DELETE ON kinds BEGIN SELECT name FROM kinds WHERE id = OLD.id; END;" +
		"CREATE TABLE kinds (id integer PRIMARY KEY, name text, extra text DEFAULT 'x', " +
		"CONSTRAINT name_len CHECK (length(name) > 0));CREATE INDEX idx_name ON kinds (name)"
)

// sqliteCarryFixture is a database file built from a live schema, and the
// desired schema file.
type sqliteCarryFixture struct {
	url, schema string
	db          *sql.DB
}

func newSQLiteCarryFixture(c *qt.C, live, desired string) sqliteCarryFixture {
	c.Helper()
	dir := c.TempDir()
	path := filepath.Join(dir, "live.db")
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	_, err = db.ExecContext(c.Context(), live)
	c.Assert(err, qt.IsNil)
	schema := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)
	return sqliteCarryFixture{url: "sqlite://" + filepath.ToSlash(path), schema: schema, db: db}
}

// stored returns the statements sqlite_schema holds for kinds, its indexes
// and its triggers, the triggers first and each kind by name.
func (f sqliteCarryFixture) stored(c *qt.C) string {
	c.Helper()
	var stored string
	c.Assert(f.db.QueryRowContext(c.Context(), "SELECT group_concat(sql, ';') FROM (SELECT sql FROM sqlite_schema "+
		"WHERE tbl_name = 'kinds' AND sql IS NOT NULL ORDER BY type DESC, name)").Scan(&stored), qt.IsNil)
	return stored
}

// rows returns every row of kinds in the order of its key, a row's values
// joined by "|" and the rows by ";".
func (f sqliteCarryFixture) rows(c *qt.C) string {
	c.Helper()
	rows, err := f.db.QueryContext(c.Context(), "SELECT * FROM kinds ORDER BY 1")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	columns, err := rows.Columns()
	c.Assert(err, qt.IsNil)
	var lines []string
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		targets := make([]any, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		c.Assert(rows.Scan(targets...), qt.IsNil)
		texts := make([]string, len(values))
		for i, value := range values {
			texts[i] = value.String
		}
		lines = append(lines, strings.Join(texts, "|"))
	}
	c.Assert(rows.Err(), qt.IsNil)
	return strings.Join(lines, ";")
}

func (f sqliteCarryFixture) apply(c *qt.C, exclude, mode string) clirun.Result {
	c.Helper()
	return runPtahIn(c, "schema", "apply", "--db-url", f.url, "--schema-file", f.schema, "--exclude", exclude, mode)
}

func (f sqliteCarryFixture) compatApply(c *qt.C, exclude string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, "schema", "apply",
		"-u", f.url, "--to", "file://"+filepath.ToSlash(f.schema), "--dev-url", "sqlite://dev?mode=memory",
		"--exclude", exclude, "--auto-approve")
}

func (f sqliteCarryFixture) compatDiff(c *qt.C, exclude string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, "schema", "diff",
		"--from", f.url, "--to", "file://"+filepath.ToSlash(f.schema), "--dev-url", "sqlite://dev?mode=memory", "--exclude", exclude)
}

// TestExcludeKeepsWhatASQLiteRebuildCarriesE2E applies a desired schema that
// leaves parts of kinds out, under a selector that keeps one of them, through
// both binaries. Each binary keeps the part the selector names and the rows,
// drops the parts it does not name, and a run that follows finds nothing to
// do. The community binary v1.3.0 drops the column, the index, the trigger and
// the CHECK under these selectors, and with `kinds.code_len[type=check]` its
// apply fails on `no such index`.
//
// A selector that names every constraint leaves nothing of the table to
// change, so the table stays as it is; `*[type=index]` names the UNIQUE and
// the primary key too, since SQLite builds an index for each.
func TestExcludeKeepsWhatASQLiteRebuildCarriesE2E(t *testing.T) {
	tests := []struct {
		name, live, desired, exclude, native, compat, rows string
	}{
		{
			name: "a column", live: sqliteDetailedLive, desired: sqliteDetailedDesired, exclude: "kinds.extra[type=column]",
			native: "CREATE TABLE \"kinds\" (\n  \"id\" integer PRIMARY KEY,\n  \"name\" TEXT,\n  \"extra\" TEXT DEFAULT 'x'\n)",
			compat: "CREATE TABLE \"kinds\" (\n  \"id\" integer NULL,\n  \"name\" text NULL,\n  \"extra\" text NULL DEFAULT 'x',\n" +
				"  PRIMARY KEY (\"id\")\n)",
			rows: "1|a|one;2|b|two",
		},
		{
			name: "an index", live: sqliteDetailedLive, desired: sqliteDetailedDesired, exclude: "kinds.idx_name[type=index]",
			native: "CREATE TABLE \"kinds\" (\n  \"id\" integer PRIMARY KEY,\n  \"name\" TEXT\n);CREATE INDEX idx_name ON kinds (name)",
			compat: "CREATE TABLE \"kinds\" (\n  \"id\" integer NULL,\n  \"name\" text NULL,\n  PRIMARY KEY (\"id\")\n);" +
				"CREATE INDEX idx_name ON kinds (name)",
			rows: "1|a;2|b",
		},
		{
			name: "a trigger", live: sqliteDetailedLive, desired: sqliteDetailedDesired, exclude: "kinds.trg_name[type=trigger]",
			native: "CREATE TRIGGER trg_name AFTER DELETE ON kinds BEGIN SELECT name FROM kinds WHERE id = OLD.id; END;" +
				"CREATE TABLE \"kinds\" (\n  \"id\" integer PRIMARY KEY,\n  \"name\" TEXT\n)",
			compat: "CREATE TRIGGER trg_name AFTER DELETE ON kinds BEGIN SELECT name FROM kinds WHERE id = OLD.id; END;" +
				"CREATE TABLE \"kinds\" (\n  \"id\" integer NULL,\n  \"name\" text NULL,\n  PRIMARY KEY (\"id\")\n)",
			rows: "1|a;2|b",
		},
		{
			name: "a CHECK", live: sqliteKeyedLive, desired: sqliteKeyedDesired, exclude: "kinds.code_len[type=check]",
			native: "CREATE TABLE \"kinds\" (\n  \"id\" integer NOT NULL,\n  \"code\" TEXT,\n  CONSTRAINT \"kinds_pk\" PRIMARY KEY (\"id\"),\n" +
				"  CONSTRAINT \"code_len\" CHECK (length(code) > 0)\n)",
			compat: "CREATE TABLE \"kinds\" (\n  \"id\" integer NOT NULL,\n  \"code\" text NULL,\n  CONSTRAINT \"kinds_pk\" PRIMARY KEY (\"id\"),\n" +
				"  CONSTRAINT \"code_len\" CHECK (length(code) > 0)\n)",
			rows: "1|a;2|b",
		},
		{
			name: "every constraint", live: sqliteKeyedLive, desired: sqliteKeyedDesired, exclude: "*[type=constraint]",
			native: sqliteKeyedKept, compat: sqliteKeyedKept, rows: "1|a;2|b",
		},
		{
			name: "every index", live: sqliteKeyedLive, desired: sqliteKeyedDesired, exclude: "*[type=index]",
			native: "CREATE TABLE \"kinds\" (\n  \"id\" integer NOT NULL,\n  \"code\" TEXT,\n  CONSTRAINT \"uq_code\" UNIQUE (\"code\"),\n" +
				"  CONSTRAINT \"kinds_pk\" PRIMARY KEY (\"id\")\n);CREATE INDEX idx_code ON kinds (code)",
			compat: "CREATE TABLE \"kinds\" (\n  \"id\" integer NOT NULL,\n  \"code\" text NULL,\n  CONSTRAINT \"kinds_pk\" PRIMARY KEY (\"id\"),\n" +
				"  CONSTRAINT \"uq_code\" UNIQUE (\"code\")\n);CREATE INDEX idx_code ON kinds (code)",
			rows: "1|a;2|b",
		},
		{
			name: "every index of the table", live: sqliteKeyedLive, desired: sqliteKeyedDesired, exclude: "kinds.*[type=index]",
			native: "CREATE TABLE \"kinds\" (\n  \"id\" integer NOT NULL,\n  \"code\" TEXT,\n  CONSTRAINT \"uq_code\" UNIQUE (\"code\"),\n" +
				"  CONSTRAINT \"kinds_pk\" PRIMARY KEY (\"id\")\n);CREATE INDEX idx_code ON kinds (code)",
			compat: "CREATE TABLE \"kinds\" (\n  \"id\" integer NOT NULL,\n  \"code\" text NULL,\n  CONSTRAINT \"kinds_pk\" PRIMARY KEY (\"id\"),\n" +
				"  CONSTRAINT \"uq_code\" UNIQUE (\"code\")\n);CREATE INDEX idx_code ON kinds (code)",
			rows: "1|a;2|b",
		},
		{
			name: "every column of the table", live: sqliteKeyedLive, desired: sqliteKeyedDesired, exclude: "kinds.*",
			native: sqliteKeyedKept + ";CREATE INDEX idx_code ON kinds (code)",
			compat: sqliteKeyedKept + ";CREATE INDEX idx_code ON kinds (code)",
			rows:   "1|a;2|b",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			native := newSQLiteCarryFixture(c, test.live, test.desired)
			compat := newSQLiteCarryFixture(c, test.live, test.desired)

			nativeApplied := native.apply(c, test.exclude, "--auto-approve")
			compatApplied := compat.compatApply(c, test.exclude)
			planned := native.apply(c, test.exclude, "--dry-run")
			diffed := compat.compatDiff(c, test.exclude)

			c.Assert(nativeApplied.ExitCode, qt.Equals, 0, historyComment(nativeApplied))
			c.Assert(native.stored(c), qt.Equals, test.native)
			c.Assert(native.rows(c), qt.Equals, test.rows)
			c.Assert(compatApplied.ExitCode, qt.Equals, 0, historyComment(compatApplied))
			c.Assert(compat.stored(c), qt.Equals, test.compat)
			c.Assert(compat.rows(c), qt.Equals, test.rows)
			c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
			c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
			c.Assert(diffed.Stdout, qt.Equals, includeSynced)
		})
	}
}

// TestExcludeRefusesWhatASQLiteRebuildCannotCarryE2E keeps a trigger out of
// scope whose statement reads a column the rebuild drops. SQLite resolves a
// trigger's columns when it fires, so the carried trigger would be created and
// then fail every DELETE. Both binaries refuse the plan, name the trigger and
// the column, and leave the table and its rows as they are.
func TestExcludeRefusesWhatASQLiteRebuildCannotCarryE2E(t *testing.T) {
	const want = "sqlite: rebuilding table kinds cannot keep trigger trg_extra as it is: an exclude selector keeps it " +
		"out of scope, and its statement names column extra, which the rebuilt table drops. Keep the column, or " +
		"exclude the whole table"
	c := qt.New(t)
	native := newSQLiteCarryFixture(c, sqliteDetailedLive, sqliteDetailedDesired)
	compat := newSQLiteCarryFixture(c, sqliteDetailedLive, sqliteDetailedDesired)

	nativeApplied := native.apply(c, "kinds.trg_extra[type=trigger]", "--auto-approve")
	compatApplied := compat.compatApply(c, "kinds.trg_extra[type=trigger]")

	c.Assert(nativeApplied.ExitCode, qt.Equals, 2, historyComment(nativeApplied))
	c.Assert(nativeApplied.Stderr, qt.Contains, want)
	c.Assert(native.stored(c), qt.Equals, sqliteDetailedKept)
	c.Assert(native.rows(c), qt.Equals, "1|a|one;2|b|two")
	c.Assert(compatApplied.ExitCode, qt.Equals, 1, historyComment(compatApplied))
	c.Assert(compatApplied.Stderr, qt.Contains, want)
	c.Assert(compat.stored(c), qt.Equals, sqliteDetailedKept)
	c.Assert(compat.rows(c), qt.Equals, "1|a|one;2|b|two")
}
