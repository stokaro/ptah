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

	// sqliteNamedLive holds kinds, a view that reads it and a table log, and
	// sqliteNamedDesired drops kinds.extra, so a plan writes kinds anew.
	sqliteNamedLive = "CREATE TABLE kinds (id integer PRIMARY KEY, name text, extra text);\n" +
		"CREATE TABLE log (id integer PRIMARY KEY, msg text);\n" +
		"CREATE VIEW v AS SELECT id, name FROM kinds;\n" +
		"INSERT INTO kinds VALUES (1, 'a', 'one'), (2, 'b', 'two');\n"
	sqliteNamedDesired = "CREATE TABLE kinds (id integer PRIMARY KEY, name text);\n" +
		"CREATE TABLE log (id integer PRIMARY KEY, msg text);\n"
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
			name: "a column whose default is a double-quoted identifier",
			live: "CREATE TABLE kinds (id integer PRIMARY KEY, name text, d7 text DEFAULT \"dq\");\n" +
				"INSERT INTO kinds (id, name) VALUES (1, 'a'), (2, 'b');\n",
			desired: "CREATE TABLE kinds (id integer PRIMARY KEY);\n", exclude: "kinds.d7[type=column]",
			native: "CREATE TABLE \"kinds\" (\n  \"id\" integer PRIMARY KEY,\n  \"d7\" TEXT DEFAULT 'dq'\n)",
			compat: "CREATE TABLE \"kinds\" (\n  \"id\" integer NULL,\n  \"d7\" text NULL DEFAULT 'dq',\n  PRIMARY KEY (\"id\")\n)",
			rows:   "1|dq;2|dq",
		},
		{
			name: "an AUTOINCREMENT column, with its key",
			live: "CREATE TABLE kinds (id INTEGER PRIMARY KEY AUTOINCREMENT, name text, extra text);\n" +
				"INSERT INTO kinds (name, extra) VALUES ('a', 'one'), ('b', 'two');\n",
			desired: "CREATE TABLE kinds (name text);\n", exclude: "kinds.id[type=column]",
			native: "CREATE TABLE \"kinds\" (\n  \"id\" INTEGER PRIMARY KEY AUTOINCREMENT,\n  \"name\" TEXT\n)",
			compat: "CREATE TABLE \"kinds\" (\n  \"id\" integer PRIMARY KEY AUTOINCREMENT,\n  \"name\" text NULL\n)",
			rows:   "1|a;2|b",
		},
		{
			name: "a trigger that writes a column of the dropped name into another table",
			live: "CREATE TABLE kinds (id integer PRIMARY KEY, code text, name text);\n" +
				"CREATE TABLE log (id integer PRIMARY KEY, name text);\n" +
				"CREATE TRIGGER kinds_log AFTER DELETE ON kinds BEGIN /* name */ INSERT INTO log(name) VALUES (OLD.code); END;\n" +
				"INSERT INTO kinds VALUES (1, 'c', 'a'), (2, 'd', 'b');\n",
			desired: "CREATE TABLE kinds (id integer PRIMARY KEY, code text);\nCREATE TABLE log (id integer PRIMARY KEY, name text);\n",
			exclude: "kinds.kinds_log[type=trigger]",
			native: "CREATE TRIGGER kinds_log AFTER DELETE ON kinds BEGIN /* name */ INSERT INTO log(name) VALUES (OLD.code); END;" +
				"CREATE TABLE \"kinds\" (\n  \"id\" integer PRIMARY KEY,\n  \"code\" TEXT\n)",
			compat: "CREATE TRIGGER kinds_log AFTER DELETE ON kinds BEGIN /* name */ INSERT INTO log(name) VALUES (OLD.code); END;" +
				"CREATE TABLE \"kinds\" (\n  \"id\" integer NULL,\n  \"code\" text NULL,\n  PRIMARY KEY (\"id\")\n)",
			rows: "1|c;2|d",
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

// TestExcludeRefusesWhatASQLiteRebuildCannotCarryE2E keeps a part out of
// scope that reads a column the rebuild drops: a trigger, which SQLite
// resolves when it fires, an index whose key expression or WHERE clause reads
// the column, which CREATE INDEX would refuse after the old table is gone, a
// generated column, which CREATE TABLE would refuse, and a view, which would
// fail when it runs. Both binaries refuse the plan, name the part and the
// column, and leave the table and its rows as they are.
func TestExcludeRefusesWhatASQLiteRebuildCannotCarryE2E(t *testing.T) {
	const keyed = "CREATE TABLE kinds (id integer PRIMARY KEY, code text, name text);\n"
	const seeded = "INSERT INTO kinds (id, code, name) VALUES (1, 'c', 'a'), (2, 'd', 'b');\n"
	tests := []struct {
		name, live, desired, exclude, want string
	}{
		{
			name: "a trigger", live: sqliteDetailedLive, desired: sqliteDetailedDesired, exclude: "kinds.trg_extra[type=trigger]",
			want: "sqlite: rebuilding table kinds cannot keep trigger trg_extra as it is: an exclude selector keeps it " +
				"out of scope, and its statement reads column extra, which the rebuilt table drops. Keep the column, or " +
				"exclude the whole table",
		},
		{
			name:    "an index whose key expression reads the column",
			live:    keyed + "CREATE INDEX kinds_lower ON kinds (lower(name));\n" + seeded,
			desired: "CREATE TABLE kinds (id integer PRIMARY KEY, code text);\n", exclude: "kinds.kinds_lower[type=index]",
			want: "sqlite: rebuilding table kinds cannot keep index kinds_lower: its statement reads column name, which the " +
				"rebuilt table drops. Keep the column, or remove the index in a separate migration first",
		},
		{
			name:    "a partial index whose WHERE clause reads the column",
			live:    keyed + "CREATE INDEX kinds_code ON kinds (code) WHERE name IS NOT NULL;\n" + seeded,
			desired: "CREATE TABLE kinds (id integer PRIMARY KEY, code text);\n", exclude: "kinds.kinds_code[type=index]",
			want: "sqlite: rebuilding table kinds cannot keep index kinds_code: its statement reads column name, which the " +
				"rebuilt table drops. Keep the column, or remove the index in a separate migration first",
		},
		{
			name: "a generated column that reads the column",
			live: "CREATE TABLE kinds (id integer PRIMARY KEY, name text, low text GENERATED ALWAYS AS (lower(name)) VIRTUAL);\n" +
				"INSERT INTO kinds (id, name) VALUES (1, 'A'), (2, 'B');\n",
			desired: "CREATE TABLE kinds (id integer PRIMARY KEY);\n", exclude: "kinds.low[type=column]",
			want: "sqlite: rebuilding table kinds cannot keep column low as it is: an exclude selector keeps it out of scope, " +
				"and its generated expression reads column name, which the rebuilt table drops. Keep the column, or " +
				"exclude the whole table",
		},
		{
			name: "a view that reads the column",
			live: "CREATE TABLE kinds (id integer PRIMARY KEY, name text, extra text);\n" +
				"CREATE VIEW v AS SELECT k.id, k.extra FROM kinds AS k;\nINSERT INTO kinds VALUES (1, 'a', 'one');\n",
			desired: "CREATE TABLE kinds (id integer PRIMARY KEY, name text);\n", exclude: "v[type=view]",
			want: "sqlite: rebuilding table kinds would break view v: it reads column extra, which the rebuilt table drops. " +
				"Keep the column, or change the view first",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			native := newSQLiteCarryFixture(c, test.live, test.desired)
			compat := newSQLiteCarryFixture(c, test.live, test.desired)
			untouched := newSQLiteCarryFixture(c, test.live, test.desired)

			nativeApplied := native.apply(c, test.exclude, "--auto-approve")
			compatApplied := compat.compatApply(c, test.exclude)

			c.Assert(nativeApplied.ExitCode, qt.Equals, 2, historyComment(nativeApplied))
			c.Assert(nativeApplied.Stderr, qt.Contains, test.want)
			c.Assert(native.stored(c), qt.Equals, untouched.stored(c))
			c.Assert(native.rows(c), qt.Equals, untouched.rows(c))
			c.Assert(compatApplied.ExitCode, qt.Equals, 1, historyComment(compatApplied))
			c.Assert(compatApplied.Stderr, qt.Contains, test.want)
			c.Assert(compat.stored(c), qt.Equals, untouched.stored(c))
			c.Assert(compat.rows(c), qt.Equals, untouched.rows(c))
		})
	}
}

// scalar returns the one value query reads, after statement runs.
func (f sqliteCarryFixture) scalar(c *qt.C, statement, query string) string {
	c.Helper()
	_, err := f.db.ExecContext(c.Context(), statement)
	c.Assert(err, qt.IsNil)
	var value sql.NullString
	c.Assert(f.db.QueryRowContext(c.Context(), query).Scan(&value), qt.IsNil)
	return value.String
}

// definition returns the statement sqlite_schema holds for the object name.
func (f sqliteCarryFixture) definition(c *qt.C, name string) string {
	c.Helper()
	var definition string
	c.Assert(f.db.QueryRowContext(c.Context(), "SELECT sql FROM sqlite_schema WHERE name = ?", name).Scan(&definition), qt.IsNil)
	return definition
}

// TestSQLiteRebuildKeepsWhatNamesTheTableE2E rebuilds a table that a view, or
// a trigger of another table, names. From SQLite 3.26 the rename of the copy
// checks every view and trigger, and fails while the old table is dropped:
// `error in view v: no such table: main.kinds`. Both binaries here rename with
// legacy_alter_table on, so the plan applies, the object stays as it was and
// still works, and a run that follows finds nothing to do.
func TestSQLiteRebuildKeepsWhatNamesTheTableE2E(t *testing.T) {
	tests := []struct {
		name, live, desired, exclude, object, fire, query, want string
	}{
		{
			name: "a declared view", live: sqliteNamedLive + "CREATE TABLE other (x int);\n",
			desired: sqliteNamedDesired + "CREATE VIEW v AS SELECT id, name FROM kinds;\n", exclude: "other",
			object: "v", fire: "SELECT 1", query: "SELECT group_concat(id || ':' || name, ';') FROM v", want: "1:a;2:b",
		},
		{
			name: "a view out of scope", live: sqliteNamedLive, desired: sqliteNamedDesired, exclude: "v[type=view]",
			object: "v", fire: "SELECT 1", query: "SELECT group_concat(id || ':' || name, ';') FROM v", want: "1:a;2:b",
		},
		{
			name:    "a trigger of another table out of scope that writes the table",
			live:    sqliteNamedLive + "CREATE TRIGGER log_touch AFTER INSERT ON log BEGIN UPDATE kinds SET name = NEW.msg WHERE id = NEW.id; END;\n",
			desired: sqliteNamedDesired + "CREATE VIEW v AS SELECT id, name FROM kinds;\n", exclude: "log.log_touch[type=trigger]",
			object: "log_touch", fire: "INSERT INTO log VALUES (1, 'z')", query: "SELECT name FROM kinds WHERE id = 1", want: "z",
		},
		{
			name: "a trigger of the table out of scope that writes another table the plan rebuilds",
			live: "CREATE TABLE kinds (id integer PRIMARY KEY, name text);\n" +
				"CREATE TABLE log (id integer PRIMARY KEY, msg text, extra text);\n" +
				"CREATE TRIGGER kinds_gone AFTER DELETE ON kinds BEGIN INSERT INTO log(msg) VALUES (OLD.name); END;\n" +
				"INSERT INTO kinds VALUES (1, 'a'), (2, 'b');\n",
			desired: "CREATE TABLE kinds (id integer PRIMARY KEY, name text);\nCREATE TABLE log (id integer PRIMARY KEY, msg text);\n",
			exclude: "kinds.kinds_gone[type=trigger]",
			object:  "kinds_gone", fire: "DELETE FROM kinds WHERE id = 1", query: "SELECT group_concat(msg, ';') FROM log", want: "a",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			native := newSQLiteCarryFixture(c, test.live, test.desired)
			compat := newSQLiteCarryFixture(c, test.live, test.desired)
			untouched := newSQLiteCarryFixture(c, test.live, test.desired)

			nativeApplied := native.apply(c, test.exclude, "--auto-approve")
			compatApplied := compat.compatApply(c, test.exclude)
			planned := native.apply(c, test.exclude, "--dry-run")
			diffed := compat.compatDiff(c, test.exclude)

			c.Assert(nativeApplied.ExitCode, qt.Equals, 0, historyComment(nativeApplied))
			c.Assert(nativeApplied.Stdout, qt.Contains, "PRAGMA legacy_alter_table = on;")
			c.Assert(native.definition(c, test.object), qt.Equals, untouched.definition(c, test.object))
			c.Assert(native.scalar(c, test.fire, test.query), qt.Equals, test.want)
			c.Assert(compatApplied.ExitCode, qt.Equals, 0, historyComment(compatApplied))
			c.Assert(compat.definition(c, test.object), qt.Equals, untouched.definition(c, test.object))
			c.Assert(compat.scalar(c, test.fire, test.query), qt.Equals, test.want)
			c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
			c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
			c.Assert(diffed.Stdout, qt.Equals, includeSynced)
		})
	}
}
