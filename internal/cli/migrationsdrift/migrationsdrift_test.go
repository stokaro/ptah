package migrationsdrift_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/migrationsdrift"
	"ptah.run/internal/cli/root"
	"ptah.run/migration/migrator"
)

// driftMigrations is a history whose second migration adds an index, so a
// target at version 1 has the index pending and must not read it as drift.
var driftMigrations = map[string]string{
	"0000000001_users.up.sql":         "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);\n",
	"0000000001_users.down.sql":       "DROP TABLE users;\n",
	"0000000002_users_email.up.sql":   "CREATE INDEX users_email ON users (email);\n",
	"0000000002_users_email.down.sql": "DROP INDEX users_email;\n",
}

// target is a SQLite database migrated to a version of driftMigrations, with
// statements run on it afterwards outside any migration, and an empty shadow
// database of its own.
type target struct {
	dir, url, shadowURL string
}

func newTarget(c *qt.C, version string, outOfBand ...string) target {
	c.Helper()
	tgt := emptyTarget(c)
	tgt.up(c, version)
	tgt.exec(c, outOfBand...)
	return tgt
}

// up applies the target's directory to version, with extra flags for the run.
func (tgt target) up(c *qt.C, version string, extra ...string) {
	c.Helper()
	_, stderr, code := run(c, append([]string{
		"migrations", "up", "--db-url", tgt.url, "--migrations-dir", tgt.dir, "--to-version", version,
	}, extra...)...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
}

// write replaces the content of the migration file name in the target's
// directory.
func (tgt target) write(c *qt.C, name, content string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(tgt.dir, name), []byte(content), 0o600), qt.IsNil)
}

// backslashShadow is a shadow database URL whose path holds a backslash on
// every platform, as each Windows path does, so a fixture that writes it into
// atlas.hcl without quoting it fails on every platform: `\U` is an HCL escape.
func backslashShadow(c *qt.C) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), `a\Users`)
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	return "sqlite://" + filepath.Join(dir, "shadow.db")
}

// config writes a project file named name into a directory of its own and
// returns its path.
func config(c *qt.C, name, content string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), name)
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	return path
}

// emptyTarget is a database no migration ever ran on: it has no revision
// table.
func emptyTarget(c *qt.C) target {
	c.Helper()
	tgt := target{
		dir:       c.TempDir(),
		url:       "sqlite://" + filepath.Join(c.TempDir(), "target.db"),
		shadowURL: "sqlite://" + filepath.Join(c.TempDir(), "shadow.db"),
	}
	for name, content := range driftMigrations {
		c.Assert(os.WriteFile(filepath.Join(tgt.dir, name), []byte(content), 0o600), qt.IsNil)
	}
	return tgt
}

func (tgt target) exec(c *qt.C, statements ...string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), tgt.url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range statements {
		_, err := conn.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// tables lists the tables the database at url holds, in name order.
func tables(c *qt.C, url string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(context.Background(),
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	names := make([]string, 0)
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// run drives the shipped command tree and returns both streams and the exit
// status.
func run(c *qt.C, args ...string) (stdout, stderr string, code int) {
	c.Helper()
	cmd := root.NewRootCommand()
	var out, diagnostics bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&diagnostics)
	code = root.RunContext(context.Background(), cmd, args...)
	return out.String(), diagnostics.String(), code
}

func (tgt target) drift(c *qt.C, extra ...string) (stdout, stderr string, code int) {
	c.Helper()
	return run(c, append([]string{
		"migrations", "drift", "--db-url", tgt.url, "--migrations-dir", tgt.dir, "--shadow-db", tgt.shadowURL,
	}, extra...)...)
}

// document is the report with its diff left undecoded: the diff is the
// planner's input, and these tests read what the report says about it.
type document struct {
	migrationsdrift.Document
	Diff json.RawMessage `json:"diff"`
}

func (tgt target) document(c *qt.C, extra ...string) (document, string, int) {
	c.Helper()
	stdout, stderr, code := tgt.drift(c, append([]string{"--json"}, extra...)...)
	var got document
	c.Assert(json.Unmarshal([]byte(stdout), &got), qt.IsNil, qt.Commentf("stdout: %s\nstderr: %s", stdout, stderr))
	return got, stderr, code
}

func TestMigrationsDrift_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name      string
		version   string
		outOfBand []string
		extra     []string
		replayed  []int64
		current   string
		pending   []string
	}{
		{
			name: "a pending index is not drift", version: "1",
			replayed: []int64{1}, current: "1", pending: []string{"2"},
		},
		{
			name: "an up-to-date target", version: "2",
			replayed: []int64{1, 2}, current: "2",
		},
		{
			name: "an out-of-band table the run excludes", version: "1",
			outOfBand: []string{"CREATE TABLE scratch (id INTEGER PRIMARY KEY)"},
			extra:     []string{"--exclude", "scratch"},
			replayed:  []int64{1}, current: "1", pending: []string{"2"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			tgt := newTarget(c, test.version, test.outOfBand...)
			before := tables(c, tgt.url)

			got, stderr, code := tgt.document(c, test.extra...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
			c.Assert(got.ContractVersion, qt.Equals, migrationsdrift.ContractVersion)
			c.Assert(got.Command, qt.Equals, migrationsdrift.Command)
			c.Assert(got.Dialect, qt.Equals, "sqlite")
			c.Assert(got.CurrentVersion, qt.Equals, test.current)
			c.Assert(got.Pending, qt.DeepEquals, test.pending)
			c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftClean)
			c.Assert(got.Check.Replayed, qt.DeepEquals, test.replayed)
			c.Assert(got.Check.Findings, qt.HasLen, 0)
			c.Assert(got.Diff, qt.IsNil)
			c.Assert(got.Statements, qt.HasLen, 0)
			c.Assert(tables(c, tgt.url), qt.DeepEquals, before)
			c.Assert(tables(c, tgt.shadowURL), qt.HasLen, 0)
		})
	}
}

// A report writes nothing to the database: a database no migration ran on
// keeps having no revision table, and every migration is pending. It holds no
// object either, so it is not drift and gets no warning about its history.
func TestMigrationsDrift_HappyPathWritesNoRevisionTable(t *testing.T) {
	c := qt.New(t)
	tgt := emptyTarget(c)

	got, stderr, code := tgt.document(c)

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	c.Assert(got.CurrentVersion, qt.Equals, "")
	c.Assert(got.Pending, qt.DeepEquals, []string{"1", "2"})
	c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftClean)
	c.Assert(got.Check.Replayed, qt.HasLen, 0)
	c.Assert(got.Warnings, qt.HasLen, 0)
	c.Assert(tables(c, tgt.url), qt.HasLen, 0)
}

func TestMigrationsDrift_HappyPathText(t *testing.T) {
	c := qt.New(t)
	tgt := newTarget(c, "1")

	stdout, stderr, code := tgt.drift(c)

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	c.Assert(stdout, qt.Equals, "Dialect: sqlite\nRevision table: \"schema_migrations\"\nCurrent version: 1\n"+
		"Pending migrations: 1\n  - 2\n\n"+
		"No drift: the database matches the schema its 1 applied migration produces.\n")
	c.Assert(stderr, qt.Equals, "")
}

func TestMigrationsDrift_FailurePathReportsDrift(t *testing.T) {
	for _, test := range []struct {
		name      string
		version   string
		outOfBand []string
		findings  []migrator.DriftFinding
	}{
		{
			name: "a column no migration added", version: "1",
			outOfBand: []string{"ALTER TABLE users ADD COLUMN nickname TEXT"},
			findings: []migrator.DriftFinding{{
				Kind: "extra_column", Object: "users.nickname", Table: "users", Column: "nickname",
				Message: "extra column users.nickname",
			}},
		},
		{
			name: "an index an applied migration created and the database lost", version: "2",
			outOfBand: []string{"DROP INDEX users_email"},
			findings: []migrator.DriftFinding{{
				Kind: "missing_index", Object: "users.users_email", Table: "users",
				Message: "missing index users.users_email",
			}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			tgt := newTarget(c, test.version, test.outOfBand...)
			before := tables(c, tgt.url)

			got, stderr, code := tgt.document(c)

			c.Assert(code, qt.Equals, 1, qt.Commentf("%s", stderr))
			c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftDrifted)
			c.Assert(got.Check.Findings, qt.DeepEquals, test.findings)
			c.Assert(got.Diff, qt.IsNotNil)
			c.Assert(got.PlanError, qt.Equals, "")
			c.Assert(got.Statements, qt.Not(qt.HasLen), 0)
			c.Assert(tables(c, tgt.url), qt.DeepEquals, before)
			c.Assert(tables(c, tgt.shadowURL), qt.HasLen, 0)

			// The statements are what the report says they are: run on the
			// database, they bring it back to its history.
			tgt.exec(c, got.Statements...)
			again, stderr, code := tgt.document(c)
			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
			c.Assert(again.Check.Outcome, qt.Equals, migrator.DriftClean)
		})
	}
}

func TestMigrationsDrift_FailurePathReportsDriftText(t *testing.T) {
	c := qt.New(t)
	tgt := newTarget(c, "2", "DROP INDEX users_email")

	stdout, stderr, code := tgt.drift(c)

	c.Assert(code, qt.Equals, 1, qt.Commentf("%s", stderr))
	c.Assert(stdout, qt.Equals, "Dialect: sqlite\nRevision table: \"schema_migrations\"\nCurrent version: 2\nPending migrations: 0\n\n"+
		"Drift: the database differs from the schema its 2 applied migrations produce:\n"+
		"  - missing index users.users_email\n\n"+
		"Statements that would bring the database back to its history (review them; nothing ran):\n"+
		"CREATE INDEX \"users_email\" ON \"users\" (\"email\");\n")
	c.Assert(stderr, qt.Equals, "")
}

// assertCannotDecide holds a report whose comparison could not decide to the
// stage and reason it stopped at: it is a report, not a match, it exits 2, the
// report alone says why, and the database is as it was.
func (tgt target) assertCannotDecide(c *qt.C, stage, reason string) {
	c.Helper()
	before := tables(c, tgt.url)

	got, stderr, code := tgt.document(c)

	c.Assert(code, qt.Equals, 2, qt.Commentf("%s", stderr))
	c.Assert(stderr, qt.Equals, "")
	c.Assert(got.Check.Message, qt.Not(qt.Equals), "")
	c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftUnable)
	c.Assert(got.Check.Stage, qt.Equals, stage)
	c.Assert(got.Check.Reason, qt.Equals, reason)
	c.Assert(got.Diff, qt.IsNil)
	c.Assert(tables(c, tgt.url), qt.DeepEquals, before)
}

func TestMigrationsDrift_FailurePathCannotDecideOnTheTargetAsShadow(t *testing.T) {
	c := qt.New(t)
	tgt := newTarget(c, "1")
	tgt.shadowURL = tgt.url

	tgt.assertCannotDecide(c, "realm-check", "target_shadow_same_realm")
}

func TestMigrationsDrift_FailurePathCannotDecideWithoutAnAppliedFile(t *testing.T) {
	c := qt.New(t)
	tgt := newTarget(c, "1")
	c.Assert(os.Remove(filepath.Join(tgt.dir, "0000000001_users.up.sql")), qt.IsNil)
	c.Assert(os.Remove(filepath.Join(tgt.dir, "0000000001_users.down.sql")), qt.IsNil)

	tgt.assertCannotDecide(c, "history", migrator.UnreplayableMissingFile)
}

// An applied file that changed since it ran is not the history the database
// has, whatever the edit: a replay of it would offer to bring the database to
// a schema it never had. The comment-only edit is the one a schema comparison
// alone would call clean.
func TestMigrationsDrift_FailurePathCannotDecideOnAChangedAppliedFile(t *testing.T) {
	for _, test := range []struct {
		name  string
		users string
	}{
		{name: "a column added to the file", users: "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL, age INTEGER);\n"},
		{name: "a comment added to the file", users: "-- the users\n" + driftMigrations["0000000001_users.up.sql"]},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			tgt := newTarget(c, "1")
			tgt.write(c, "0000000001_users.up.sql", test.users)

			tgt.assertCannotDecide(c, "history", migrator.UnreplayableChanged)
		})
	}
}

// The text report says once why the comparison could not decide: the report
// on standard output, and nothing more on standard error.
func TestMigrationsDrift_FailurePathCannotDecideText(t *testing.T) {
	c := qt.New(t)
	tgt := newTarget(c, "1")
	tgt.write(c, "0000000001_users.up.sql", "-- the users\n"+driftMigrations["0000000001_users.up.sql"])

	stdout, stderr, code := tgt.drift(c)

	c.Assert(code, qt.Equals, 2)
	c.Assert(stdout, qt.Matches, `(?s)Dialect: sqlite\n.*\n\nCould not compare the database with its applied history: `+
		`the applied history cannot be replayed: version 1: the checksum it recorded when it applied is \S+ `+
		`and the directory's is \S+: its file, or the checksums the directory carries, changed after it applied\n`)
	c.Assert(stderr, qt.Equals, "")
}

// A migration that ran against an object no migration created replays on
// the empty shadow database and fails there. The report cannot decide, and
// says the replay is why.
func TestMigrationsDrift_FailurePathCannotDecideWhenTheReplayFails(t *testing.T) {
	c := qt.New(t)
	tgt := emptyTarget(c)
	tgt.write(c, "0000000003_legacy_name.up.sql", "CREATE INDEX legacy_name ON legacy (name);\n")
	tgt.write(c, "0000000003_legacy_name.down.sql", "DROP INDEX legacy_name;\n")
	tgt.exec(c, "CREATE TABLE legacy (id INTEGER PRIMARY KEY, name TEXT)")
	tgt.up(c, "3")

	tgt.assertCannotDecide(c, "replay", "replay_error")
}

// A table the project leaves out is not drift, and an atlas.hcl drift block
// that sets its own exclude list replaces the env's for the report as it does
// for `migrations up`. The env's dev database is the shadow the report
// replays on when no --shadow-db is given.
func TestMigrationsDrift_HappyPathReadsTheProject(t *testing.T) {
	for _, test := range []struct {
		name       string
		envExclude string
		drift      string
	}{
		{name: "the env's exclude", envExclude: `["scratch"]`},
		{name: "a drift block exclude replaces the env's", envExclude: `["orders_archive"]`, drift: "      exclude = [\"scratch\"]\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			tgt := newTarget(c, "1", "CREATE TABLE scratch (id INTEGER PRIMARY KEY)")
			path := config(c, "atlas.hcl", "env \"local\" {\n"+
				"  dev = "+strconv.Quote(backslashShadow(c))+"\n"+
				"  exclude = "+test.envExclude+"\n"+
				"  check \"migrate_apply\" {\n    drift {\n"+test.drift+"    }\n  }\n"+
				"}\n")

			stdout, stderr, code := run(c, "migrations", "drift", "--json", "--db-url", tgt.url,
				"--migrations-dir", tgt.dir, "--config", path, "--env", "local")

			c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			var got document
			c.Assert(json.Unmarshal([]byte(stdout), &got), qt.IsNil)
			c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftClean)
		})
	}
}

// A drift block exclude replaces the env's list rather than adding to it: the
// table the env leaves out is compared again, and is drift.
func TestMigrationsDrift_FailurePathADriftBlockExcludeReplacesTheEnvs(t *testing.T) {
	c := qt.New(t)
	tgt := newTarget(c, "1", "CREATE TABLE scratch (id INTEGER PRIMARY KEY)")
	path := config(c, "atlas.hcl", "env \"local\" {\n"+
		"  dev = "+strconv.Quote(backslashShadow(c))+"\n"+
		"  exclude = [\"scratch\"]\n"+
		"  check \"migrate_apply\" {\n    drift {\n      exclude = [\"orders_archive\"]\n    }\n  }\n"+
		"}\n")

	stdout, stderr, code := run(c, "migrations", "drift", "--json", "--db-url", tgt.url,
		"--migrations-dir", tgt.dir, "--config", path, "--env", "local")

	c.Assert(code, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	var got document
	c.Assert(json.Unmarshal([]byte(stdout), &got), qt.IsNil)
	c.Assert(got.Check.Findings, qt.DeepEquals, []migrator.DriftFinding{
		{Kind: "extra_table", Object: "scratch", Table: "scratch", Message: "extra table scratch"},
		{
			Kind: "extra_constraint", Object: "scratch.scratch_pkey", Table: "scratch", Constraint: "scratch_pkey",
			Message: "extra constraint scratch.scratch_pkey",
		},
	})
}

// A selector of the drift block that matches nothing is reported under the
// block's own place in atlas.hcl, so the warning points at the list the
// report used rather than at the env's.
func TestMigrationsDrift_HappyPathNamesTheDriftBlockOfAnUnmatchedSelector(t *testing.T) {
	c := qt.New(t)
	tgt := newTarget(c, "1")
	path := config(c, "atlas.hcl", "env \"local\" {\n"+
		"  dev = "+strconv.Quote(backslashShadow(c))+"\n"+
		"  exclude = [\"orders_archive\"]\n"+
		"  check \"migrate_apply\" {\n    drift {\n      exclude = [\"nothing_here\"]\n    }\n  }\n"+
		"}\n")

	stdout, stderr, code := run(c, "migrations", "drift", "--db-url", tgt.url,
		"--migrations-dir", tgt.dir, "--config", path, "--env", "local")

	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stderr, qt.Matches, `Warning: the exclude selection from atlas\.hcl check "migrate_apply" drift at .+:5 `+
		`matched no objects: "nothing_here"\.\n`)
}

// A revision table under another name is the one the report reads, and the
// one it leaves out of the comparison: without that, the table itself would
// be drift.
func TestMigrationsDrift_HappyPathReadsAConfiguredRevisionTable(t *testing.T) {
	c := qt.New(t)
	tgt := emptyTarget(c)
	tgt.up(c, "1", "--migrations-table", "app_revisions")

	got, stderr, code := tgt.document(c, "--migrations-table", "app_revisions")

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	c.Assert(got.RevisionTable, qt.Equals, `"app_revisions"`)
	c.Assert(got.CurrentVersion, qt.Equals, "1")
	c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftClean)
	c.Assert(got.Warnings, qt.HasLen, 0)
}

// A report that reads the wrong revision table finds no applied migration, so
// every object reads as drift and the statements drop all of them, the
// history's own revision table included. The exit status stands, and the
// report says so before anyone runs them.
func TestMigrationsDrift_FailurePathWarnsOfAnEmptyRevisionTable(t *testing.T) {
	c := qt.New(t)
	tgt := emptyTarget(c)
	tgt.up(c, "1", "--migrations-table", "app_revisions")

	got, stderr, code := tgt.document(c)

	c.Assert(code, qt.Equals, 1, qt.Commentf("%s", stderr))
	c.Assert(got.RevisionTable, qt.Equals, `"schema_migrations"`)
	c.Assert(got.CurrentVersion, qt.Equals, "")
	c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftDrifted)
	c.Assert(got.Warnings, qt.DeepEquals, []string{
		`the revision table "schema_migrations" records no applied migration, or does not exist, and the database holds objects: ` +
			"if the history is recorded elsewhere, name its table with --migrations-table and --migrations-schema, " +
			"and its format with --revision-format; the statements below would drop every object the report compares",
	})
	c.Assert(got.Statements, qt.Contains, `DROP TABLE "app_revisions"`)

	stdout, _, code := tgt.drift(c)

	c.Assert(code, qt.Equals, 1)
	c.Assert(stdout, qt.Contains, "\n\nWarning: the revision table \"schema_migrations\" records no applied migration")
}

// A history written in one revision format and read in the other is the
// likeliest reason a database with objects records no applied migration, so
// the report names the revision table of the other format and the flag that
// reads it. Read with that flag, the same history is clean.
func TestMigrationsDrift_FailurePathNamesAHistoryOfTheOtherFormat(t *testing.T) {
	for _, test := range []struct {
		name                    string
		written, read           string
		writtenTable, readTable string
	}{
		{
			name: "an atlas history read as ptah", written: "atlas", read: "ptah",
			writtenTable: `"atlas_schema_revisions"`, readTable: `"schema_migrations"`,
		},
		{
			name: "a ptah history read as atlas", written: "ptah", read: "atlas",
			writtenTable: `"schema_migrations"`, readTable: `"atlas_schema_revisions"`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			tgt := emptyTarget(c)
			tgt.up(c, "1", "--revision-format", test.written)

			got, stderr, code := tgt.document(c, "--revision-format", test.read)

			c.Assert(code, qt.Equals, 1, qt.Commentf("%s", stderr))
			c.Assert(got.RevisionTable, qt.Equals, test.readTable)
			c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftDrifted)
			c.Assert(got.Warnings, qt.DeepEquals, []string{
				"the revision table " + test.readTable + " records no applied migration, or does not exist, " +
					"but the " + test.written + "-format revision table " + test.writtenTable + " records 1 applied migration: " +
					"run the report again with --revision-format " + test.written + "; " +
					"the statements below would drop every object the report compares",
			})

			got, stderr, code = tgt.document(c, "--revision-format", test.written)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
			c.Assert(got.RevisionTable, qt.Equals, test.writtenTable)
			c.Assert(got.CurrentVersion, qt.Equals, "1")
			c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftClean)
			c.Assert(got.Warnings, qt.HasLen, 0)
		})
	}
}

// A run that cannot start prints the failure document instead of a report,
// and opens nothing.
func TestMigrationsDrift_FailurePathRefusesBeforeConnecting(t *testing.T) {
	for _, test := range []struct {
		name  string
		args  []string
		error string
	}{
		{
			name:  "no shadow database",
			args:  []string{"--db-url", "sqlite://unused.db", "--migrations-dir", "."},
			error: "--shadow-db is required: the applied history is replayed on a disposable shadow database",
		},
		{
			name:  "no target",
			args:  []string{"--migrations-dir", ".", "--shadow-db", "sqlite://shadow.db"},
			error: "database URL is required",
		},
		{
			name: "a lock timeout SQLite cannot honor",
			args: []string{
				"--db-url", "sqlite://unused.db", "--migrations-dir", ".", "--shadow-db", "sqlite://shadow.db",
				"--migration-lock-timeout", "5s",
			},
			error: `--migration-lock-timeout requested the migration advisory lock, and dialect "sqlite" has none: ` +
				"only postgres, mysql, mariadb, sqlserver, yugabytedb, oracle take a session advisory lock. " +
				"Remove --migration-lock-timeout to run without a lock",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			c.Chdir(dir)

			stdout, stderr, code := run(c, append([]string{"migrations", "drift", "--json"}, test.args...)...)

			c.Assert(code, qt.Equals, 2)
			var failure struct {
				ContractVersion int    `json:"contract_version"`
				Command         string `json:"command"`
				Error           struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			c.Assert(json.Unmarshal([]byte(stdout), &failure), qt.IsNil, qt.Commentf("%s", stdout))
			c.Assert(failure.Command, qt.Equals, migrationsdrift.Command)
			c.Assert(failure.Error.Message, qt.Equals, test.error)
			c.Assert(stderr, qt.Contains, test.error)
			entries, err := os.ReadDir(dir)
			c.Assert(err, qt.IsNil)
			c.Assert(entries, qt.HasLen, 0)
		})
	}
}
