package migrationsdrift_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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
	tgt := target{
		dir:       c.TempDir(),
		url:       "sqlite://" + filepath.Join(c.TempDir(), "target.db"),
		shadowURL: "sqlite://" + filepath.Join(c.TempDir(), "shadow.db"),
	}
	for name, content := range driftMigrations {
		c.Assert(os.WriteFile(filepath.Join(tgt.dir, name), []byte(content), 0o600), qt.IsNil)
	}
	_, stderr, code := run(c, "migrations", "up", "--db-url", tgt.url, "--migrations-dir", tgt.dir, "--to-version", version)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	tgt.exec(c, outOfBand...)
	return tgt
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
// keeps having no revision table, and every migration is pending.
func TestMigrationsDrift_HappyPathWritesNoRevisionTable(t *testing.T) {
	c := qt.New(t)
	tgt := emptyTarget(c)

	got, stderr, code := tgt.document(c)

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	c.Assert(got.CurrentVersion, qt.Equals, "")
	c.Assert(got.Pending, qt.DeepEquals, []string{"1", "2"})
	c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftClean)
	c.Assert(got.Check.Replayed, qt.HasLen, 0)
	c.Assert(tables(c, tgt.url), qt.HasLen, 0)
}

func TestMigrationsDrift_HappyPathText(t *testing.T) {
	c := qt.New(t)
	tgt := newTarget(c, "1")

	stdout, stderr, code := tgt.drift(c)

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	c.Assert(stdout, qt.Equals, "Dialect: sqlite\nCurrent version: 1\nPending migrations: 1\n  - 2\n\n"+
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
	c.Assert(stdout, qt.Equals, "Dialect: sqlite\nCurrent version: 2\nPending migrations: 0\n\n"+
		"Drift: the database differs from the schema its 2 applied migrations produce:\n"+
		"  - missing index users.users_email\n\n"+
		"Statements that would bring the database back to its history (review them; nothing ran):\n"+
		"CREATE INDEX \"users_email\" ON \"users\" (\"email\");\n")
	c.Assert(stderr, qt.Equals, "")
}

// assertCannotDecide holds a report whose comparison could not decide to the
// stage and reason it stopped at: it is a report, not a match, it exits 2, and
// the database is as it was.
func (tgt target) assertCannotDecide(c *qt.C, stage, reason string) {
	c.Helper()
	before := tables(c, tgt.url)

	got, stderr, code := tgt.document(c)

	c.Assert(code, qt.Equals, 2, qt.Commentf("%s", stderr))
	c.Assert(stderr, qt.Contains, "could not compare the database with its applied history: ")
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
