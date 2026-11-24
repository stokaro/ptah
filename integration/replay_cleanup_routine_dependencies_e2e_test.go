//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// replayCleanupEngines are the engines whose replay database the drift check
// empties with RESTRICT drops. CockroachDB refuses a drop that a routine body
// still names; PostgreSQL is the control.
var replayCleanupEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "CockroachDB", engine: dbtarget.CockroachDB},
	{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
}

// replayCleanupHistories are applied histories whose objects no fixed drop
// order empties on CockroachDB. A trigger function writes a table the cleanup
// drops before routines, and a function calls one that sorts before it by
// name. Measured on master with CockroachDB v26.4.1, `migrations drift` stops
// at `failed to drop table audit` and at `failed to drop function a`, SQLSTATE
// 2BP01, after the replay. Both compare clean once the cleanup finishes.
var replayCleanupHistories = []struct {
	name string
	up   string
}{
	{
		name: "a trigger function that writes another table",
		up: `CREATE TABLE audit (id int);
CREATE TABLE t2 (id int);
CREATE FUNCTION trg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO audit VALUES (NEW.id); RETURN NEW; END $$;
CREATE TRIGGER t2_audit AFTER INSERT ON t2 FOR EACH ROW EXECUTE FUNCTION trg();
`,
	},
	{
		name: "a function that calls one named before it",
		up: `CREATE FUNCTION a() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
CREATE FUNCTION b() RETURNS int LANGUAGE sql AS $$ SELECT a() $$;
`,
	},
}

// replayCleanupHistory is a target database with migration 1 applied and
// migration 2 pending, and an empty shadow database beside it on the same
// server.
type replayCleanupHistory struct {
	dir       string
	targetURL string
	shadowURL string
	target    *sql.DB
	shadow    *sql.DB
}

// newReplayCleanupHistory creates the two databases on engine, applies up as
// migration 1 and leaves a migration that creates pending_marker pending. Both
// databases are dropped when the test ends.
func newReplayCleanupHistory(c *qt.C, ctx context.Context, engine dbtarget.Engine, up string) replayCleanupHistory {
	c.Helper()
	adminURL := dbtarget.URL(c, engine)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	suffix := time.Now().UnixNano()
	history := replayCleanupHistory{dir: c.TempDir()}
	history.targetURL, history.target = newReplayCleanupDatabase(c, ctx, admin, adminURL,
		fmt.Sprintf("ptah_replay_cleanup_target_%d", suffix))
	history.shadowURL, history.shadow = newReplayCleanupDatabase(c, ctx, admin, adminURL,
		fmt.Sprintf("ptah_replay_cleanup_shadow_%d", suffix))

	migrations := filepath.Join(history.dir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
	writeReplayCleanupMigration(c, migrations, "0000000001_history", up, "SELECT 1;\n")
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: history.dir},
		"migrations", "up", "--db-url", history.targetURL, "--migrations-dir", migrations)
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	writeReplayCleanupMigration(c, migrations, "0000000002_pending",
		"CREATE TABLE pending_marker (id int);\n", "DROP TABLE pending_marker;\n")
	return history
}

// newReplayCleanupDatabase creates the database name through admin, whose
// address is adminURL, and drops it when the test ends. It returns the address
// ptah connects with and a driver handle for reading the database back.
func newReplayCleanupDatabase(c *qt.C, ctx context.Context, admin *sql.DB, adminURL, name string) (string, *sql.DB) {
	c.Helper()
	createE2EDatabase(c, ctx, admin, name)
	c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, name) })
	dbURL := replaceDatabaseName(c, adminURL, name)
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	return dbURL, db
}

// writeReplayCleanupMigration writes the up and down files of one migration.
func writeReplayCleanupMigration(c *qt.C, dir, name, up, down string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name+".up.sql"), []byte(up), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, name+".down.sql"), []byte(down), 0o600), qt.IsNil)
}

// userObjects counts the tables, views and routines in the public schema of
// db, the revision table aside.
func userObjects(c *qt.C, ctx context.Context, db *sql.DB) int {
	c.Helper()
	var count int
	c.Assert(db.QueryRowContext(ctx, `
		SELECT
			(SELECT count(*) FROM pg_class r JOIN pg_namespace n ON n.oid = r.relnamespace
			 WHERE n.nspname = 'public' AND r.relkind IN ('r', 'v', 'm', 'S') AND r.relname <> 'schema_migrations')
			+ (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
			   WHERE n.nspname = 'public')`,
	).Scan(&count), qt.IsNil)
	return count
}

// pendingMarkerTables counts the tables named pending_marker in db.
func pendingMarkerTables(c *qt.C, ctx context.Context, db *sql.DB) int {
	c.Helper()
	var count int
	c.Assert(db.QueryRowContext(ctx,
		"SELECT count(*) FROM pg_class WHERE relname = 'pending_marker' AND relkind = 'r'",
	).Scan(&count), qt.IsNil)
	return count
}

// TestReplayCleanupRoutineDependenciesE2E_HappyPath replays each history on the
// shadow database under `migrations drift` and under `migrations up
// --check-drift`, and reads both databases back. The report is clean, the
// pending migration runs only under `migrations up`, and the shadow database
// holds nothing afterward: the cleanup dropped a table a trigger function
// writes and a function another one calls.
func TestReplayCleanupRoutineDependenciesE2E_HappyPath(t *testing.T) {
	for _, engine := range replayCleanupEngines {
		for _, history := range replayCleanupHistories {
			t.Run(engine.name+"/"+history.name+"/migrations drift", func(t *testing.T) {
				c := qt.New(t)
				ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
				defer cancel()
				replay := newReplayCleanupHistory(c, ctx, engine.engine, history.up)

				got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: replay.dir},
					"migrations", "drift", "--db-url", replay.targetURL,
					"--migrations-dir", filepath.Join(replay.dir, "migrations"), "--shadow-db", replay.shadowURL)

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(got.Stdout, qt.Contains, "No drift: the database matches the schema its 1 applied migration produces.")
				c.Assert(userObjects(c, ctx, replay.shadow), qt.Equals, 0)
				c.Assert(pendingMarkerTables(c, ctx, replay.target), qt.Equals, 0)
			})
			t.Run(engine.name+"/"+history.name+"/migrations up --check-drift", func(t *testing.T) {
				c := qt.New(t)
				ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
				defer cancel()
				replay := newReplayCleanupHistory(c, ctx, engine.engine, history.up)

				got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: replay.dir},
					"migrations", "up", "--check-drift", "--db-url", replay.targetURL,
					"--migrations-dir", filepath.Join(replay.dir, "migrations"), "--shadow-db", replay.shadowURL)

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(userObjects(c, ctx, replay.shadow), qt.Equals, 0)
				c.Assert(pendingMarkerTables(c, ctx, replay.target), qt.Equals, 1)
			})
		}
	}
}

// withAutocommitBeforeDDLOff returns dbURL with the CockroachDB connection
// option that starts the session with autocommit_before_ddl off. The option
// is written byte for byte: url.Values spells the space as "+", and CockroachDB
// ignores the option then without a word.
func withAutocommitBeforeDDLOff(c *qt.C, dbURL string) string {
	c.Helper()
	parsed, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	query := slices.DeleteFunc([]string{parsed.RawQuery, "options=-c%20autocommit_before_ddl%3Doff"}, func(part string) bool {
		return part == ""
	})
	parsed.RawQuery = strings.Join(query, "&")
	return parsed.String()
}

// autocommitBeforeDDL reads the setting a session opened with dbURL runs with.
func autocommitBeforeDDL(c *qt.C, ctx context.Context, dbURL string) string {
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbURL))
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	var setting string
	c.Assert(db.QueryRowContext(ctx, "SHOW autocommit_before_ddl").Scan(&setting), qt.IsNil)
	return setting
}

// TestReplayCleanupRoutineDependenciesE2E_FailurePath replays, on a
// CockroachDB shadow whose session runs with autocommit_before_ddl off, a
// history where function b calls function a. The drops share the cleanup's
// transaction there, so the cleanup makes one pass: the drift check reports
// the refusal of a's drop, SQLSTATE 2BP01, and names the setting, and the
// rollback leaves the replayed objects in place. A retry would have answered
// every statement after the refusal with 25P02 in place of it.
func TestReplayCleanupRoutineDependenciesE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	replay := newReplayCleanupHistory(c, ctx, dbtarget.CockroachDB, `CREATE TABLE z (id int);
CREATE FUNCTION a() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
CREATE FUNCTION b() RETURNS int LANGUAGE sql AS $$ SELECT a() $$;
`)
	shadowURL := withAutocommitBeforeDDLOff(c, replay.shadowURL)
	c.Assert(autocommitBeforeDDL(c, ctx, shadowURL), qt.Equals, "off")

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: replay.dir},
		"migrations", "drift", "--db-url", replay.targetURL,
		"--migrations-dir", filepath.Join(replay.dir, "migrations"), "--shadow-db", shadowURL)

	c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	output := got.Stdout + got.Stderr
	c.Assert(output, qt.Contains, `cannot drop function "a" because other objects`)
	c.Assert(output, qt.Contains, "(SQLSTATE 2BP01)")
	c.Assert(output, qt.Contains, "the session runs with autocommit_before_ddl off")
	c.Assert(output, qt.Not(qt.Contains), "25P02")
	c.Assert(userObjects(c, ctx, replay.shadow), qt.Equals, 3)
}

// TestReplayCleanupProcedureHistoryE2E_HappyPath replays the history
// stokaro/ptah#6568 reports, a procedure that writes a table, and empties the
// shadow database afterward on both engines. On CockroachDB the comparison
// that follows reports drift, because the server qualifies the procedure body
// with the database's name (stokaro/ptah#6581); the row pins that exit status
// so the cleanup's success is not read from it.
func TestReplayCleanupProcedureHistoryE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		engine   dbtarget.Engine
		wantExit int
	}{
		{name: "CockroachDB", engine: dbtarget.CockroachDB, wantExit: 1},
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL, wantExit: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
			defer cancel()
			replay := newReplayCleanupHistory(c, ctx, test.engine, `CREATE TABLE p (id int);
CREATE PROCEDURE pc() LANGUAGE plpgsql AS $$ BEGIN INSERT INTO p VALUES (1); END $$;
`)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: replay.dir},
				"migrations", "drift", "--db-url", replay.targetURL,
				"--migrations-dir", filepath.Join(replay.dir, "migrations"), "--shadow-db", replay.shadowURL)

			c.Assert(got.ExitCode, qt.Equals, test.wantExit, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout+got.Stderr, qt.Not(qt.Contains), "empty the database after the replay")
			c.Assert(userObjects(c, ctx, replay.shadow), qt.Equals, 0)
		})
	}
}
