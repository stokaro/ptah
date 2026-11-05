//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
)

const (
	// failedBuildUp is the statement `migrations generate` writes for a concurrent
	// unique index: it carries no IF NOT EXISTS.
	failedBuildUp = "-- +ptah no_transaction\nCREATE UNIQUE INDEX CONCURRENTLY \"idx_members_email\" ON \"members\" (\"email\");\n"
	// failedBuildPointer is the part of the error that names what holds the name.
	failedBuildPointer = `PostgreSQL reports index "public"."idx_members_email" (indisvalid=false, indisready=false) unusable, ` +
		`as a failed concurrent build leaves it, and it holds the name; drop the index and run the migration again, ` +
		`or run REINDEX INDEX CONCURRENTLY "public"."idx_members_email" and repair with --force once the rest of the migration has run`
	// failedBuildSeed holds the duplicate that stops the build.
	failedBuildSeed = "CREATE TABLE members (id bigint PRIMARY KEY, email text);\n" +
		"INSERT INTO members VALUES (1, 'a@example.com'), (2, 'a@example.com');\n"
	// failedBuildValid counts the valid indexes named idx_members_email.
	failedBuildValid = "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'idx_members_email' AND i.indisvalid"
)

// failedBuildDir writes a migration directory of one up body.
func failedBuildDir(c *qt.C, up string) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0001_members_email.up.sql"), []byte(up), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0001_members_email.down.sql"),
		[]byte("-- +ptah no_transaction\nDROP INDEX CONCURRENTLY \"idx_members_email\";\n"), 0o600), qt.IsNil)
	return dir
}

// failedBuildExec runs statement on the database at target.
func failedBuildExec(c *qt.C, target, statement string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), statement)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
}

// failedBuildUpRun runs `migrations up` over dir with extra flags.
func failedBuildUpRun(c *qt.C, target, dir string, extra ...string) clirun.Result {
	c.Helper()
	return runPtahIn(c, append([]string{"migrations", "up", "--db-url", target, "--migrations-dir", dir}, extra...)...)
}

// failedBuildAfterFirstRun leaves the database as a failed build does, with the
// duplicate that stopped it gone. The first failure is about the data: nothing
// yet held the name when the server answered, so it names no index.
func failedBuildAfterFirstRun(c *qt.C) (target, dir string) {
	c.Helper()
	target = databaseBuiltFrom(c, failedBuildSeed)
	dir = failedBuildDir(c, failedBuildUp)
	first := failedBuildUpRun(c, target, dir)
	c.Assert(first.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
	c.Assert(first.Stderr, qt.Contains, `could not create unique index "idx_members_email"`)
	c.Assert(first.Stderr, qt.Not(qt.Contains), "unusable")
	failedBuildExec(c, target, "DELETE FROM members WHERE id = 2")
	return target, dir
}

// TestMigrationsUpOverAFailedBuildE2E_NamesTheInvalidIndex is the rerun: the server
// answers `relation ... already exists` and says nothing of the invalid index, so
// the error adds it. The index reads back invalid, and status keeps the row dirty.
func TestMigrationsUpOverAFailedBuildE2E_NamesTheInvalidIndex(t *testing.T) {
	c := qt.New(t)
	target, dir := failedBuildAfterFirstRun(c)

	rerun := failedBuildUpRun(c, target, dir, "--allow-dirty")

	c.Assert(rerun.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", rerun.Stdout, rerun.Stderr))
	c.Assert(rerun.Stderr, qt.Contains, `relation "idx_members_email" already exists (SQLSTATE 42P07)`)
	c.Assert(rerun.Stderr, qt.Contains, failedBuildPointer)
	c.Assert(readInt(c, target, failedBuildValid), qt.Equals, 0)
	status := runPtahIn(c, "migrations", "status", "--db-url", target, "--migrations-dir", dir)
	c.Assert(status.Stdout, qt.Contains, "Dirty migration state detected")
	c.Assert(status.Stdout, qt.Contains, failedBuildPointer)
}

// TestMigrationsUpOverAFailedBuildE2E_ThePointerLeadsOut follows each way out the
// error names, and reads the result back: the index is valid, and the server
// refuses a duplicate through it.
func TestMigrationsUpOverAFailedBuildE2E_ThePointerLeadsOut(t *testing.T) {
	tests := []struct {
		name, statement string
		finish          []string
	}{
		{name: "drop the index and run the migration again", statement: `DROP INDEX CONCURRENTLY "public"."idx_members_email"`,
			finish: []string{"migrations", "up", "--allow-dirty"}},
		{name: "rebuild the index and repair the migration", statement: `REINDEX INDEX CONCURRENTLY "public"."idx_members_email"`,
			finish: []string{"migrations", "repair", "--version", "1", "--force"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, dir := failedBuildAfterFirstRun(c)
			failedBuildExec(c, target, test.statement)

			finished := runPtahIn(c, append(test.finish, "--db-url", target, "--migrations-dir", dir)...)

			c.Assert(finished.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", finished.Stdout, finished.Stderr))
			c.Assert(readInt(c, target, failedBuildValid), qt.Equals, 1)
			status := runPtahIn(c, "migrations", "status", "--db-url", target, "--migrations-dir", dir)
			c.Assert(status.Stdout, qt.Not(qt.Contains), "Dirty migration state detected")
			conn, err := dbschema.ConnectToDatabase(c.Context(), target)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			_, err = conn.ExecContext(c.Context(), "INSERT INTO members VALUES (3, 'a@example.com')")
			c.Assert(err, qt.ErrorMatches, `(?s).*duplicate key value violates unique constraint "idx_members_email".*`)
		})
	}
}

// TestMigrationsUpOverAFailedBuildE2E_NamesTheIndexTheStatementResolved reads the
// name on the session that ran the statement, so a search_path the file set decides
// which schema's index the error names. A public table of the same name holds the
// schema-level name in the other schema.
func TestMigrationsUpOverAFailedBuildE2E_NamesTheIndexTheStatementResolved(t *testing.T) {
	c := qt.New(t)
	target := databaseBuiltFrom(c, "CREATE SCHEMA app;\nCREATE TABLE app.members (id bigint PRIMARY KEY, email text);\n"+
		"INSERT INTO app.members VALUES (1, 'a@example.com'), (2, 'a@example.com');\n"+
		"CREATE TABLE public.idx_members_email (id bigint PRIMARY KEY);\n")
	dir := failedBuildDir(c, "-- +ptah no_transaction\nSET search_path = app;\nCREATE UNIQUE INDEX CONCURRENTLY \"idx_members_email\" ON \"members\" (\"email\");\n")
	first := failedBuildUpRun(c, target, dir)
	c.Assert(first.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
	failedBuildExec(c, target, "DELETE FROM app.members WHERE id = 2")

	rerun := failedBuildUpRun(c, target, dir, "--allow-dirty")

	c.Assert(rerun.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", rerun.Stdout, rerun.Stderr))
	c.Assert(rerun.Stderr, qt.Contains, `PostgreSQL reports index "app"."idx_members_email" (indisvalid=false, indisready=false) unusable`)
}

// TestMigrationsUpOverAHeldNameE2E_NamesNothingIfNothingIsUnusable is the control:
// the server answers the same error, the catalog says what holds the name, and the
// error adds nothing when that is not an unusable index.
func TestMigrationsUpOverAHeldNameE2E_NamesNothingIfNothingIsUnusable(t *testing.T) {
	tests := []struct{ name, holder string }{
		{name: "a valid index", holder: "CREATE UNIQUE INDEX idx_members_email ON members (email);\n"},
		{name: "a table", holder: "CREATE TABLE idx_members_email (id bigint PRIMARY KEY);\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := databaseBuiltFrom(c, "CREATE TABLE members (id bigint PRIMARY KEY, email text);\n"+test.holder)

			run := failedBuildUpRun(c, target, failedBuildDir(c, failedBuildUp))

			c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stderr, qt.Contains, `relation "idx_members_email" already exists (SQLSTATE 42P07)`)
			c.Assert(run.Stderr, qt.Not(qt.Contains), "unusable")
		})
	}
}

// TestMigrationsUpOverAFailedBuildE2E_AFileThatFreesTheNameRuns is why the read
// waits for the server's answer. A check ahead of the run would stop this file over
// an index its own first statement removes.
func TestMigrationsUpOverAFailedBuildE2E_AFileThatFreesTheNameRuns(t *testing.T) {
	c := qt.New(t)
	target := databaseBuiltFrom(c, failedBuildSeed)
	first := failedBuildUpRun(c, target, failedBuildDir(c, failedBuildUp))
	c.Assert(first.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
	failedBuildExec(c, target, "DELETE FROM members WHERE id = 2")
	failedBuildExec(c, target, `DROP TABLE IF EXISTS schema_migrations`)

	run := failedBuildUpRun(c, target, failedBuildDir(c, "-- +ptah no_transaction\nALTER INDEX \"idx_members_email\" RENAME TO \"idx_members_email_old\";\n"+
		"DROP INDEX CONCURRENTLY \"idx_members_email_old\";\n"+
		"CREATE UNIQUE INDEX CONCURRENTLY \"idx_members_email\" ON \"members\" (\"email\");\n"))

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(readInt(c, target, failedBuildValid), qt.Equals, 1)
}
