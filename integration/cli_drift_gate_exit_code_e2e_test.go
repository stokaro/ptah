//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// The exit status a continuous-integration drift gate reads, driven through the
// shipped binary against a live PostgreSQL server.
//
// `ptah schema compare --exit-code` and `ptah migrations status --exit-code`
// each turn a finding into process status 1, and each decision sits in an
// unexported helper -- compare.nonEmptyDiffExitCode and
// migratestatus.pendingMigrationsExitCode. Both are called from one place in
// production and asserted on directly from a white-box test in their own
// package, so deleting the call site leaves every test green while the gate
// reports success on a drifted database forever. A process is the only place
// the two halves meet: the helper's error has to survive Cobra, the exit-code
// normalizer and the root command before it becomes a number a shell reads.
//
// The stream assertions carry the other half of the contract. An error that
// carries an explicit exit code is deliberately not printed as a diagnostic,
// so a gate firing on a real difference writes its findings to standard output
// and leaves standard error empty; a diagnostic appearing there would mean the
// run failed rather than found something.
//
// PostgreSQL rather than the compiled-in engine because the comparison is what
// produces the difference, and it is the live catalog that answers what the
// database actually carries.

// compareExitCodeEntities declares one table. The database is moved around it
// between the runs below, so the same declaration compares against a drifted
// state and then against a converged one.
const compareExitCodeEntities = `package entities

//ptah:schema:table name="gates"
type Gate struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int

	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string

	//ptah:schema:field name="region" type="TEXT"
	Region string
}
`

// exitCodeMigrationUp and exitCodeMigrationDown are the one pending migration
// `migrations status` reports on. The table is not the subject; whether the
// database has caught up with the directory is.
const (
	exitCodeMigrationUp   = "CREATE TABLE ledgers (id INTEGER PRIMARY KEY, label TEXT NOT NULL);\n"
	exitCodeMigrationDown = "DROP TABLE ledgers;\n"
)

// TestSchemaCompareExitCodeE2E holds both halves of the `--exit-code` contract
// on `ptah schema compare`.
//
// The drifted run is the half no offline test reaches: exit 1 has to come out
// of the process, with the category naming the table on standard output. The
// converged run is its control -- without it, a compare that exited 1 on every
// invocation would satisfy the first row just as well.
func TestSchemaCompareExitCodeE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { adminDB.Close() })

	databaseName := fmt.Sprintf("ptah_compare_exit_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, databaseName)
	t.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, databaseName) })
	scopedURL := replaceDatabaseName(c, dbURL, databaseName)

	targetDB, err := sql.Open("pgx", scopedURL)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { targetDB.Close() })

	workDir := c.TempDir()
	root := filepath.Join(workDir, "entities")
	c.Assert(os.MkdirAll(root, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(compareExitCodeEntities), 0o600), qt.IsNil)

	// Each row carries the statement that puts the database into the state it
	// names, so the two runs differ in the database and in nothing else: same
	// declaration, same flags, same working directory.
	steps := []struct {
		name       string
		prepare    string
		wantExit   int
		wantStdout string
	}{
		{
			name:       "a column the database does not carry",
			prepare:    `CREATE TABLE gates (id SERIAL PRIMARY KEY, name TEXT NOT NULL)`,
			wantExit:   1,
			wantStdout: "tables_modified (1): gates",
		},
		{
			name:       "the database has caught up with the declaration",
			prepare:    `ALTER TABLE gates ADD COLUMN region TEXT`,
			wantExit:   0,
			wantStdout: "No schema differences detected.",
		},
	}

	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			c := qt.New(t)
			execExitCodeSQL(c, ctx, targetDB, step.prepare)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir},
				"schema", "compare", "--db-url", scopedURL, "--root-dir", root, "--exit-code")

			c.Assert(got.ExitCode, qt.Equals, step.wantExit,
				qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Equals, "")
			c.Assert(got.Stdout, qt.Contains, step.wantStdout)
		})
	}
}

// TestMigrationsStatusExitCodeE2E holds the same contract on `ptah migrations
// status`, the other verb a deployment gate reads.
//
// The sequence is not a table because the state between the runs is produced by
// a command rather than by a statement: `migrations up` is what makes the
// pending migration applied, and asserting on a status run that never followed
// one would measure a fixture instead of the verb.
func TestMigrationsStatusExitCodeE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { adminDB.Close() })

	databaseName := fmt.Sprintf("ptah_status_exit_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, databaseName)
	t.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, databaseName) })
	scopedURL := replaceDatabaseName(c, dbURL, databaseName)

	workDir := c.TempDir()
	migrations := filepath.Join(workDir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrations, "0000000001_create_ledgers.up.sql"),
		[]byte(exitCodeMigrationUp), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrations, "0000000001_create_ledgers.down.sql"),
		[]byte(exitCodeMigrationDown), 0o600), qt.IsNil)

	pending := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir},
		"migrations", "status", "--migrations-dir", migrations, "--db-url", scopedURL, "--exit-code")

	c.Assert(pending.ExitCode, qt.Equals, 1,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", pending.Stdout, pending.Stderr))
	c.Assert(pending.Stderr, qt.Equals, "")
	c.Assert(pending.Stdout, qt.Contains, "Pending Migrations: 1")
	c.Assert(pending.Stdout, qt.Contains, "Applied Migrations: 0")

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir},
		"migrations", "up", "--migrations-dir", migrations, "--db-url", scopedURL)
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))

	// The control. Without it a status that exited 1 on every run -- because the
	// flag was read as "always fail" rather than as "fail on pending" -- would
	// satisfy the assertions above.
	upToDate := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir},
		"migrations", "status", "--migrations-dir", migrations, "--db-url", scopedURL, "--exit-code")

	c.Assert(upToDate.ExitCode, qt.Equals, 0,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", upToDate.Stdout, upToDate.Stderr))
	c.Assert(upToDate.Stderr, qt.Equals, "")
	c.Assert(upToDate.Stdout, qt.Contains, "Pending Migrations: 0")
	c.Assert(upToDate.Stdout, qt.Contains, "Applied Migrations: 1")
}

// execExitCodeSQL runs one statement against the scoped database, reporting the
// statement itself when the server refuses it: a setup failure here would
// otherwise read as the command under test finding nothing.
func execExitCodeSQL(c *qt.C, ctx context.Context, db *sql.DB, statement string) {
	c.Helper()
	_, err := db.ExecContext(ctx, statement)
	c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
}
