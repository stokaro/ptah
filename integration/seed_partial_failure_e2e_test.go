//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// The seed run that stops halfway, driven through the shipped binary.
//
// A seed run is not atomic. The seeder commits each file in its own
// transaction, so a failure in the second file leaves the first one in the
// database for good. What an operator needs from that run is which work
// survived and where a re-run starts, and both are properties of the database
// rather than of the Result value the library hands back.
//
// PostgreSQL decides every assertion here. The transaction boundary is the
// server's, the primary-key violation that stops the second file is the
// server's, and whether the first file's rows outlived the failure is a
// question only a database that committed them can answer. Calling the seeder
// in process compares a Result struct against a literal: it sees neither the
// exit code, nor which stream carried the diagnostic, nor the row still
// sitting in the table.

const seedPartialSchema = "seed_partial_failure"

// The first file creates the table and writes one row. It is the work that has
// to survive.
const seedPartialFirstFile = `CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
INSERT INTO widgets (id, name) VALUES (1, 'committed');
`

// The second file writes a row and then collides with the first file's primary
// key. The leading INSERT is what makes the rollback measurable: a statement in
// the failing file ran, and the file still has to leave nothing behind.
const seedPartialSecondFileBroken = `INSERT INTO widgets (id, name) VALUES (2, 'rolled back with its file');
INSERT INTO widgets (id, name) VALUES (1, 'collides with the first file');
`

// The repair an operator makes: the colliding statement goes, the row the
// second file was always meant to write stays.
const seedPartialSecondFileFixed = `INSERT INTO widgets (id, name) VALUES (2, 'applied on the retry');
`

// TestSeedPartialFailureE2E measures what a stopped seed run leaves behind.
//
// One test rather than three, because the steps are not independent: the
// tracker contents are only meaningful on the database the failed run wrote,
// and the retry has nothing to skip without it. Splitting them would mean
// rebuilding the same state three times and asserting on a fixture rather than
// on a sequence.
//
// The seeds run under their own schema, so the `schema_seeds` tracker the
// seeder creates unqualified lands there too. The suite runs every test against
// one server, and a tracker in `public` is the one the other seeder tests drop
// and rewrite, which would make this test's tracker assertions read whatever
// ran last.
func TestSeedPartialFailureE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, realmURL(c, dbURL))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	dropSeedPartialFixture(context.WithoutCancel(ctx), conn)
	defer dropSeedPartialFixture(context.WithoutCancel(ctx), conn)
	execPostgres(ctx, c, conn, "CREATE SCHEMA "+seedPartialSchema)

	dir := c.TempDir()
	seeds := filepath.Join(dir, "seeds")
	c.Assert(os.MkdirAll(seeds, 0o755), qt.IsNil)
	writeSeedPartialFile(c, seeds, "010_widgets.test.sql", seedPartialFirstFile)
	writeSeedPartialFile(c, seeds, "020_more_widgets.test.sql", seedPartialSecondFileBroken)

	scoped := withSearchPath(c, realmURL(c, dbURL), seedPartialSchema)
	failed := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"seed", "--db-url", scoped, "--env", "test", "--seeds-dir", "seeds")

	// A seed that would not run is a command error, which the exit-code
	// contract puts at 2. Nothing but a process can say so.
	c.Assert(failed.ExitCode, qt.Equals, 2,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", failed.Stdout, failed.Stderr))

	// The diagnostic names the file that stopped the run, which is the one
	// piece of the report an operator needs to edit anything, and it carries
	// the server's own words so nobody has to take Ptah's account on trust.
	c.Assert(failed.Stderr, qt.Contains,
		"error: error applying seeds: apply seed 020_more_widgets.test.sql:")
	c.Assert(failed.Stderr, qt.Contains, "duplicate key value violates unique constraint")

	// Streams apart: a diagnostic printed on stdout would be indistinguishable
	// from a result for anything parsing the report.
	c.Assert(failed.Stdout, qt.Not(qt.Contains), "apply seed")

	// The credential reaches the command as an argument and must not come back
	// out of it.
	c.Assert(failed.Stdout+failed.Stderr, qt.Not(qt.Contains), "ptah_password")

	// The first file committed and the second did not, including the statement
	// inside it that ran before the collision. Row 2 being absent is what says
	// the file, and not the statement, is the unit that rolls back.
	c.Assert(seedPartialWidgetIDs(ctx, c, conn), qt.DeepEquals, []int{1})

	// The tracker records the work that survived and nothing else, which is
	// what makes the retry below start in the right place. Reading it through
	// the schema also proves the run created its tracker where its connection
	// pointed.
	c.Assert(seedPartialTrackedPaths(ctx, c, conn), qt.DeepEquals, []string{"010_widgets.test.sql"})

	writeSeedPartialFile(c, seeds, "020_more_widgets.test.sql", seedPartialSecondFileFixed)
	retried := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"seed", "--db-url", scoped, "--env", "test", "--seeds-dir", "seeds")

	c.Assert(retried.ExitCode, qt.Equals, 0,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", retried.Stdout, retried.Stderr))

	// The counts are the recovery report. `Skipped seeds: 1` is the sentence
	// that says the committed file was recognized rather than replayed, and a
	// run that replayed it would collide with its own row and stop again.
	c.Assert(retried.Stdout, qt.Contains, "Matching seeds: 2")
	c.Assert(retried.Stdout, qt.Contains, "Applied seeds: 1")
	c.Assert(retried.Stdout, qt.Contains, "Skipped seeds: 1")
	c.Assert(retried.Stdout, qt.Contains, "Seeds completed successfully.")

	// An edited file is refused when the tracker recorded it, so the retry
	// getting this far also says the failed file was never recorded.
	c.Assert(retried.Stderr, qt.Equals, "")

	c.Assert(seedPartialWidgetIDs(ctx, c, conn), qt.DeepEquals, []int{1, 2})
	c.Assert(seedPartialTrackedPaths(ctx, c, conn), qt.DeepEquals,
		[]string{"010_widgets.test.sql", "020_more_widgets.test.sql"})
}

// writeSeedPartialFile puts one seed file in the directory the command reads.
func writeSeedPartialFile(c *qt.C, dir, name, body string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
}

// seedPartialWidgetIDs reads the rows the seed files wrote.
func seedPartialWidgetIDs(ctx context.Context, c *qt.C, conn *dbschema.DatabaseConnection) []int {
	c.Helper()
	rows, err := conn.QueryContext(ctx,
		"SELECT id FROM "+seedPartialSchema+".widgets ORDER BY id")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()

	ids := make([]int, 0)
	for rows.Next() {
		var id int
		c.Assert(rows.Scan(&id), qt.IsNil)
		ids = append(ids, id)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return ids
}

// seedPartialTrackedPaths reads the tracker the seeder writes, which is the
// database's own answer to which seeds have run.
func seedPartialTrackedPaths(ctx context.Context, c *qt.C, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx,
		"SELECT seed_path FROM "+seedPartialSchema+".schema_seeds ORDER BY seed_path")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()

	paths := make([]string, 0)
	for rows.Next() {
		var path string
		c.Assert(rows.Scan(&path), qt.IsNil)
		paths = append(paths, path)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return paths
}

// dropSeedPartialFixture removes what this test creates, tolerating a run that
// was killed before it created all of it. The suite runs against one server, so
// a schema left behind joins every later realm-wide read.
func dropSeedPartialFixture(ctx context.Context, conn *dbschema.DatabaseConnection) {
	_ = conn.SchemaWriter().ExecuteSQL(ctx, "DROP SCHEMA IF EXISTS "+seedPartialSchema+" CASCADE")
}
