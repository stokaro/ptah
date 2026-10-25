//go:build integration

package atlas_test

import (
	"bytes"
	"context"
	"database/sql"
	"maps"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/dbtarget"
)

// `ptah-compat migrate apply` runs a golang-migrate directory the way
// golang-migrate runs it, which opens no transaction around a file
// (stokaro/ptah#4962). A file with its own BEGIN and COMMIT runs outside any
// transaction of the migrator's, so the file's COMMIT commits its own work and
// the revision row records what committed. A file that holds one CREATE INDEX
// CONCURRENTLY runs without a transaction, as golang-migrate sends it.
//
// Wrapped in the migrator's transaction, the first file nests: PostgreSQL
// ignores its BEGIN with a warning, its COMMIT commits the migrator's
// transaction too, and when a later statement fails the run treats the file as
// rolled back and drops its revision row while the table stays. The community
// binary nests the same way; that is a defect it has, not a contract.

// applyGolangMigrateDirectory hashes files as a golang-migrate directory,
// applies it to a fresh database and returns that database, the command's
// output and its error.
func applyGolangMigrateDirectory(c *qt.C, adminURL string, files map[string]string) (*sql.DB, string, error) {
	c.Helper()
	run := newGolangMigrateApply(c, adminURL, files)
	out, err := run.apply()
	return run.db, out, err
}

// golangMigrateApply is a golang-migrate directory and the fresh database it
// is applied to, for a test that applies it more than once.
type golangMigrateApply struct {
	db     *sql.DB
	dirURL string
	dbURL  string
}

// newGolangMigrateApply hashes files as a golang-migrate directory and creates
// the database it is applied to.
func newGolangMigrateApply(c *qt.C, adminURL string, files map[string]string) *golangMigrateApply {
	c.Helper()
	dir := writeHashedLayoutDirectory(c, "golang-migrate", files)
	dbURL := newCleanGateDatabase(c, adminURL, nil)
	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	return &golangMigrateApply{db: db, dirURL: "file://" + dir + "?format=golang-migrate", dbURL: dbURL}
}

// apply runs `migrate apply` over the directory with flags, and returns the
// command's output and its error.
func (r *golangMigrateApply) apply(flags ...string) (string, error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(append([]string{"migrate", "apply", "--dir", r.dirURL, "--url", r.dbURL}, flags...))
	err := cmd.Execute()
	return out.String(), err
}

// latestRevision returns what the revision row of version records: how many
// statements it applied, -1 when there is no row, and its error.
func latestRevision(c *qt.C, db *sql.DB, version string) (int, string) {
	c.Helper()
	var applied int
	var failure string
	c.Assert(db.QueryRowContext(context.Background(),
		"SELECT COALESCE(max(applied), -1), COALESCE(max(error), '') "+
			"FROM atlas_schema_revisions.atlas_schema_revisions WHERE version = $1", version,
	).Scan(&applied, &failure), qt.IsNil)
	return applied, failure
}

// tableExistsIn reports whether name is a table db can see.
func tableExistsIn(c *qt.C, db *sql.DB, name string) bool {
	c.Helper()
	var exists bool
	c.Assert(db.QueryRowContext(context.Background(), "SELECT to_regclass($1) IS NOT NULL", name).Scan(&exists), qt.IsNil)
	return exists
}

// golangMigrateBase is the first migration of every directory here.
var golangMigrateBase = map[string]string{
	"20240826120919_init.up.sql":   "CREATE TABLE base (id int);\n",
	"20240826120919_init.down.sql": "DROP TABLE base;\n",
}

// withGolangMigrateBase returns golangMigrateBase and files together.
func withGolangMigrateBase(files map[string]string) map[string]string {
	all := maps.Clone(golangMigrateBase)
	maps.Copy(all, files)
	return all
}

func TestMigrateApplyGolangMigrateOwnTransactions_HappyPathLive(t *testing.T) {
	c := qt.New(t)
	db, out, err := applyGolangMigrateDirectory(c, dbtarget.URL(c, dbtarget.PostgreSQL), withGolangMigrateBase(map[string]string{
		"20240827000000_backfill.up.sql":   "BEGIN;\nCREATE TABLE t1 (id int);\nINSERT INTO t1 VALUES (1);\nCOMMIT;\n",
		"20240827000000_backfill.down.sql": "BEGIN;\nDROP TABLE t1;\nCOMMIT;\n",
	}))

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
	var rows int
	c.Assert(db.QueryRowContext(context.Background(), "SELECT count(*) FROM t1").Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, 1)
	applied, failure := latestRevision(c, db, "20240827000000")
	c.Assert(applied, qt.Equals, 4)
	c.Assert(failure, qt.Equals, "")
}

// The file's own transaction commits before a later statement fails. The
// table it created stays, and the revision row records the three statements
// that committed, with the failure.
func TestMigrateApplyGolangMigrateOwnTransactions_FailureAfterTheFileCommitsLive(t *testing.T) {
	c := qt.New(t)
	db, out, err := applyGolangMigrateDirectory(c, dbtarget.URL(c, dbtarget.PostgreSQL), withGolangMigrateBase(map[string]string{
		"20240828000000_fail.up.sql":   "BEGIN;\nCREATE TABLE t2 (id int);\nCOMMIT;\nINSERT INTO missing_table VALUES (1);\n",
		"20240828000000_fail.down.sql": "DROP TABLE t2;\n",
	}))

	c.Assert(err, qt.ErrorMatches, `(?s).*relation "missing_table" does not exist.*`, qt.Commentf("output:\n%s", out))
	c.Assert(tableExistsIn(c, db, "t2"), qt.IsTrue)
	applied, failure := latestRevision(c, db, "20240828000000")
	c.Assert(applied, qt.Equals, 3)
	c.Assert(failure, qt.Matches, `ERROR: relation "missing_table" does not exist.*`)
}

// The next apply after that failure resumes after the statements that
// committed, on a fresh session, and runs the statement that failed. It needs
// --allow-dirty, as every retry over a failed revision does.
func TestMigrateApplyGolangMigrateOwnTransactions_RetryAfterTheFileCommitsLive(t *testing.T) {
	c := qt.New(t)
	run := newGolangMigrateApply(c, dbtarget.URL(c, dbtarget.PostgreSQL), withGolangMigrateBase(map[string]string{
		"20240828000000_fail.up.sql":   "BEGIN;\nCREATE TABLE t2 (id int);\nCOMMIT;\nINSERT INTO t3 VALUES (1);\n",
		"20240828000000_fail.down.sql": "DROP TABLE t2;\n",
	}))
	out, err := run.apply()
	c.Assert(err, qt.ErrorMatches, `(?s).*relation "t3" does not exist.*`, qt.Commentf("output:\n%s", out))
	_, err = run.db.ExecContext(context.Background(), "CREATE TABLE t3 (id int)")
	c.Assert(err, qt.IsNil)

	out, err = run.apply("--allow-dirty")

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
	var rows int
	c.Assert(run.db.QueryRowContext(context.Background(), "SELECT count(*) FROM t3").Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, 1)
	applied, failure := latestRevision(c, run.db, "20240828000000")
	c.Assert(applied, qt.Equals, 4)
	c.Assert(failure, qt.Equals, "")
}

// A failure inside the file's first transaction leaves nothing of the file
// committed. The run discards the revision row it wrote, as it does for a file
// the migrator's transaction rolled back, so the next apply runs the file from
// its start without --allow-dirty.
func TestMigrateApplyGolangMigrateOwnTransactions_FailureWithNothingCommittedLive(t *testing.T) {
	c := qt.New(t)
	run := newGolangMigrateApply(c, dbtarget.URL(c, dbtarget.PostgreSQL), withGolangMigrateBase(map[string]string{
		"20240828000000_fail.up.sql":   "BEGIN;\nCREATE TABLE t2 (id int);\nINSERT INTO t3 VALUES (1);\nCOMMIT;\n",
		"20240828000000_fail.down.sql": "DROP TABLE t2;\n",
	}))

	out, err := run.apply()

	c.Assert(err, qt.ErrorMatches, `(?s).*relation "t3" does not exist.*`, qt.Commentf("output:\n%s", out))
	c.Assert(tableExistsIn(c, run.db, "t2"), qt.IsFalse)
	applied, _ := latestRevision(c, run.db, "20240828000000")
	c.Assert(applied, qt.Equals, -1)
	_, err = run.db.ExecContext(context.Background(), "CREATE TABLE t3 (id int)")
	c.Assert(err, qt.IsNil)
	out, err = run.apply()
	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
	c.Assert(tableExistsIn(c, run.db, "t2"), qt.IsTrue)
}

func TestMigrateApplyGolangMigrateLoneConcurrentIndexLive(t *testing.T) {
	c := qt.New(t)
	db, out, err := applyGolangMigrateDirectory(c, dbtarget.URL(c, dbtarget.PostgreSQL), withGolangMigrateBase(map[string]string{
		"20240829000000_idx.up.sql":   "CREATE INDEX CONCURRENTLY base_id_idx ON base (id);\n",
		"20240829000000_idx.down.sql": "DROP INDEX CONCURRENTLY base_id_idx;\n",
	}))

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
	var valid bool
	c.Assert(db.QueryRowContext(context.Background(),
		"SELECT i.indisvalid FROM pg_index i JOIN pg_class x ON x.oid = i.indexrelid WHERE x.relname = 'base_id_idx'",
	).Scan(&valid), qt.IsNil)
	c.Assert(valid, qt.IsTrue)
	applied, failure := latestRevision(c, db, "20240829000000")
	c.Assert(applied, qt.Equals, 1)
	c.Assert(failure, qt.Equals, "")
}

// The policy follows the layout: the same file in Atlas's layout, which Atlas
// wraps per file, still runs in the migrator's transaction, where PostgreSQL
// refuses the build before anything runs.
func TestMigrateApplyAtlasLoneConcurrentIndexStaysWrappedLive(t *testing.T) {
	c := qt.New(t)
	dir := writeHashedLayoutDirectory(c, "atlas", map[string]string{
		"20240826120919_init.sql": "CREATE TABLE base (id int);\n",
		"20240829000000_idx.sql":  "CREATE INDEX CONCURRENTLY base_id_idx ON base (id);\n",
	})
	dbURL := newCleanGateDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL), nil)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "apply", "--dir", "file://" + dir, "--url", dbURL})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `(?s).*cannot run inside a transaction: line 1 CREATE or DROP INDEX CONCURRENTLY is refused inside a transaction block.*`)
}
