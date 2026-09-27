package devclean_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/devclean"
	"ptah.run/internal/migrateclean"
)

// devNotClean is the refusal of a SQLite dev database whose first table is
// keep_me.
const devNotClean = `connected database is not clean: found table "keep_me"; ` +
	`Ptah resets a dev database before and after it uses one, so point --dev-url at an empty database`

// openSQLiteDev opens a SQLite dev database that has run ddl, closed when the
// test ends.
func openSQLiteDev(c *qt.C, ddl string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "dev.db")))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	_, err = conn.ExecContext(c.Context(), ddl)
	c.Assert(err, qt.IsNil)
	return conn
}

// TestEnsureClean_HappyPath is the dev databases a run may reset: ones that
// hold nothing the reset would drop.
func TestEnsureClean_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		ddl  string
	}{
		{name: "an empty database", ddl: "SELECT 1"},
		{
			// sqlite_sequence outlives the table that made it and is SQLite's.
			name: "sqlite_sequence alone",
			ddl: "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT); INSERT INTO t DEFAULT VALUES; " +
				"DROP TABLE t",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openSQLiteDev(c, test.ddl)

			err := devclean.EnsureClean(c.Context(), conn)

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestEnsureClean_FailurePath refuses a dev database that holds a table,
// before anything drops it.
func TestEnsureClean_FailurePath(t *testing.T) {
	c := qt.New(t)
	conn := openSQLiteDev(c, "CREATE TABLE keep_me (id INTEGER PRIMARY KEY); INSERT INTO keep_me VALUES (1)")

	err := devclean.EnsureClean(c.Context(), conn)

	var notClean *migrateclean.NotCleanError
	c.Assert(err, qt.ErrorAs, &notClean)
	c.Assert(err, qt.ErrorMatches, devNotClean)
}

// TestEnsureClean_RefusesAViewTheResetDrops refuses a dev database holding a
// view alone. The pinned binary counts tables only and drops the view; Ptah
// counts what its reset drops, so the view is still there afterwards
// (stokaro/ptah#3851).
func TestEnsureClean_RefusesAViewTheResetDrops(t *testing.T) {
	c := qt.New(t)
	conn := openSQLiteDev(c, "CREATE VIEW keep_v AS SELECT 1 AS id")

	err := devclean.EnsureClean(c.Context(), conn)

	c.Assert(err, qt.ErrorMatches, `connected database is not clean: found view "keep_v"; `+
		`Ptah resets a dev database before and after it uses one, so point --dev-url at an empty database`)
	var views int
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count(*) FROM sqlite_schema WHERE name = 'keep_v'").Scan(&views), qt.IsNil)
	c.Assert(views, qt.Equals, 1)
}

// TestClaim_FailurePath is the refusal as a replay meets it: Claim refuses the
// database and captures no baseline, so nothing is kept or dropped on its
// word, and the table and its row are still there.
func TestClaim_FailurePath(t *testing.T) {
	c := qt.New(t)
	conn := openSQLiteDev(c, "CREATE TABLE keep_me (id INTEGER PRIMARY KEY); INSERT INTO keep_me VALUES (1)")

	baseline, err := devclean.Claim(c.Context(), conn)

	c.Assert(err, qt.ErrorMatches, devNotClean)
	c.Assert(baseline.Extensions(), qt.HasLen, 0)
	var rows int
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count(*) FROM keep_me").Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, 1)
}

// TestEnsureClean_NamesTheFirstTableSQLiteCreated reads the refusal's table
// from the catalog in the order SQLite keeps it, which is the order the tables
// were created in: the pinned binary names zzz_t here, not aaa_t.
func TestEnsureClean_NamesTheFirstTableSQLiteCreated(t *testing.T) {
	c := qt.New(t)
	conn := openSQLiteDev(c, "CREATE TABLE zzz_t (id INTEGER PRIMARY KEY); CREATE TABLE aaa_t (id INTEGER PRIMARY KEY)")

	err := devclean.EnsureClean(c.Context(), conn)

	c.Assert(err, qt.ErrorMatches, `connected database is not clean: found table "zzz_t"; .*`)
}
