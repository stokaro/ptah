//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/exeext"
)

const (
	// sqlServerDropTableIfExists removes a table when it exists.
	sqlServerDropTableIfExists = "DROP TABLE IF EXISTS %s"
	// oracleDropTableIfExists is the same statement for Oracle. Oracle 21 has
	// no DROP TABLE IF EXISTS, so it is a block that ignores ORA-00942, table
	// or view does not exist.
	oracleDropTableIfExists = `BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE %s';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;`
)

// TestSeedCreatesItsTrackerOnSQLServerE2E is stokaro/ptah#3287 on SQL Server.
// The tracker statement was CREATE TABLE IF NOT EXISTS, which T-SQL refuses at
// parse time with Msg 156, so `ptah seed` failed before it read a seed file.
// Behind that, applied_at was TIMESTAMP, which T-SQL reads as rowversion, and
// recording the first seed failed with Msg 273.
func TestSeedCreatesItsTrackerOnSQLServerE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)

	assertSeedCreatesItsTracker(c, e2eRepoRoot(t), dbURL, sqlServerDropTableIfExists)
}

// TestSeedCreatesItsTrackerOnOracleE2E runs the Oracle tracker statement, a
// PL/SQL block, through the shipped binary.
//
// Oracle 23 also accepts CREATE TABLE IF NOT EXISTS, so on 23 this test cannot
// tell the block from the portable statement; the unit test in
// migration/seeder pins which one is sent. What this test proves is that the
// block a 21 server is sent creates the tracker, and that the second run over
// an existing tracker still succeeds.
func TestSeedCreatesItsTrackerOnOracleE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)

	assertSeedCreatesItsTracker(c, e2eRepoRoot(t), dbURL, oracleDropTableIfExists)
}

// TestSeedCreatesItsTrackerOnSpannerE2E is stokaro/ptah#3325. The portable
// tracker statement declares checksum CHAR(64) and applied_at TIMESTAMP, and
// Spanner's PostgreSQL interface has neither: the endpoint answers
// `Type <bpchar> is not supported. (SQLSTATE P0001)`, so `ptah seed` stopped
// before it read a seed file.
//
// Spanner takes DROP TABLE IF EXISTS, so it shares the portable spelling here.
func TestSeedCreatesItsTrackerOnSpannerE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Spanner)
	c := qt.New(t)

	assertSeedCreatesItsTracker(c, e2eRepoRoot(t), dbURL, sqlServerDropTableIfExists)
}

// assertSeedCreatesItsTracker runs `ptah seed` twice against a database with no
// tracker table. The first run creates the tracker and records the seed. The
// second run finds both and skips the seed, so the tracker statement runs once
// with the table absent and once with it present.
func assertSeedCreatesItsTracker(c *qt.C, repoRoot, dbURL, dropTableIfExists string) {
	c.Helper()

	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()

	binary := filepath.Join(c.TempDir(), "ptah"+exeext.Suffix)
	buildPtah(c, ctx, repoRoot, binary)

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	table := fmt.Sprintf("seed_tracker_e2e_%d", time.Now().UnixNano()%1_000_000_000)
	dropTables := func() {
		for _, name := range []string{table, "schema_seeds"} {
			_, dropErr := conn.ExecContext(context.WithoutCancel(ctx), fmt.Sprintf(dropTableIfExists, name))
			c.Check(dropErr, qt.IsNil)
		}
	}
	// The first run has to create the tracker, so a table left by an earlier
	// run is removed before it.
	dropTables()
	defer dropTables()

	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL)", table))
	c.Assert(err, qt.IsNil)

	seedsDir := filepath.Join(c.TempDir(), "seeds")
	c.Assert(os.MkdirAll(seedsDir, 0o755), qt.IsNil)
	seed := fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'first');\n", table)
	c.Assert(os.WriteFile(filepath.Join(seedsDir, "010_widgets.test.sql"), []byte(seed), 0o600), qt.IsNil)

	args := []string{"seed", "--db-url", dbURL, "--seeds-dir", seedsDir, "--env", "test"}

	output, err := runPtah(ctx, repoRoot, binary, args...)
	c.Assert(err, qt.IsNil, qt.Commentf("first ptah seed run:\n%s", output))
	c.Assert(output, qt.Contains, "Applied seeds: 1\n")

	output, err = runPtah(ctx, repoRoot, binary, args...)
	c.Assert(err, qt.IsNil, qt.Commentf("second ptah seed run:\n%s", output))
	c.Assert(output, qt.Contains, "Skipped seeds: 1\n")

	var rows int
	c.Assert(conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, 1)

	var env string
	recorded := sqlutil.Rebind(conn.Info().Dialect, "SELECT env FROM schema_seeds WHERE seed_path = ?")
	c.Assert(conn.QueryRowContext(ctx, recorded, "010_widgets.test.sql").Scan(&env), qt.IsNil)
	c.Assert(env, qt.Equals, "test")
}
