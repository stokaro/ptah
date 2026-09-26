package atlasschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// The tests in this file name the target as the dev database and give the
// comparison no URL to match it with: SimulateOptions.TargetURL and
// PlanRehearsalOptions.TargetURL are left empty, and PrepareApply has none.
// That is the case a URL comparison cannot see -- a connection pooler serving
// the target under another database name -- so the live comparison of the two
// connections is the only thing between the dev reset and the target. Each
// test reads the target's row back after the refusal (stokaro/ptah#3769).

// targetRefusal is the refusal of a dev database that is the target.
const targetRefusal = `--dev-url must not point at the target database: the dev database is reset destructively before the plan is rehearsed on it`

// seedKeptRow puts one row in sim_existing, which prepareSimulationPlan
// creates in the target.
func seedKeptRow(c *qt.C, dbPath string) {
	c.Helper()
	conn := connectSQLite(c, dbPath)
	defer dbschema.CloseAndWarn(conn)
	c.Assert(atlasschema.ApplySQL(c.Context(), conn, migrator.MigrationTxModeAll,
		"INSERT INTO sim_existing (id) VALUES (1);"), qt.IsNil)
}

// keptRows counts the rows of sim_existing, reading the file directly.
func keptRows(c *qt.C, dbPath string) int {
	c.Helper()
	conn := connectSQLite(c, dbPath)
	defer dbschema.CloseAndWarn(conn)
	var rows int
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count(*) FROM sim_existing").Scan(&rows), qt.IsNil)
	return rows
}

// TestSimulateOnDev_RefusesADevDatabaseThatIsTheTargetBeforeArmingItsCleanup
// rehearses a plan with the target as the dev database. The rehearsal cleans
// the dev database on every exit, and a cleanup armed before the comparison
// ran on the refusal path too, dropping sim_existing from the target.
func TestSimulateOnDev_RefusesADevDatabaseThatIsTheTargetBeforeArmingItsCleanup(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "target.db")
	plan := prepareSimulationPlan(c, dbPath)
	seedKeptRow(c, dbPath)

	err := plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{
		DevURL: atlasurl.SQLiteURLFromPath(dbPath),
	})

	c.Assert(err, qt.ErrorMatches, targetRefusal)
	c.Assert(keptRows(c, dbPath), qt.Equals, 1)
	c.Assert(sqliteTableExists(c, dbPath, "sim_added"), qt.IsFalse)
}

// TestRehearsePlanStatements_RefusesADevDatabaseThatIsTheTargetBeforeArmingItsCleanup
// is the same case on the plan-file path, which arms the same cleanup.
func TestRehearsePlanStatements_RefusesADevDatabaseThatIsTheTargetBeforeArmingItsCleanup(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "target.db")
	prepareSimulationPlan(c, dbPath)
	seedKeptRow(c, dbPath)
	conn := connectSQLite(c, dbPath)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	err := atlasschema.RehearsePlanStatements(c.Context(), conn,
		[]string{"CREATE TABLE sim_added (id INTEGER PRIMARY KEY)"},
		&schemamodel.Database{},
		atlasschema.PlanRehearsalOptions{DevURL: atlasurl.SQLiteURLFromPath(dbPath)})

	c.Assert(err, qt.ErrorMatches, targetRefusal)
	c.Assert(keptRows(c, dbPath), qt.Equals, 1)
	c.Assert(sqliteTableExists(c, dbPath, "sim_added"), qt.IsFalse)
}

// TestPrepareApply_RefusesADevDatabaseThatIsTheTargetBeforeReplayingADirectory
// plans toward a migration directory, which is replayed on the dev database
// while the plan is computed. The replay resets the dev database first, so a
// dev database that is the target lost sim_existing before any rehearsal.
func TestPrepareApply_RefusesADevDatabaseThatIsTheTargetBeforeReplayingADirectory(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dbPath := filepath.Join(root, "target.db")
	prepareSimulationPlan(c, dbPath)
	seedKeptRow(c, dbPath)
	dir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_added.sql"),
		[]byte("CREATE TABLE sim_existing (id INTEGER PRIMARY KEY);\nCREATE TABLE sim_added (id INTEGER PRIMARY KEY);\n"),
		0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	conn := connectSQLite(c, dbPath)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	plan, err := atlasschema.PrepareApply(c.Context(), conn, atlasschema.ApplyRuntimeOptions{
		ToURLs: []string{"file://" + dir},
		DevURL: atlasurl.SQLiteURLFromPath(dbPath),
		TxMode: migrator.MigrationTxModeAll,
	})

	c.Assert(err, qt.ErrorMatches, `load --to schema: --dev-url must not point at the target database: the dev database is reset destructively before the migration directory is replayed on it`)
	c.Assert(plan.HasChanges(), qt.IsFalse)
	c.Assert(keptRows(c, dbPath), qt.Equals, 1)
}

// TestSimulateOnDev_RehearsesOnADevDatabaseThatIsNotTheTarget is the control:
// with no URL to compare either, a dev database that is another file passes
// the live comparison and the plan is rehearsed there.
func TestSimulateOnDev_RehearsesOnADevDatabaseThatIsNotTheTarget(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dbPath := filepath.Join(root, "target.db")
	devPath := filepath.Join(root, "dev.db")
	plan := prepareSimulationPlan(c, dbPath)
	seedKeptRow(c, dbPath)

	err := plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{
		DevURL: atlasurl.SQLiteURLFromPath(devPath),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(keptRows(c, dbPath), qt.Equals, 1)
}
