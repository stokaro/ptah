package atlasschema_test

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// prepareSimulationPlan builds a runtime plan for a sqlite target that already
// contains sim_existing and whose desired state adds sim_added.
func prepareSimulationPlan(c *qt.C, dbPath string) atlasschema.ApplyRuntimePlan {
	c.Helper()
	schemaPath := filepath.Join(filepath.Dir(dbPath), "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE sim_existing (
  id INTEGER PRIMARY KEY
);
CREATE TABLE sim_added (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);
`), 0o600), qt.IsNil)
	conn := connectSQLite(c, dbPath)
	defer dbschema.CloseAndWarn(conn)
	c.Assert(atlasschema.ApplySQL(c.Context(), conn, migrator.MigrationTxModeAll, `
CREATE TABLE sim_existing (
  id INTEGER PRIMARY KEY
);
`), qt.IsNil)

	plan, err := atlasschema.PrepareApply(c.Context(), conn, atlasschema.ApplyRuntimeOptions{
		ToURLs: []string{"file://" + schemaPath},
		TxMode: migrator.MigrationTxModeAll,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.HasChanges(), qt.IsTrue)
	return plan
}

func TestSimulateOnDev_HappyPath(t *testing.T) {
	c := qt.New(t)

	c.Run("plan rehearses on the reset dev database", func(c *qt.C) {
		dir := c.TB.TempDir()
		dbPath := filepath.Join(dir, "target.db")
		devPath := filepath.Join(dir, "dev.db")
		plan := prepareSimulationPlan(c, dbPath)

		// Litter the dev database to prove the deterministic reset: stale
		// objects must not leak into the rehearsal.
		devConn := connectSQLite(c, devPath)
		c.Assert(atlasschema.ApplySQL(c.Context(), devConn, migrator.MigrationTxModeAll, `
CREATE TABLE sim_stale (
  id INTEGER PRIMARY KEY
);
`), qt.IsNil)
		dbschema.CloseAndWarn(devConn)

		err := plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{
			DevURL:    "sqlite://" + devPath,
			TargetURL: "sqlite://" + dbPath,
		})

		c.Assert(err, qt.IsNil)
		// The dev database ends at baseline + plan: current state recreated,
		// plan applied, stale objects gone. The target only has the baseline.
		c.Assert(sqliteTableExists(c, devPath, "sim_existing"), qt.IsTrue)
		c.Assert(sqliteTableExists(c, devPath, "sim_added"), qt.IsTrue)
		c.Assert(sqliteTableExists(c, devPath, "sim_stale"), qt.IsFalse)
		c.Assert(sqliteTableExists(c, dbPath, "sim_added"), qt.IsFalse)
	})

	c.Run("empty dev URL skips simulation", func(c *qt.C) {
		dir := c.TB.TempDir()
		plan := prepareSimulationPlan(c, filepath.Join(dir, "target.db"))

		c.Assert(plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{}), qt.IsNil)
	})

	c.Run("edited statements are rehearsed instead of the plan", func(c *qt.C) {
		dir := c.TB.TempDir()
		dbPath := filepath.Join(dir, "target.db")
		devPath := filepath.Join(dir, "dev.db")
		plan := prepareSimulationPlan(c, dbPath)

		err := plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{
			DevURL:     "sqlite://" + devPath,
			TargetURL:  "sqlite://" + dbPath,
			Statements: []string{"CREATE TABLE sim_edited (id INTEGER PRIMARY KEY)"},
		})

		c.Assert(err, qt.IsNil)
		c.Assert(sqliteTableExists(c, devPath, "sim_edited"), qt.IsTrue)
		c.Assert(sqliteTableExists(c, devPath, "sim_added"), qt.IsFalse)
	})
}

func TestSimulateOnDev_FailedSimulationLeavesTargetUnchanged(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	devPath := filepath.Join(dir, "dev.db")
	plan := prepareSimulationPlan(c, dbPath)

	err := plan.SimulateOnDev(t.Context(), atlasschema.SimulateOptions{
		DevURL:    "sqlite://" + devPath,
		TargetURL: "sqlite://" + dbPath,
		// sim_existing is part of the recreated baseline, so this statement
		// fails on the dev database exactly as it would on the target.
		Statements: []string{"CREATE TABLE sim_existing (id INTEGER PRIMARY KEY)"},
	})

	var simulationErr *atlasschema.SimulationError
	c.Assert(err, qt.ErrorAs, &simulationErr)
	c.Assert(simulationErr.Stage, qt.Equals, "plan")
	c.Assert(atlasschema.IsSimulationFailure(err), qt.IsTrue)
	c.Assert(err, qt.ErrorMatches, `(?s)dev database simulation failed during plan: .*sim_existing.*; the plan was not applied to the target database`)
	// The failed rehearsal must not have touched the target.
	c.Assert(sqliteTableExists(c, dbPath, "sim_existing"), qt.IsTrue)
	c.Assert(sqliteTableExists(c, dbPath, "sim_added"), qt.IsFalse)
}

func TestSimulateOnDev_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("docker dev URL rejected", func(c *qt.C) {
		dir := c.TB.TempDir()
		plan := prepareSimulationPlan(c, filepath.Join(dir, "target.db"))

		err := plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{
			DevURL: "docker://sqlite/3/dev",
		})
		c.Assert(err, qt.ErrorMatches, `docker --dev-url values are accepted by Atlas, but Ptah requires a directly connectable dev database URL for schema apply simulation`)
	})

	c.Run("dev URL dialect mismatch rejected before connecting", func(c *qt.C) {
		dir := c.TB.TempDir()
		plan := prepareSimulationPlan(c, filepath.Join(dir, "target.db"))

		err := plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{
			DevURL: "postgres://localhost/dev",
		})
		c.Assert(err, qt.ErrorMatches, `--dev-url dialect "postgres" does not match --url dialect "sqlite"`)
	})

	c.Run("dev URL must not equal the target URL", func(c *qt.C) {
		dir := c.TB.TempDir()
		dbPath := filepath.Join(dir, "target.db")
		plan := prepareSimulationPlan(c, dbPath)

		err := plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{
			DevURL:    "sqlite://" + dbPath,
			TargetURL: "sqlite://" + dbPath,
		})
		c.Assert(err, qt.ErrorMatches, `--dev-url must not point at the target database: the dev database is reset destructively before the plan is rehearsed on it`)
		c.Assert(sqliteTableExists(c, dbPath, "sim_existing"), qt.IsTrue)
	})

	c.Run("dev URL must not alias the target URL", func(c *qt.C) {
		dir := c.TB.TempDir()
		dbPath := filepath.Join(dir, "target.db")
		plan := prepareSimulationPlan(c, dbPath)
		aliasPath := filepath.Dir(dbPath) + string(os.PathSeparator) + "." +
			string(os.PathSeparator) + filepath.Base(dbPath)

		err := plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{
			DevURL:    "sqlite://" + aliasPath + "?mode=rwc",
			TargetURL: "sqlite://" + dbPath,
		})
		c.Assert(err, qt.ErrorMatches, `--dev-url must not point at the target database: the dev database is reset destructively before the plan is rehearsed on it`)
		c.Assert(sqliteTableExists(c, dbPath, "sim_existing"), qt.IsTrue)
	})

	c.Run("percent-encoded dev URL must not alias the target URL", func(c *qt.C) {
		dir := c.TB.TempDir()
		dbPath := filepath.Join(dir, "target.db")
		plan := prepareSimulationPlan(c, dbPath)
		encodedPath := url.PathEscape(filepath.ToSlash(dbPath))

		err := plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{
			DevURL:    "sqlite:file:" + encodedPath + "?mode=rwc",
			TargetURL: "sqlite://" + dbPath,
		})
		c.Assert(err, qt.ErrorMatches, `--dev-url must not point at the target database: the dev database is reset destructively before the plan is rehearsed on it`)
		c.Assert(sqliteTableExists(c, dbPath, "sim_existing"), qt.IsTrue)
	})

	c.Run("dev URL must not equal a database desired-state URL", func(c *qt.C) {
		dir := c.TB.TempDir()
		dbPath := filepath.Join(dir, "target.db")
		devPath := filepath.Join(dir, "desired.db")
		plan := prepareSimulationPlan(c, dbPath)

		err := plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{
			DevURL:      "sqlite://" + devPath,
			TargetURL:   "sqlite://" + dbPath,
			DesiredURLs: []string{"sqlite://" + devPath},
		})
		c.Assert(err, qt.ErrorMatches, `--dev-url must not point at the --to desired-state database ".*desired\.db": the dev database is reset destructively before the plan is rehearsed on it`)
	})

	c.Run("dev URL must not alias a database desired-state URL", func(c *qt.C) {
		dir := c.TB.TempDir()
		dbPath := filepath.Join(dir, "target.db")
		desiredPath := filepath.Join(dir, "desired.db")
		plan := prepareSimulationPlan(c, dbPath)

		err := plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{
			DevURL:      "sqlite://" + desiredPath + "?mode=rwc",
			TargetURL:   "sqlite://" + dbPath,
			DesiredURLs: []string{"sqlite://" + desiredPath},
		})
		c.Assert(err, qt.ErrorMatches, `--dev-url must not point at the --to desired-state database ".*desired\.db": the dev database is reset destructively before the plan is rehearsed on it`)
	})

	c.Run("percent-encoded dev URL must not alias a database desired-state URL", func(c *qt.C) {
		dir := c.TB.TempDir()
		dbPath := filepath.Join(dir, "target.db")
		desiredPath := filepath.Join(dir, "desired.db")
		plan := prepareSimulationPlan(c, dbPath)
		desiredConn := connectSQLite(c, desiredPath)
		c.Assert(atlasschema.ApplySQL(c.Context(), desiredConn, migrator.MigrationTxModeAll,
			"CREATE TABLE desired_kept (id INTEGER PRIMARY KEY);"), qt.IsNil)
		dbschema.CloseAndWarn(desiredConn)

		err := plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{
			DevURL:      "sqlite:file:" + url.PathEscape(filepath.ToSlash(desiredPath)) + "?mode=rwc",
			TargetURL:   "sqlite://" + dbPath,
			DesiredURLs: []string{"sqlite://" + desiredPath},
		})
		c.Assert(err, qt.ErrorMatches, `--dev-url must not point at the --to desired-state database ".*desired\.db": the dev database is reset destructively before the plan is rehearsed on it`)
		c.Assert(sqliteTableExists(c, desiredPath, "desired_kept"), qt.IsTrue)
	})

	c.Run("malformed target URL fails closed before resetting dev", func(c *qt.C) {
		dir := c.TB.TempDir()
		dbPath := filepath.Join(dir, "target.db")
		devPath := filepath.Join(dir, "dev.db")
		plan := prepareSimulationPlan(c, dbPath)
		devConn := connectSQLite(c, devPath)
		c.Assert(atlasschema.ApplySQL(c.Context(), devConn, migrator.MigrationTxModeAll,
			"CREATE TABLE dev_kept (id INTEGER PRIMARY KEY);"), qt.IsNil)
		dbschema.CloseAndWarn(devConn)

		err := plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{
			DevURL:    "sqlite://" + devPath,
			TargetURL: "sqlite:file:%ZZ",
		})
		c.Assert(err, qt.ErrorMatches, `compare --dev-url with target database: invalid SQLite database URL`)
		c.Assert(sqliteTableExists(c, devPath, "dev_kept"), qt.IsTrue)
	})

	c.Run("malformed desired URL fails closed before resetting dev", func(c *qt.C) {
		dir := c.TB.TempDir()
		dbPath := filepath.Join(dir, "target.db")
		devPath := filepath.Join(dir, "dev.db")
		plan := prepareSimulationPlan(c, dbPath)
		devConn := connectSQLite(c, devPath)
		c.Assert(atlasschema.ApplySQL(c.Context(), devConn, migrator.MigrationTxModeAll,
			"CREATE TABLE dev_kept (id INTEGER PRIMARY KEY);"), qt.IsNil)
		dbschema.CloseAndWarn(devConn)

		err := plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{
			DevURL:      "sqlite://" + devPath,
			TargetURL:   "sqlite://" + dbPath,
			DesiredURLs: []string{"sqlite:file:%ZZ"},
		})
		c.Assert(err, qt.ErrorMatches, `compare --dev-url with --to desired-state database "sqlite:file:%ZZ": invalid SQLite database URL`)
		c.Assert(sqliteTableExists(c, devPath, "dev_kept"), qt.IsTrue)
	})

	c.Run("unreachable dev database", func(c *qt.C) {
		dir := c.TB.TempDir()
		plan := prepareSimulationPlan(c, filepath.Join(dir, "target.db"))

		err := plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{
			DevURL: "sqlite://" + filepath.Join(dir, "missing-dir", "sub", "dev.db"),
		})
		c.Assert(err, qt.ErrorMatches, `connect to --dev-url: .*`)
	})
}
