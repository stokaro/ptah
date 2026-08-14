package atlas_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// stokaro/ptah#1231 case 1: the compat surface applied every migration to a
// database that already held objects it did not create, and exited 0, where the
// pinned community binary v1.3.0 refuses and leaves the database untouched.
//
// Every expected string here was transcribed from that binary on 2026-08-07
// against SQLite files; the state each one belongs to is named on the test.

const (
	cleanGateVersionOne = "20240101000000"
	cleanGateVersionTwo = "20240102000000"
)

// writeCleanGateFixture writes a hashed two-migration Atlas directory and
// returns it with a path for a database file that does not exist yet.
func writeCleanGateFixture(c *qt.C) (migrationsDir, dbPath string) {
	c.Helper()
	root := c.TempDir()
	migrationsDir = filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	files := map[string]string{
		cleanGateVersionOne + "_first.sql":  "CREATE TABLE cg_users (id INTEGER PRIMARY KEY);\n",
		cleanGateVersionTwo + "_second.sql": "CREATE TABLE cg_orders (id INTEGER PRIMARY KEY);\n",
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(migrationsDir, name), []byte(body), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return migrationsDir, filepath.Join(root, "gate.db")
}

// execCleanGateSQL runs statements against the fixture database directly, so a
// test can put it in a state no migration in the directory produces.
func execCleanGateSQL(c *qt.C, dbPath string, statements ...string) {
	c.Helper()
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, atlasurl.SQLiteURLFromPath(dbPath))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

// runCleanGateApply applies the fixture with any extra flags the case needs.
func runCleanGateApply(c *qt.C, migrationsDir, dbPath string, extra ...string) (stdout string, err error) {
	c.Helper()
	args := []string{
		"migrate", "apply",
		"--url", atlasurl.SQLiteURLFromPath(dbPath),
		"--dir", "file://" + migrationsDir,
	}
	args = append(args, extra...)
	stdout, _, err = runCompatStreams(c, args...)
	return stdout, err
}

// TestCompatCommand_MigrateApplyRefusesUncleanDatabase is the headline
// reproduction. The refusal text and the count in it are the binary's, measured
// on a SQLite database holding one unrelated table.
//
// Reverted, this test fails on the first assertion: the apply succeeds, and
// cg_users exists in a database nobody asked Ptah to adopt.
func TestCompatCommand_MigrateApplyRefusesUncleanDatabase(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath := writeCleanGateFixture(c)
	execCleanGateSQL(c, dbPath, "CREATE TABLE legacy_stuff (id INTEGER PRIMARY KEY)")

	_, err := runCleanGateApply(c, migrationsDir, dbPath)

	c.Assert(err, qt.IsNotNil)
	c.Assert(
		err.Error(),
		qt.Equals,
		"sql/migrate: connected database is not clean: found multiple tables: 2. "+
			"baseline version or allow-dirty is required",
	)
	// The refusal is worth nothing if the migrations ran anyway.
	c.Assert(compatTableNames(c, dbPath), qt.Not(qt.Contains), "cg_users")
	c.Assert(compatTableNames(c, dbPath), qt.Not(qt.Contains), "cg_orders")
}

// The count grows with the number of unmanaged tables, exactly as measured, so
// a reader can tell one stray table from a whole legacy schema.
func TestCompatCommand_MigrateApplyUncleanCountReportsEveryTable(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath := writeCleanGateFixture(c)
	execCleanGateSQL(c, dbPath,
		"CREATE TABLE legacy_stuff (id INTEGER PRIMARY KEY)",
		"CREATE TABLE other_stuff (id INTEGER PRIMARY KEY)",
	)

	_, err := runCleanGateApply(c, migrationsDir, dbPath)

	c.Assert(err, qt.IsNotNil)
	c.Assert(
		err.Error(),
		qt.Equals,
		"sql/migrate: connected database is not clean: found multiple tables: 3. "+
			"baseline version or allow-dirty is required",
	)
}

// A dry run refuses too. The binary checks before it decides whether anything
// will be written, so --dry-run against an unclean database is an error rather
// than a printed plan — and the count is the same one a real apply reports,
// even though this implementation has not created its revisions table yet.
func TestCompatCommand_MigrateApplyDryRunRefusesUncleanDatabase(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath := writeCleanGateFixture(c)
	execCleanGateSQL(c, dbPath, "CREATE TABLE legacy_stuff (id INTEGER PRIMARY KEY)")

	stdout, err := runCleanGateApply(c, migrationsDir, dbPath, "--dry-run")

	c.Assert(err, qt.IsNotNil)
	c.Assert(
		err.Error(),
		qt.Equals,
		"sql/migrate: connected database is not clean: found multiple tables: 2. "+
			"baseline version or allow-dirty is required",
	)
	c.Assert(stdout, qt.Not(qt.Contains), "Would have applied")
}

// The gate is unconditional on how much work there is: the binary refuses even
// when the directory has nothing to run, so an operator cannot discover the
// problem only once a migration is added.
func TestCompatCommand_MigrateApplyRefusesUncleanDatabaseWithEmptyDirectory(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	migrationsDir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	_, sumErr := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	c.Assert(sumErr, qt.IsNil)
	dbPath := filepath.Join(root, "empty-dir.db")
	execCleanGateSQL(c, dbPath, "CREATE TABLE legacy_stuff (id INTEGER PRIMARY KEY)")

	_, err := runCleanGateApply(c, migrationsDir, dbPath)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "connected database is not clean")
}

// --allow-dirty and --baseline are the binary's two documented opt-ins, and
// each one alone applies against the same database the gate refused. Refusing
// either would be the other half of the parity rule broken.
func TestCompatCommand_MigrateApplyUncleanOptIns(t *testing.T) {
	tests := []struct {
		name  string
		extra []string
		// wantApplied names a table the run must have created, which is how
		// each row proves the opt-in ran migrations rather than merely
		// exiting 0.
		wantApplied string
	}{
		{name: "allow-dirty", extra: []string{"--allow-dirty"}, wantApplied: "cg_users"},
		{
			name:        "baseline",
			extra:       []string{"--baseline", cleanGateVersionOne},
			wantApplied: "cg_orders",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			migrationsDir, dbPath := writeCleanGateFixture(c)
			execCleanGateSQL(c, dbPath, "CREATE TABLE legacy_stuff (id INTEGER PRIMARY KEY)")

			_, err := runCleanGateApply(c, migrationsDir, dbPath, test.extra...)

			c.Assert(err, qt.IsNil)
			c.Assert(compatTableNames(c, dbPath), qt.Contains, test.wantApplied)
			c.Assert(compatTableNames(c, dbPath), qt.Contains, "legacy_stuff")
		})
	}
}

// --baseline under --dry-run is the cell where honoring the baseline opt-out
// is load-bearing rather than redundant. A real --baseline run records a
// revision row, which switches the gate off on its own; a dry run records
// nothing, so a gate that only asked "are there revisions yet" would refuse
// here. Measured on PostgreSQL 17 and SQLite, the binary exits 0 and prints its
// plan.
func TestCompatCommand_MigrateApplyDryRunBaselineAcceptsUncleanDatabase(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath := writeCleanGateFixture(c)
	execCleanGateSQL(c, dbPath, "CREATE TABLE legacy_stuff (id INTEGER PRIMARY KEY)")

	stdout, err := runCleanGateApply(c, migrationsDir, dbPath,
		"--baseline", cleanGateVersionOne, "--dry-run")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Would have applied 1 migrations.")
	c.Assert(compatTableNames(c, dbPath), qt.Not(qt.Contains), "cg_orders")
}

// The two opt-ins are not composable. Measured, the binary refuses the pair
// before recording anything, because they select different histories.
func TestCompatCommand_MigrateApplyBaselineAndAllowDirtyAreExclusive(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath := writeCleanGateFixture(c)
	execCleanGateSQL(c, dbPath, "CREATE TABLE legacy_stuff (id INTEGER PRIMARY KEY)")

	_, err := runCleanGateApply(c, migrationsDir, dbPath,
		"--allow-dirty", "--baseline", cleanGateVersionOne)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, "sql/migrate: baseline and allow-dirty are mutually exclusive")
	// Refused before the baseline was recorded: no revisions table exists to
	// carry a row the operator never asked to keep.
	c.Assert(compatTableNames(c, dbPath), qt.Not(qt.Contains), "atlas_schema_revisions")
	c.Assert(compatTableNames(c, dbPath), qt.Not(qt.Contains), "cg_orders")
}

// The states the binary calls clean. Each row is a database the gate must let
// through, and the assertion is that the migrations really ran.
func TestCompatCommand_MigrateApplyAcceptsCleanDatabases(t *testing.T) {
	tests := []struct {
		name string
		// setup puts the database into the state under test. A nil-free
		// signature keeps the table free of behavior switches.
		setup func(c *qt.C, dbPath string)
	}{
		{
			name:  "no database file at all",
			setup: func(_ *qt.C, _ string) {},
		},
		{
			name: "a view is not a table",
			setup: func(c *qt.C, dbPath string) {
				execCleanGateSQL(c, dbPath, "CREATE VIEW cg_view AS SELECT 1 AS one")
			},
		},
		{
			name: "SQLite's own bookkeeping table does not count",
			setup: func(c *qt.C, dbPath string) {
				execCleanGateSQL(c, dbPath,
					"CREATE TABLE cg_seq (id INTEGER PRIMARY KEY AUTOINCREMENT)",
					"INSERT INTO cg_seq DEFAULT VALUES",
					"DROP TABLE cg_seq",
				)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			migrationsDir, dbPath := writeCleanGateFixture(c)
			test.setup(c, dbPath)

			stdout, err := runCleanGateApply(c, migrationsDir, dbPath)

			c.Assert(err, qt.IsNil)
			c.Assert(stdout, qt.Contains, "Migration complete. Current version: "+cleanGateVersionTwo)
			c.Assert(compatTableNames(c, dbPath), qt.Contains, "cg_users")
		})
	}
}

// An empty revisions table left behind by an earlier run is not dirt: measured,
// the binary applies against a database whose only table is its own. This is
// the row that stops the gate from being "the schema must have no tables".
func TestCompatCommand_MigrateApplyAcceptsLoneEmptyRevisionsTable(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath := writeCleanGateFixture(c)

	_, firstErr := runCleanGateApply(c, migrationsDir, dbPath)
	c.Assert(firstErr, qt.IsNil)
	// Roll the database back to "adopted once, then emptied": the revisions
	// table survives with no rows and nothing else remains.
	execCleanGateSQL(c, dbPath,
		"DROP TABLE cg_users",
		"DROP TABLE cg_orders",
		"DELETE FROM atlas_schema_revisions",
	)
	c.Assert(compatTableNames(c, dbPath), qt.DeepEquals, []string{"atlas_schema_revisions"})

	stdout, err := runCleanGateApply(c, migrationsDir, dbPath)

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Migration complete. Current version: "+cleanGateVersionTwo)
	c.Assert(compatTableNames(c, dbPath), qt.Contains, "cg_users")
}

// The gate is an adoption gate, not a standing drift check. Once a revision has
// been recorded, a table that appears later never triggers it again — measured,
// the binary applies the next migration against exactly this state at exit 0.
//
// Reverted to a check that reads the schema instead of the revision rows, this
// test fails on the second apply with the not-clean refusal.
func TestCompatCommand_MigrateApplyIgnoresDriftOnAnAdoptedDatabase(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath := writeCleanGateFixture(c)

	_, firstErr := runCleanGateApply(c, migrationsDir, dbPath, "--to-version", cleanGateVersionOne)
	c.Assert(firstErr, qt.IsNil)
	execCleanGateSQL(c, dbPath, "CREATE TABLE legacy_stuff (id INTEGER PRIMARY KEY)")

	stdout, err := runCleanGateApply(c, migrationsDir, dbPath)

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Migration complete. Current version: "+cleanGateVersionTwo)
	c.Assert(compatTableNames(c, dbPath), qt.Contains, "cg_orders")
}
