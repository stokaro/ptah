//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/dbtarget"
)

// dropAllDryRunFixture creates one object of each kind the PostgreSQL cleanup
// plan reports from a different source: a table from the reader's snapshot, a
// view from the same snapshot's view list, and a sequence that only the live
// catalog carries. A fixture of tables alone would leave the two other sources
// unmeasured, and the sequence is standalone rather than a serial column's so
// that it survives or falls on its own drop statement.
var dropAllDryRunFixture = []string{
	`CREATE TABLE products (id integer PRIMARY KEY, name text NOT NULL)`,
	`CREATE VIEW active_products AS SELECT id, name FROM products`,
	`CREATE SEQUENCE order_number_seq`,
}

// dropAllDryRunInventoryQuery reads what the database still holds. It asks the
// catalog rather than Ptah's own reader: the question is whether the objects
// exist, and answering it through the same code the command used to plan the
// drop would make a reader defect look like a successful dry run.
//
// The C collation orders the rows, because the server's default collation is
// configuration and a test asserting an exact slice must not depend on it.
const dropAllDryRunInventoryQuery = `
WITH present AS (
	SELECT
		CASE class.relkind
			WHEN 'r' THEN 'table'
			WHEN 'v' THEN 'view'
			WHEN 'S' THEN 'sequence'
		END AS kind,
		class.relname::text AS name
	FROM pg_class class
	JOIN pg_namespace namespace ON namespace.oid = class.relnamespace
	WHERE namespace.nspname = 'public'
	  AND class.relkind IN ('r', 'v', 'S')
)
SELECT kind || ' ' || name FROM present ORDER BY kind COLLATE "C", name COLLATE "C"`

// TestDBDropAllDryRunE2E drives the shipped `ptah` binary against a live
// PostgreSQL database, which is the only place the branch separating a dry run
// from a real drop has an observable.
//
// schemaclean.Execute returns the plan and stops on DryRun, and calls
// SchemaWriter.DropAllTables otherwise. Against an empty SQLite file -- what
// every existing drop-all test points at -- both arms leave the same database
// behind, so a run that took the wrong one still passes. Here the database
// holds a table, a view and a sequence, so the dry run has something to
// destroy and the assertion that it destroyed nothing means something.
//
// The run without --dry-run is the control. Without it "all three objects are
// still present" is also what a command that connects and gives up would
// produce, and the test would pass on a drop-all that never drops.
func TestDBDropAllDryRunE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	// A throwaway database, because the command's scope is the whole database
	// and pointing it at the shared one would take every other test's fixtures
	// with it.
	testDBName := fmt.Sprintf("ptah_drop_all_dry_run_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, testDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)
	scopedURL := replaceDatabaseName(c, dbURL, testDBName)

	targetDB, err := sql.Open("pgx", scopedURL)
	c.Assert(err, qt.IsNil)
	defer targetDB.Close()
	applyDropAllDryRunFixture(c, ctx, targetDB)

	workDir := c.TempDir()
	binary := filepath.Join(workDir, "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)

	populated := []string{"sequence order_number_seq", "table products", "view active_products"}
	c.Assert(dropAllDryRunInventory(c, ctx, targetDB), qt.DeepEquals, populated)

	// No stdin is attached to the process, so reaching exit 0 also says the dry
	// run never asked for the two confirmations: reading one would hit EOF and
	// fail the command.
	t.Run("--dry-run reports the drops and performs none", func(t *testing.T) {
		c := qt.New(t)

		stdout, stderr, runErr := runCLIProcess(ctx, workDir, binary,
			"db", "drop-all", "--db-url", scopedURL, "--dry-run")

		c.Assert(exitStatusOf(c, runErr), qt.Equals, 0)
		c.Assert(stderr, qt.Equals, "")
		c.Assert(stdout, qt.Contains, "[DRY RUN] Would drop ALL tables and enums from database")
		c.Assert(stdout, qt.Contains, "[DRY RUN] Would drop 3 supported schema objects from database")
		c.Assert(stdout, qt.Contains, "[DRY RUN] Drop all operations completed successfully!")
		c.Assert(stdout, qt.Not(qt.Contains), "All tables and enums dropped successfully!")
		c.Assert(dropAllDryRunInventory(c, ctx, targetDB), qt.DeepEquals, populated)
	})

	t.Run("the same invocation without --dry-run empties the database", func(t *testing.T) {
		c := qt.New(t)

		stdout, stderr, runErr := runCLIProcess(ctx, workDir, binary,
			"db", "drop-all", "--db-url", scopedURL, "--auto-approve")

		c.Assert(exitStatusOf(c, runErr), qt.Equals, 0)
		c.Assert(stderr, qt.Equals, "")
		c.Assert(stdout, qt.Not(qt.Contains), "[DRY RUN]")
		c.Assert(stdout, qt.Contains, "Dropping 3 supported schema objects from database")
		c.Assert(stdout, qt.Contains, "All tables and enums dropped successfully!")
		c.Assert(dropAllDryRunInventory(c, ctx, targetDB), qt.HasLen, 0)
	})
}

func applyDropAllDryRunFixture(c *qt.C, ctx context.Context, db *sql.DB) {
	c.Helper()
	for _, statement := range dropAllDryRunFixture {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

func dropAllDryRunInventory(c *qt.C, ctx context.Context, db *sql.DB) []string {
	c.Helper()

	rows, err := db.QueryContext(ctx, dropAllDryRunInventoryQuery)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var objects []string
	for rows.Next() {
		var object string
		c.Assert(rows.Scan(&object), qt.IsNil)
		objects = append(objects, object)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return objects
}
