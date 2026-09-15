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

	"ptah.run/internal/dbtarget"
)

// driftManagedDataEntities declares one reference table. The database is moved
// around it — created, seeded, edited — so every run below compares the same
// declaration against a different live state.
const driftManagedDataEntities = `package entities

//ptah:schema:data table="regions" key="code" file="regions.yaml"
//ptah:schema:table name="regions"
type Region struct {
	//ptah:schema:field name="code" type="TEXT" primary="true"
	Code string

	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string
}
`

const driftManagedDataRows = `- code: US
  name: United States
- code: CZ
  name: Czechia
`

// TestPostgresDriftManagedDataE2E drives `ptah schema drift` against a live
// PostgreSQL server, which is what decides the two answers a unit test on
// SQLite cannot see.
//
// PostgreSQL refuses a SELECT naming a table it does not have (42P01) and a
// column the table does not carry (42703). SQLite refuses neither: it answers a
// quoted name it cannot resolve with a string literal, so an offline test
// agrees with a read the server would have rejected. Both states are ordinary
// for a check — a table this release creates, a column this release adds — and
// a drift check that exits 2 on them reports "the check is broken" where the
// answer is "the database has not caught up".
func TestPostgresDriftManagedDataE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	// The name carries none of the strings this test asserts are absent from the
	// report: the report echoes the database URL, so a name spelling one of them
	// would satisfy the assertion by accident.
	testDBName := fmt.Sprintf("ptah_drift_rows_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, testDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)
	scopedURL := replaceDatabaseName(c, dbURL, testDBName)

	targetDB, err := sql.Open("pgx", scopedURL)
	c.Assert(err, qt.IsNil)
	defer targetDB.Close()

	repoRoot := e2eRepoRoot(t)
	workDir := c.TempDir()
	binary := filepath.Join(workDir, "ptah")
	buildPtah(c, ctx, repoRoot, binary)

	root := filepath.Join(workDir, "entities")
	c.Assert(os.MkdirAll(root, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(driftManagedDataEntities), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "regions.yaml"), []byte(driftManagedDataRows), 0o600), qt.IsNil)

	// The table does not exist yet. PostgreSQL answers 42P01 to a read of it, so
	// this run is the one that used to take the whole check down.
	t.Run("a table the database has not created", func(t *testing.T) {
		c := qt.New(t)

		stdout, stderr, runErr := runCLIProcess(ctx, workDir, binary,
			"schema", "drift", "--db-url", scopedURL, "--root-dir", root, "--format", "json")

		c.Assert(exitStatusOf(c, runErr), qt.Equals, 1)
		c.Assert(stderr, qt.Equals, "")
		c.Assert(stdout, qt.Contains, `"drift": true`)
		c.Assert(stdout, qt.Contains, `"table": "regions"`)
		c.Assert(stdout, qt.Contains, `"inserts": 2`)
		c.Assert(stdout, qt.Contains, `"category": "data_rows_inserted"`)
		c.Assert(stdout, qt.Not(qt.Contains), "42P01")
	})

	_, err = targetDB.ExecContext(ctx, `CREATE TABLE regions (code TEXT PRIMARY KEY, name TEXT NOT NULL)`)
	c.Assert(err, qt.IsNil)
	_, err = targetDB.ExecContext(ctx,
		`INSERT INTO regions (code, name) VALUES ('US', 'United States'), ('CZ', 'Czechia')`)
	c.Assert(err, qt.IsNil)

	// The control, and it runs before the edit: with the declared rows in place
	// the check passes, so the failures around it are the drift and not a check
	// that reports every declared table on every run.
	t.Run("the declared rows are in place", func(t *testing.T) {
		c := qt.New(t)

		stdout, stderr, runErr := runCLIProcess(ctx, workDir, binary,
			"schema", "drift", "--db-url", scopedURL, "--root-dir", root, "--format", "json")

		c.Assert(exitStatusOf(c, runErr), qt.Equals, 0)
		c.Assert(stderr, qt.Equals, "")
		c.Assert(stdout, qt.Contains, `"drift": false`)
		c.Assert(stdout, qt.Not(qt.Contains), "managed_data")
	})

	t.Run("a row edited in the database", func(t *testing.T) {
		c := qt.New(t)
		_, execErr := targetDB.ExecContext(ctx, `UPDATE regions SET name = 'Czech Republic' WHERE code = 'CZ'`)
		c.Assert(execErr, qt.IsNil)
		// The precondition, read back from the server rather than assumed: the
		// database holds a value the declaration does not.
		c.Assert(regionName(c, ctx, targetDB, "CZ"), qt.Equals, "Czech Republic")

		stdout, stderr, runErr := runCLIProcess(ctx, workDir, binary,
			"schema", "drift", "--db-url", scopedURL, "--root-dir", root,
			"--format", "json", "--severity", "destructive")

		c.Assert(exitStatusOf(c, runErr), qt.Equals, 1)
		c.Assert(stderr, qt.Equals, "")
		c.Assert(stdout, qt.Contains, `"drift": true`)
		c.Assert(stdout, qt.Contains, `"highest_severity": "destructive"`)
		c.Assert(stdout, qt.Contains, `"updates": 1`)
		c.Assert(stdout, qt.Contains, `"category": "data_rows_updated"`)
		// The value the server holds and the value the declaration holds are
		// both absent from the document a pipeline would archive.
		c.Assert(stdout, qt.Not(qt.Contains), "Czech Republic")
		c.Assert(stdout, qt.Not(qt.Contains), "Czechia")

		_, execErr = targetDB.ExecContext(ctx, `UPDATE regions SET name = 'Czechia' WHERE code = 'CZ'`)
		c.Assert(execErr, qt.IsNil)
	})

	// A column the declaration names and the table does not carry. PostgreSQL
	// answers 42703 to a read that asks for it, so the check has to leave it out
	// of the projection and compare it as absent.
	t.Run("a column the table has not gained", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(os.WriteFile(filepath.Join(root, "regions.yaml"), []byte(
			`- code: US
  name: United States
  iso3: USA
- code: CZ
  name: Czechia
  iso3: CZE
`), 0o600), qt.IsNil)
		t.Cleanup(func() {
			c.Assert(os.WriteFile(filepath.Join(root, "regions.yaml"), []byte(driftManagedDataRows), 0o600), qt.IsNil)
		})

		stdout, stderr, runErr := runCLIProcess(ctx, workDir, binary,
			"schema", "drift", "--db-url", scopedURL, "--root-dir", root, "--format", "json")

		c.Assert(exitStatusOf(c, runErr), qt.Equals, 1)
		c.Assert(stderr, qt.Equals, "")
		c.Assert(stdout, qt.Contains, `"updates": 2`)
		c.Assert(stdout, qt.Not(qt.Contains), "42703")
		c.Assert(stdout, qt.Not(qt.Contains), "iso3 does not exist")
	})

	// Nothing the check did wrote to the table. A read-only verb that left a row
	// behind would be the worse defect, and only the server can say.
	c.Assert(regionName(c, ctx, targetDB, "CZ"), qt.Equals, "Czechia")
	c.Assert(regionCount(c, ctx, targetDB), qt.Equals, 2)
}

// regionName reads one row's name back from the server.
func regionName(c *qt.C, ctx context.Context, db *sql.DB, code string) string {
	c.Helper()
	var name string
	err := db.QueryRowContext(ctx, `SELECT name FROM regions WHERE code = $1`, code).Scan(&name)
	c.Assert(err, qt.IsNil)
	return name
}

// regionCount reads how many rows the table holds.
func regionCount(c *qt.C, ctx context.Context, db *sql.DB) int {
	c.Helper()
	var count int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM regions`).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}
