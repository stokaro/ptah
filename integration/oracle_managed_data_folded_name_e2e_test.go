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

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// TestOracleManagedDataFoldedNameE2E_HappyPath drives `ptah migrations data`
// against a live Oracle server for a declaration written in lower case against
// a table Oracle created bare.
//
// Oracle decides the answer. A bare `CREATE TABLE ptah_x (code ...)` stores
// PTAH_X and CODE, and the declaration that names `ptah_x` and keys on `code`
// names that same table. Compared exactly, the declaration matched no live
// table: emptying it exited 2 with "it was not found in the live schema" for a
// table that is there (stokaro/ptah#3321).
//
// The migration is applied and rolled back through the server, because an exit
// code says nothing about whether the rollback Oracle is handed is one it
// takes.
func TestOracleManagedDataFoldedNameE2E_HappyPath(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	// Registered as a cleanup rather than deferred, and registered first so it
	// runs last: cleanups run after the test function returns and in reverse
	// order, so a deferred close would shut the connection before the table
	// drops below reach the server, and each run would leave its table behind.
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	repoRoot := e2eRepoRoot(t)
	workDir := c.TempDir()
	binary := filepath.Join(workDir, "ptah")
	buildPtah(c, ctx, repoRoot, binary)

	suffix := time.Now().UnixNano() % 100000000

	t.Run("an emptied declaration deletes every row and rolls back", func(t *testing.T) {
		c := qt.New(t)
		table := fmt.Sprintf("ptah_md3321_full_%d", suffix)
		createOracleFoldedTable(c, ctx, conn, table)
		root, migrationsDir := writeOracleFoldedFixture(c, table, "[]\n")

		stdout, stderr, runErr := runCLIProcess(ctx, workDir, binary,
			"migrations", "data", "--db-url", dbURL, "--root-dir", root,
			"--migrations-dir", migrationsDir, "--allow-destructive", "--description", "folded")

		c.Assert(exitStatusOf(c, runErr), qt.Equals, 0)
		c.Assert(stderr, qt.Equals, "")
		c.Assert(stdout, qt.Contains, "Wrote data migration version 1")

		up := readOracleMigrationFile(c, migrationsDir, "0000000001_folded.up.sql")
		c.Assert(up, qt.Contains, fmt.Sprintf("DELETE FROM %s WHERE code = 'US';", table))
		c.Assert(up, qt.Contains, fmt.Sprintf("DELETE FROM %s WHERE code = 'CZ';", table))
		// The rollback re-inserts every column the catalog reports, under the
		// catalog's spelling except the key the declaration named.
		down := readOracleMigrationFile(c, migrationsDir, "0000000001_folded.down.sql")
		c.Assert(down, qt.Contains, fmt.Sprintf("INSERT INTO %s (LABEL, code) VALUES ('United States', 'US');", table))
		c.Assert(down, qt.Contains, fmt.Sprintf("INSERT INTO %s (LABEL, code) VALUES ('Czechia', 'CZ');", table))

		for _, statement := range oracleMigrationStatements(up) {
			c.Assert(conn.SchemaWriter().ExecuteSQL(ctx, statement), qt.IsNil)
		}
		c.Assert(oracleFoldedRows(c, ctx, conn, table), qt.HasLen, 0)

		for _, statement := range oracleMigrationStatements(down) {
			c.Assert(conn.SchemaWriter().ExecuteSQL(ctx, statement), qt.IsNil)
		}
		c.Assert(oracleFoldedRows(c, ctx, conn, table), qt.DeepEquals,
			[]string{"CZ:Czechia", "US:United States"})
	})

	t.Run("a partial change updates and deletes under the folded name", func(t *testing.T) {
		c := qt.New(t)
		table := fmt.Sprintf("ptah_md3321_part_%d", suffix)
		createOracleFoldedTable(c, ctx, conn, table)
		root, migrationsDir := writeOracleFoldedFixture(c, table,
			"- code: US\n  label: United States of America\n")

		stdout, stderr, runErr := runCLIProcess(ctx, workDir, binary,
			"migrations", "data", "--db-url", dbURL, "--root-dir", root,
			"--migrations-dir", migrationsDir, "--allow-destructive", "--description", "folded")

		c.Assert(exitStatusOf(c, runErr), qt.Equals, 0)
		c.Assert(stderr, qt.Equals, "")
		c.Assert(stdout, qt.Contains, "Wrote data migration version 1")

		up := readOracleMigrationFile(c, migrationsDir, "0000000001_folded.up.sql")
		c.Assert(up, qt.Contains,
			fmt.Sprintf("UPDATE %s SET label = 'United States of America' WHERE code = 'US';", table))
		c.Assert(up, qt.Contains, fmt.Sprintf("DELETE FROM %s WHERE code = 'CZ';", table))

		for _, statement := range oracleMigrationStatements(up) {
			c.Assert(conn.SchemaWriter().ExecuteSQL(ctx, statement), qt.IsNil)
		}
		c.Assert(oracleFoldedRows(c, ctx, conn, table), qt.DeepEquals,
			[]string{"US:United States of America"})
	})
}

// TestOracleManagedDataFoldedNameE2E_FailurePath is the refusal the missing
// lookup was hiding. A migration body cannot write a column the table does not
// have, and that refusal runs only once the declaration's table has been found:
// with the lookup missing, the read went out naming the column and Oracle
// answered for it with ORA-00904 instead.
func TestOracleManagedDataFoldedNameE2E_FailurePath(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	// Registered as a cleanup rather than deferred, and registered first so it
	// runs last: cleanups run after the test function returns and in reverse
	// order, so a deferred close would shut the connection before the table
	// drops below reach the server, and each run would leave its table behind.
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	repoRoot := e2eRepoRoot(t)
	workDir := c.TempDir()
	binary := filepath.Join(workDir, "ptah")
	buildPtah(c, ctx, repoRoot, binary)

	table := fmt.Sprintf("ptah_md3321_col_%d", time.Now().UnixNano()%100000000)
	createOracleFoldedTable(c, ctx, conn, table)
	root, migrationsDir := writeOracleFoldedFixture(c, table,
		"- code: US\n  label: United States\n  note: added later\n")

	stdout, stderr, runErr := runCLIProcess(ctx, workDir, binary,
		"migrations", "data", "--db-url", dbURL, "--root-dir", root,
		"--migrations-dir", migrationsDir, "--allow-destructive", "--description", "folded")

	c.Assert(exitStatusOf(c, runErr), qt.Equals, 2)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Contains,
		fmt.Sprintf(`managed table %q does not have declared column(s) "note"`, table))
	// The discriminator: the refusal is Ptah's, decided before any read, not
	// the server's answer to a column it could not resolve.
	c.Assert(stderr, qt.Not(qt.Contains), "ORA-00904")
	entries, readErr := os.ReadDir(migrationsDir)
	c.Assert(readErr, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}

// createOracleFoldedTable creates a table with a bare, lower-case name, which
// Oracle folds to upper case, seeds two rows, and drops it when the test ends.
func createOracleFoldedTable(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, table string) {
	c.Helper()
	dropOracleTable(ctx, conn, table)
	c.Cleanup(func() { dropOracleTable(context.WithoutCancel(ctx), conn, table) })

	c.Assert(conn.SchemaWriter().ExecuteSQL(ctx, fmt.Sprintf(
		`CREATE TABLE %s (code VARCHAR2(10) PRIMARY KEY, label VARCHAR2(40))`, table)), qt.IsNil)
	c.Assert(conn.SchemaWriter().ExecuteSQL(ctx, fmt.Sprintf(
		`INSERT INTO %s (code, label) VALUES ('US', 'United States')`, table)), qt.IsNil)
	c.Assert(conn.SchemaWriter().ExecuteSQL(ctx, fmt.Sprintf(
		`INSERT INTO %s (code, label) VALUES ('CZ', 'Czechia')`, table)), qt.IsNil)
	// The precondition, read back from the server: the bare name was folded.
	c.Assert(oracleFoldedRows(c, ctx, conn, table), qt.DeepEquals,
		[]string{"CZ:Czechia", "US:United States"})
}

// writeOracleFoldedFixture declares the folded table under the lower-case name
// its author wrote, and returns the source root and an empty migrations
// directory.
func writeOracleFoldedFixture(c *qt.C, table, rows string) (root, migrationsDir string) {
	c.Helper()
	dir := c.TempDir()
	root = filepath.Join(dir, "entities")
	migrationsDir = filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(root, 0o750), qt.IsNil)
	c.Assert(os.MkdirAll(migrationsDir, 0o750), qt.IsNil)

	source := fmt.Sprintf(
		"package entities\n\n//ptah:schema:data table=%q key=\"code\" file=\"rows.yaml\"\ntype Probe struct{}\n",
		table)
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(source), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "rows.yaml"), []byte(rows), 0o600), qt.IsNil)
	return root, migrationsDir
}

// oracleFoldedRows reads the table back as "CODE:LABEL" pairs in key order.
func oracleFoldedRows(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, table string) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, fmt.Sprintf(`SELECT code, label FROM %s ORDER BY code`, table))
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	got := []string{}
	for rows.Next() {
		var code, label string
		c.Assert(rows.Scan(&code, &label), qt.IsNil)
		got = append(got, fmt.Sprintf("%s:%s", code, label))
	}
	c.Assert(rows.Err(), qt.IsNil)
	return got
}
