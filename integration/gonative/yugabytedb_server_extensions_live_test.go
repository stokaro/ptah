//go:build integration

package gonative_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// serverExtensionEntities declares one table and no extension.
const serverExtensionEntities = `package models

//ptah:schema:table name="ptah3687_notes"
type Note struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}
`

// A declaration that names no extension applies to YugabyteDB, and the
// extensions the server installs in every database are still there afterwards
// (stokaro/ptah#3687). The plan dropped pg_stat_statements and postgres_fdw,
// and YugabyteDB 2026.1 refuses both drops, so the apply failed before it
// created the table. The comparison right after it finds nothing to do.
func TestYugabyteDBApplyLeavesTheServerExtensionsIntegration(t *testing.T) {
	c := qt.New(t)
	url := dbtarget.URL(t, dbtarget.YugabyteDB)
	dsn := requireReachableEngine(t, dbtarget.YugabyteDB, "pgx", "YugabyteDB")
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "models.go"), []byte(serverExtensionEntities), 0o600), qt.IsNil)
	installed := installedExtensions(c, dsn)
	c.Cleanup(func() { dropServerExtensionTable(c, dsn) })

	runNativePtah(c, "schema", "apply", "--db-url", url, "--root-dir", dir, "--auto-approve")

	c.Assert(installedExtensions(c, dsn), qt.DeepEquals, installed)
	c.Assert(installed, qt.Contains, "pg_stat_statements")
	c.Assert(runNativePtah(c, "schema", "compare", "--db-url", url, "--root-dir", dir),
		qt.Contains, "No schema differences detected")
}

// installedExtensions lists the extensions the database holds, by name, in
// order.
func installedExtensions(c *qt.C, dsn string) []string {
	c.Helper()
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	rows, err := db.QueryContext(c.Context(), "SELECT extname FROM pg_extension ORDER BY extname")
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// dropServerExtensionTable removes the table this file created, so a shared
// server is left as it was found.
func dropServerExtensionTable(c *qt.C, dsn string) {
	c.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		c.Logf("cleanup could not open the database: %v", err)
		return
	}
	defer db.Close()
	if _, err := db.Exec("DROP TABLE IF EXISTS ptah3687_notes CASCADE"); err != nil {
		c.Logf("cleanup did not drop the table: %v", err)
	}
}
