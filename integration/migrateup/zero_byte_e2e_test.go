//go:build integration

package migrateup_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// TestMigrateUp_AZeroByteFailsTheMigration applies, on PostgreSQL, a migration
// whose second statement holds a zero byte, between two CREATE TABLE
// statements. The server refuses any message that carries the byte, so the
// migration fails, its transaction takes back the first table, and the version
// is not applied. Read as the end of the file, the byte would drop the third
// statement: the run would apply the first table alone and report success.
func TestMigrateUp_AZeroByteFailsTheMigration(t *testing.T) {
	c := qt.New(t)
	url := dbtarget.URL(c, dbtarget.PostgreSQL)
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	suffix := time.Now().UnixNano()
	table := fmt.Sprintf("zero_byte_%d", suffix)
	before, after := fmt.Sprintf("zero_byte_before_%d", suffix), fmt.Sprintf("zero_byte_after_%d", suffix)
	c.Cleanup(func() {
		// The test's context is canceled before its cleanups run.
		for _, name := range []string{table, table + "_log", before, after} {
			_, _ = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+name)
		}
		dbschema.CloseAndWarn(conn)
	})
	dir := c.TempDir()
	up := fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY);\nSELECT 1 \x00;\nCREATE TABLE %s (id INT PRIMARY KEY);\n", before, after)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_step.up.sql"), []byte(up), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_step.down.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)

	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"migrations", "up", "--db-url", url, "--migrations-dir", dir, "--migrations-table", table)

	c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	c.Assert(result.Stderr, qt.Contains, "invalid message format (SQLSTATE 08P01)")
	var tables int
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT count(*) FROM pg_catalog.pg_class WHERE relname IN ($1, $2)", before, after).Scan(&tables), qt.IsNil)
	c.Assert(tables, qt.Equals, 0)
	var applied int
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count(*) FROM "+table+" WHERE state = 'applied'").Scan(&applied), qt.IsNil)
	c.Assert(applied, qt.Equals, 0)
}
