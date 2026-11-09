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

// TestMigrationsLintRebuiltViewDoesNotReportOldColumnReaderE2E checks the
// view the file actually holds when its old source column is dropped.
func TestMigrationsLintRebuiltViewDoesNotReportOldColumnReaderE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()

	binary := filepath.Join(t.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	admin, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer admin.Close()
	name := fmt.Sprintf("ptah_lint_view_rebuild_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, admin, name)
	defer dropE2EDatabase(c, context.Background(), admin, name)

	dir := c.TempDir()
	writeLintE2EFile(c, dir, "1.sql", "CREATE TABLE t (a text); CREATE VIEW v AS SELECT a FROM t;")
	writeLintE2EFile(c, dir, "2.sql", "DROP VIEW v; ALTER TABLE t ADD COLUMN b text; "+
		"CREATE VIEW v AS SELECT b FROM t; ALTER TABLE t DROP COLUMN a;")

	stdout, stderr, err := runLintE2EBinary(ctx, binary,
		"migrations", "lint", "--dir", dir, "--dir-format", "atlas",
		"--dev-url", replaceDatabaseName(c, dbURL, name), "--latest", "1", "--fail-on", "none")
	c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stderr, qt.Not(qt.Contains), "ran without the")
	c.Assert(stdout, qt.Contains, "DS102")
	c.Assert(stdout, qt.Not(qt.Contains), "DS110P")
}
