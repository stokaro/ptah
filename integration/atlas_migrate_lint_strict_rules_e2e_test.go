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

// TestAtlasMigrateLintStrictRunsOnlyTheMappedRulesE2E lints a migration that
// truncates and drops a table on PostgreSQL. destructive/drop-table is Atlas
// DS102; destructive/truncate-table has no Atlas check. Both policies print
// the drop under its Atlas code, in the community binary's line shape. The
// default policy also runs the truncate rule and names it, since there is no
// code to print; the strict policy runs only the rule the Atlas table maps, so
// a strict run that dropped everything would fail the second half of the
// strict row.
func TestAtlasMigrateLintStrictRunsOnlyTheMappedRulesE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	binaryPath := filepath.Join(t.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, e2eRepoRoot(t), binaryPath)
	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	lint := func(c *qt.C, run func(context.Context, string, ...string) (string, string, error)) string {
		name := fmt.Sprintf("ptah_lint_strict_e2e_%d", time.Now().UnixNano())
		createE2EDatabase(c, ctx, adminDB, name)
		defer dropE2EDatabase(c, context.Background(), adminDB, name)
		dir := c.TempDir()
		writeLintE2EFile(c, dir, "1.sql", "CREATE TABLE users (id int);\n")
		writeLintE2EFile(c, dir, "2.sql", "TRUNCATE TABLE users;\nDROP TABLE users;\n")
		stdout, _, _ := run(ctx, binaryPath, "migrate", "lint",
			"--dir", "file://"+dir, "--dev-url", replaceDatabaseName(c, dbURL, name), "--latest", "1")
		return stdout
	}

	t.Run("default", func(t *testing.T) {
		c := qt.New(t)
		stdout := lint(c, runLintE2EBinary)

		c.Assert(stdout, qt.Contains, "-- L1 [destructive/truncate-table]: ")
		c.Assert(stdout, qt.Contains, "-- L2: Dropping table \"users\" https://atlasgo.io/lint/analyzers#DS102\n")
		c.Assert(stdout, qt.Not(qt.Contains), "destructive/drop-table")
	})
	t.Run("strict", func(t *testing.T) {
		c := qt.New(t)
		stdout := lint(c, runLintE2EStrictBinary)

		c.Assert(stdout, qt.Contains, "-- L2: Dropping table \"users\" https://atlasgo.io/lint/analyzers#DS102\n")
		c.Assert(stdout, qt.Not(qt.Contains), "truncate")
	})
}
