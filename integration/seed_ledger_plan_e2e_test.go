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
	"ptah.run/internal/exeext"
)

// TestSchemaApplyLeavesTheSeedLedgerOnSQLiteE2E seeds a SQLite database and
// plans a declaration that does not name the seed ledger. The reader leaves the
// ledger out, as it leaves out the migration history, so the plan does not
// drop it.
func TestSchemaApplyLeavesTheSeedLedgerOnSQLiteE2E(t *testing.T) {
	c := qt.New(t)

	assertPlanLeavesTheSeedLedger(c, e2eRepoRoot(t), "sqlite://"+filepath.ToSlash(filepath.Join(c.TempDir(), "seeded.db")))
}

// TestSchemaApplyLeavesTheSeedLedgerOnPostgreSQLE2E is the same on
// PostgreSQL.
func TestSchemaApplyLeavesTheSeedLedgerOnPostgreSQLE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)

	assertPlanLeavesTheSeedLedger(c, e2eRepoRoot(t), dbURL)
}

// TestSchemaApplyLeavesTheSeedLedgerOnMySQLE2E is the same on MySQL.
func TestSchemaApplyLeavesTheSeedLedgerOnMySQLE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.MySQL)
	c := qt.New(t)

	assertPlanLeavesTheSeedLedger(c, e2eRepoRoot(t), dbURL)
}

// assertPlanLeavesTheSeedLedger creates a table, seeds it, and plans a
// declaration of that table alone. The plan, which may drop what other tests
// left in a shared database, never names the ledger, and the ledger keeps the
// seed it recorded.
func assertPlanLeavesTheSeedLedger(c *qt.C, repoRoot, dbURL string) {
	c.Helper()
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	binary := filepath.Join(c.TempDir(), "ptah"+exeext.Suffix)
	buildPtah(c, ctx, repoRoot, binary)

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	table := fmt.Sprintf("seed_ledger_e2e_%d", time.Now().UnixNano()%1_000_000_000)
	dropTables := func() {
		for _, name := range []string{table, "schema_seeds"} {
			_, dropErr := conn.ExecContext(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS "+name)
			c.Check(dropErr, qt.IsNil)
		}
	}
	dropTables()
	defer dropTables()

	dir := c.TempDir()
	schema := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, name VARCHAR(64) NOT NULL);\n", table)), 0o600), qt.IsNil)
	seeds := filepath.Join(dir, "seeds")
	c.Assert(os.MkdirAll(seeds, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(seeds, "010_rows.test.sql"), []byte(fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'first');\n", table)), 0o600), qt.IsNil)

	output, err := runPtah(ctx, repoRoot, binary, "schema", "apply", "--db-url", dbURL, "--schema-file", schema, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("schema apply:\n%s", output))
	output, err = runPtah(ctx, repoRoot, binary, "seed", "--db-url", dbURL, "--seeds-dir", seeds, "--env", "test")
	c.Assert(err, qt.IsNil, qt.Commentf("ptah seed:\n%s", output))
	output, err = runPtah(ctx, repoRoot, binary, "schema", "apply", "--db-url", dbURL, "--schema-file", schema, "--dry-run")

	c.Assert(err, qt.IsNil, qt.Commentf("schema apply --dry-run:\n%s", output))
	c.Assert(output, qt.Not(qt.Contains), "schema_seeds")
	var recorded int
	c.Assert(conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_seeds").Scan(&recorded), qt.IsNil)
	c.Assert(recorded, qt.Equals, 1)
}
