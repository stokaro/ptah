//go:build integration

package integration_test

// What `ptah seed --idempotent` does when a seed file inserts a row the table
// already has.
//
// The flag brackets each file in a savepoint so a duplicate row is rolled back
// without losing the files already applied in the same transaction. The
// statements were the portable spelling on every engine, and two refuse it, so
// the command failed on the first file: SQL Server answers SAVEPOINT with
// `Could not find stored procedure 'SAVEPOINT'` (2812), and Oracle answers
// RELEASE SAVEPOINT with ORA-00900 (stokaro/ptah#3330).
//
// Each engine runs two shapes, because the three statements are not all on one
// path. A seed whose row already exists takes the rollback, and a seed whose row
// is new takes the release -- which is the only statement Oracle refuses, so
// without that shape the Oracle row passes with the portable spelling.
//
// PostgreSQL is the control: it takes all three statements.

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

type idempotentSeedTarget struct {
	name      string
	engine    dbtarget.Engine
	table     string
	create    string
	dropTable string
}

var idempotentSeedTargets = []idempotentSeedTarget{
	{
		name: "postgres", engine: dbtarget.PostgreSQL, table: "ptah_idem_seed",
		create:    "CREATE TABLE ptah_idem_seed (id INT PRIMARY KEY)",
		dropTable: "DROP TABLE IF EXISTS %s",
	},
	{
		name: "sqlserver", engine: dbtarget.SQLServer, table: "ptah_idem_seed",
		create:    "CREATE TABLE ptah_idem_seed (id INT PRIMARY KEY)",
		dropTable: "DROP TABLE IF EXISTS %s",
	},
	{
		name: "oracle", engine: dbtarget.Oracle, table: "PTAH_IDEM_SEED",
		create:    "CREATE TABLE PTAH_IDEM_SEED (id NUMBER PRIMARY KEY)",
		dropTable: oracleDropTableIfExists,
	},
}

// TestSeedIdempotentToleratesADuplicateRowE2E drives the shipped binary.
func TestSeedIdempotentToleratesADuplicateRowE2E(t *testing.T) {
	for _, target := range idempotentSeedTargets {
		t.Run(target.name, func(t *testing.T) {
			dbURL := dbtarget.URL(t, target.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
			defer cancel()

			binary := filepath.Join(c.TempDir(), "ptah"+exeext.Suffix)
			repoRoot := e2eRepoRoot(t)
			buildPtah(c, ctx, repoRoot, binary)

			conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			drop := func() {
				for _, name := range []string{target.table, "schema_seeds"} {
					_, _ = conn.ExecContext(context.WithoutCancel(ctx), fmt.Sprintf(target.dropTable, name))
				}
			}
			drop()
			defer drop()

			_, err = conn.ExecContext(ctx, target.create)
			c.Assert(err, qt.IsNil)
			_, err = conn.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id) VALUES (1)", target.table))
			c.Assert(err, qt.IsNil)

			// 010 duplicates the row the table already has, so it takes the
			// rollback. 020 inserts a new one, so it takes the release.
			seedsDir := filepath.Join(c.TempDir(), "seeds")
			c.Assert(os.MkdirAll(seedsDir, 0o755), qt.IsNil)
			duplicate := fmt.Sprintf("INSERT INTO %s (id) VALUES (1);\n", target.table)
			c.Assert(os.WriteFile(filepath.Join(seedsDir, "010_dup.test.sql"), []byte(duplicate), 0o600), qt.IsNil)
			fresh := fmt.Sprintf("INSERT INTO %s (id) VALUES (2);\n", target.table)
			c.Assert(os.WriteFile(filepath.Join(seedsDir, "020_fresh.test.sql"), []byte(fresh), 0o600), qt.IsNil)

			output, runErr := runPtah(ctx, repoRoot, binary,
				"seed", "--db-url", dbURL, "--seeds-dir", seedsDir, "--env", "test", "--idempotent")
			c.Assert(runErr, qt.IsNil, qt.Commentf("ptah seed --idempotent:\n%s", output))
			c.Assert(output, qt.Contains, "Seeds completed successfully.")

			// The duplicate was rolled back to the savepoint rather than
			// inserted, the new row landed, and both files were recorded.
			var rows int
			c.Assert(conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+target.table).Scan(&rows), qt.IsNil)
			c.Assert(rows, qt.Equals, 2)
			var recorded int
			c.Assert(conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_seeds").Scan(&recorded), qt.IsNil)
			c.Assert(recorded, qt.Equals, 2)
		})
	}
}
