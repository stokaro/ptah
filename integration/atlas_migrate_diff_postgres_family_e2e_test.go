//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/internal/dbtarget"
)

// postgresFamilyMigrateDiffCase is one PostgreSQL-family target. The engine is
// a dbtarget.Engine rather than a variable name so this table cannot spell an
// address variable a CI step does not set.
type postgresFamilyMigrateDiffCase struct {
	name   string
	engine dbtarget.Engine
}

func TestAtlasMigrateDiffPostgresFamilyDevCleanupE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()

	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah-compat")
	buildPtahCompat(c.TB, ctx, repoRoot, binaryPath)

	tests := []postgresFamilyMigrateDiffCase{
		{name: "cockroachdb", engine: dbtarget.CockroachDB},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			runPostgresFamilyMigrateDiffCase(c.TB, ctx, binaryPath, test)
		})
	}
}

func runPostgresFamilyMigrateDiffCase(
	tb testing.TB,
	ctx context.Context,
	binaryPath string,
	test postgresFamilyMigrateDiffCase,
) {
	c := qt.New(tb)
	c.Helper()
	adminURL := dbtarget.URL(c, test.engine)
	adminDB, err := sql.Open("pgx", postgresFamilyDriverURL(c.TB, adminURL))
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()
	c.Assert(adminDB.PingContext(ctx), qt.IsNil)

	suffix := time.Now().UnixNano()
	desiredName := fmt.Sprintf("ptah_diff_desired_%d", suffix)
	devName := fmt.Sprintf("ptah_diff_dev_%d", suffix)
	applyName := fmt.Sprintf("ptah_diff_apply_%d", suffix)
	createE2EDatabase(c.TB, ctx, adminDB, desiredName)
	defer dropPostgresFamilyE2EDatabase(c.TB, adminDB, desiredName)
	createE2EDatabase(c.TB, ctx, adminDB, devName)
	defer dropPostgresFamilyE2EDatabase(c.TB, adminDB, devName)
	createE2EDatabase(c.TB, ctx, adminDB, applyName)
	defer dropPostgresFamilyE2EDatabase(c.TB, adminDB, applyName)

	desiredURL := replaceDatabaseName(c.TB, adminURL, desiredName)
	devURL := replaceDatabaseName(c.TB, adminURL, devName)
	applyURL := replaceDatabaseName(c.TB, adminURL, applyName)
	execPostgresFamilySQL(c.TB, ctx, desiredURL, `
		CREATE TABLE desired_items (
			id BIGINT PRIMARY KEY,
			name TEXT NOT NULL
		)`)
	execPostgresFamilySQL(c.TB, ctx, devURL, `
		CREATE TABLE stale_parent (id BIGINT PRIMARY KEY);
		CREATE VIEW stale_parent_view AS SELECT id FROM stale_parent`)

	dir := c.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	output, err := runPtah(ctx, dir, binaryPath,
		"migrate", "diff",
		"--to", desiredURL,
		"--dev-url", devURL,
		"--dir", "file://"+migrationsDir,
		"add_items")

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))
	migrationSQL := readFirstMatchingFile(c.TB, migrationsDir, "*_add_items.sql")
	c.Assert(migrationSQL, qt.Contains, "CREATE TABLE")
	c.Assert(migrationSQL, qt.Contains, "desired_items")
	c.Assert(postgresFamilyObjectCount(c.TB, ctx, desiredURL), qt.Equals, 1)
	c.Assert(postgresFamilyObjectCount(c.TB, ctx, devURL), qt.Equals, 0)

	output, err = runPtah(ctx, c.TempDir(), binaryPath,
		"migrate", "apply",
		"--url", applyURL,
		"--dir", "file://"+migrationsDir)
	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))
	c.Assert(e2eTableCount(c.TB, ctx, postgresFamilyDriverURL(c.TB, applyURL), "desired_items"), qt.Equals, 1)
}

func postgresFamilyDriverURL(tb testing.TB, rawURL string) string {
	c := qt.New(tb)
	c.Helper()
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	parsed.Scheme = "postgres"
	return parsed.String()
}

func execPostgresFamilySQL(tb testing.TB, ctx context.Context, dbURL, sqlExpr string) {
	c := qt.New(tb)
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c.TB, dbURL))
	c.Assert(err, qt.IsNil)
	defer db.Close()
	_, err = db.ExecContext(ctx, sqlExpr)
	c.Assert(err, qt.IsNil)
}

func postgresFamilyObjectCount(tb testing.TB, ctx context.Context, dbURL string) int {
	c := qt.New(tb)
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c.TB, dbURL))
	c.Assert(err, qt.IsNil)
	defer db.Close()

	var count int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = 'public'
		  AND table_type IN ('BASE TABLE', 'VIEW')
		  AND table_name <> 'schema_migrations'
	`).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func dropPostgresFamilyE2EDatabase(tb testing.TB, adminDB *sql.DB, name string) {
	c := qt.New(tb)
	c.Helper()
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := adminDB.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS "+quoteE2EIdent(name))
	c.Assert(err, qt.IsNil)
}
