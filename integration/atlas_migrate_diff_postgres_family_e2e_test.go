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
)

type postgresFamilyMigrateDiffCase struct {
	name   string
	urlEnv string
}

func TestAtlasMigrateDiffPostgresFamilyDevCleanupE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()

	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, repoRoot, binaryPath)

	tests := []postgresFamilyMigrateDiffCase{
		{name: "cockroachdb", urlEnv: "COCKROACHDB_URL"},
		{name: "yugabytedb", urlEnv: "YUGABYTEDB_URL"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runPostgresFamilyMigrateDiffCase(c, ctx, binaryPath, test)
		})
	}
}

func runPostgresFamilyMigrateDiffCase(
	c *qt.C,
	ctx context.Context,
	binaryPath string,
	test postgresFamilyMigrateDiffCase,
) {
	c.Helper()
	adminURL := requireIntegrationEnvironment(c, test.urlEnv)
	adminDB, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()
	c.Assert(adminDB.PingContext(ctx), qt.IsNil)

	suffix := time.Now().UnixNano()
	desiredName := fmt.Sprintf("ptah_diff_desired_%d", suffix)
	devName := fmt.Sprintf("ptah_diff_dev_%d", suffix)
	applyName := fmt.Sprintf("ptah_diff_apply_%d", suffix)
	createE2EDatabase(c, ctx, adminDB, desiredName)
	defer dropPostgresFamilyE2EDatabase(c, adminDB, desiredName)
	createE2EDatabase(c, ctx, adminDB, devName)
	defer dropPostgresFamilyE2EDatabase(c, adminDB, devName)
	createE2EDatabase(c, ctx, adminDB, applyName)
	defer dropPostgresFamilyE2EDatabase(c, adminDB, applyName)

	desiredURL := replaceDatabaseName(c, adminURL, desiredName)
	devURL := replaceDatabaseName(c, adminURL, devName)
	applyURL := replaceDatabaseName(c, adminURL, applyName)
	execPostgresFamilySQL(c, ctx, desiredURL, `
		CREATE TABLE desired_items (
			id BIGINT PRIMARY KEY,
			name TEXT NOT NULL
		)`)
	execPostgresFamilySQL(c, ctx, devURL, `
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
	migrationSQL := readFirstMatchingFile(c, migrationsDir, "*_add_items.sql")
	c.Assert(migrationSQL, qt.Contains, "CREATE TABLE")
	c.Assert(migrationSQL, qt.Contains, "desired_items")
	c.Assert(postgresFamilyObjectCount(c, ctx, desiredURL), qt.Equals, 1)
	c.Assert(postgresFamilyObjectCount(c, ctx, devURL), qt.Equals, 0)

	output, err = runPtah(ctx, c.TempDir(), binaryPath,
		"migrate", "apply",
		"--url", applyURL,
		"--dir", "file://"+migrationsDir)
	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))
	c.Assert(e2eTableCount(c, ctx, postgresFamilyDriverURL(c, applyURL), "desired_items"), qt.Equals, 1)
}

func postgresFamilyDriverURL(c *qt.C, rawURL string) string {
	c.Helper()
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	parsed.Scheme = "postgres"
	return parsed.String()
}

func execPostgresFamilySQL(c *qt.C, ctx context.Context, dbURL, sqlExpr string) {
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbURL))
	c.Assert(err, qt.IsNil)
	defer db.Close()
	_, err = db.ExecContext(ctx, sqlExpr)
	c.Assert(err, qt.IsNil)
}

func postgresFamilyObjectCount(c *qt.C, ctx context.Context, dbURL string) int {
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbURL))
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

func dropPostgresFamilyE2EDatabase(c *qt.C, adminDB *sql.DB, name string) {
	c.Helper()
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := adminDB.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS "+quoteE2EIdent(name))
	c.Assert(err, qt.IsNil)
}
