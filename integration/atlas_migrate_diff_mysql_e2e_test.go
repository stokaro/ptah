//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql"
)

type mySQLMigrateDiffCase struct {
	name        string
	adminDSNEnv string
	adminURLEnv string
}

func TestAtlasMigrateDiffMySQLFamilyDatabaseDesiredStateE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()

	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "atlas")
	buildPtahCompat(c, ctx, repoRoot, binaryPath)

	tests := []mySQLMigrateDiffCase{
		{
			name:        "mysql",
			adminDSNEnv: "MYSQL_ADMIN_TEST_DSN",
			adminURLEnv: "MYSQL_ADMIN_TEST_URL",
		},
		{
			name:        "mariadb",
			adminDSNEnv: "MARIADB_ADMIN_TEST_DSN",
			adminURLEnv: "MARIADB_ADMIN_TEST_URL",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			runMySQLMigrateDiffCase(c, ctx, binaryPath, test)
		})
	}
}

func runMySQLMigrateDiffCase(
	c *qt.C,
	ctx context.Context,
	binaryPath string,
	test mySQLMigrateDiffCase,
) {
	c.Helper()
	adminDSN := requireIntegrationEnvironment(c, test.adminDSNEnv)
	adminURL := requireIntegrationEnvironment(c, test.adminURLEnv)
	adminDB, err := sql.Open("mysql", adminDSN)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()
	c.Assert(adminDB.PingContext(ctx), qt.IsNil)

	suffix := time.Now().UnixNano()
	desiredName := fmt.Sprintf("ptah_diff_desired_%d", suffix)
	devName := fmt.Sprintf("ptah_diff_dev_%d", suffix)
	createMySQLDatabase(c, ctx, adminDB, desiredName)
	defer dropMySQLDatabase(c, context.Background(), adminDB, desiredName)
	createMySQLDatabase(c, ctx, adminDB, devName)
	defer dropMySQLDatabase(c, context.Background(), adminDB, devName)

	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE `%s`.`desired_database_items` (id BIGINT PRIMARY KEY, name VARCHAR(255) NOT NULL)",
		desiredName,
	))
	c.Assert(err, qt.IsNil)

	desiredURL := replaceMySQLDatabaseName(c, adminURL, desiredName)
	devURL := replaceMySQLDatabaseName(c, adminURL, devName)
	workDir := c.TempDir()
	migrationsDir := filepath.Join(workDir, "migrations")
	output, err := runPtah(ctx, workDir, binaryPath,
		"migrate", "diff",
		"--to", desiredURL,
		"--dev-url", devURL,
		"--dir", "file://"+migrationsDir,
		"add_database_items",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))
	migrationSQL := readFirstMatchingFile(c, migrationsDir, "*_add_database_items.sql")
	c.Assert(migrationSQL, qt.Contains, "CREATE TABLE")
	c.Assert(migrationSQL, qt.Contains, "desired_database_items")

	output, err = runPtah(ctx, workDir, binaryPath,
		"migrate", "diff",
		"--to", desiredURL,
		"--dev-url", devURL,
		"--dir", "file://"+migrationsDir,
		"noop",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))
	c.Assert(output, qt.Contains, "The migration directory is synced with the desired state")
	c.Assert(mySQLTableCount(c, ctx, adminDB, desiredName, "desired_database_items"), qt.Equals, 1)
	c.Assert(mySQLTableCount(c, ctx, adminDB, devName, "desired_database_items"), qt.Equals, 1)
}

func requireIntegrationEnvironment(c *qt.C, name string) string {
	c.Helper()
	value := os.Getenv(name)
	if value == "" {
		c.Skipf("%s is not set", name)
	}
	return value
}

func replaceMySQLDatabaseName(c *qt.C, rawURL, database string) string {
	c.Helper()
	base, query, hasQuery := strings.Cut(rawURL, "?")
	slash := strings.LastIndex(base, "/")
	c.Assert(slash >= 0, qt.IsTrue)
	replaced := base[:slash+1] + database
	if hasQuery {
		return replaced + "?" + query
	}
	return replaced
}

func createMySQLDatabase(c *qt.C, ctx context.Context, db *sql.DB, name string) {
	c.Helper()
	_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE `%s`", name))
	c.Assert(err, qt.IsNil)
}

func dropMySQLDatabase(c *qt.C, ctx context.Context, db *sql.DB, name string) {
	c.Helper()
	_, err := db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", name))
	c.Check(err, qt.IsNil)
}

func mySQLTableCount(
	c *qt.C,
	ctx context.Context,
	db *sql.DB,
	database string,
	table string,
) int {
	c.Helper()
	var count int
	err := db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		database,
		table,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}
