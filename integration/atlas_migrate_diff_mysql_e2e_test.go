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

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/sqlident"
)

type mySQLMigrateDiffCase struct {
	name            string
	adminDSNEnv     string
	adminURLEnv     string
	expectedDialect string
	staleDDL        []string
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
			name:            "mysql",
			adminDSNEnv:     "MYSQL_ADMIN_TEST_DSN",
			adminURLEnv:     "MYSQL_ADMIN_TEST_URL",
			expectedDialect: platform.MySQL,
			staleDDL: []string{
				"CREATE VIEW `%s`.`stale_dev_view` AS SELECT 1 AS id",
				"CREATE FUNCTION `%s`.`stale_dev_function`() RETURNS INTEGER DETERMINISTIC NO SQL RETURN 1",
				"CREATE PROCEDURE `%s`.`stale_dev_procedure`() SELECT 1",
				"CREATE EVENT `%s`.`stale_dev_event` ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 HOUR DO SELECT 1",
			},
		},
		{
			name:            "mariadb",
			adminDSNEnv:     "MARIADB_ADMIN_TEST_DSN",
			adminURLEnv:     "MARIADB_ADMIN_TEST_URL",
			expectedDialect: platform.MariaDB,
			staleDDL: []string{
				"CREATE VIEW `%s`.`stale_dev_view` AS SELECT 1 AS id",
				"CREATE FUNCTION `%s`.`stale_dev_function`() RETURNS INTEGER DETERMINISTIC NO SQL RETURN 1",
				"CREATE PROCEDURE `%s`.`stale_dev_procedure`() SELECT 1",
				"CREATE EVENT `%s`.`stale_dev_event` ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 HOUR DO SELECT 1",
				"CREATE SEQUENCE `%s`.`stale_dev_sequence` START WITH 1",
				"CREATE TABLE `%s`.`stale_dev_versioned` (" + `
					id BIGINT PRIMARY KEY,
					row_start TIMESTAMP(6) GENERATED ALWAYS AS ROW START,
					row_end TIMESTAMP(6) GENERATED ALWAYS AS ROW END,
					PERIOD FOR SYSTEM_TIME (row_start, row_end)
				) WITH SYSTEM VERSIONING`,
			},
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
	applyName := fmt.Sprintf("ptah_diff_apply_%d", suffix)
	externalName := fmt.Sprintf("ptah_diff_external_%d", suffix)
	createMySQLDatabase(c, ctx, adminDB, desiredName)
	defer dropMySQLDatabase(c, context.Background(), adminDB, desiredName)
	createMySQLDatabase(c, ctx, adminDB, devName)
	defer dropMySQLDatabase(c, context.Background(), adminDB, devName)
	createMySQLDatabase(c, ctx, adminDB, applyName)
	defer dropMySQLDatabase(c, context.Background(), adminDB, applyName)
	createMySQLDatabase(c, ctx, adminDB, externalName)
	defer dropMySQLDatabase(c, context.Background(), adminDB, externalName)

	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE `%s`.`desired_database_items` (id BIGINT PRIMARY KEY, name VARCHAR(255) NOT NULL)",
		desiredName,
	))
	c.Assert(err, qt.IsNil)

	desiredURL := replaceMySQLDatabaseName(c, adminURL, desiredName)
	devURL := replaceMySQLDatabaseName(c, adminURL, devName)
	publicConnection, err := dbschema.ConnectToDatabase(ctx, asMySQLURL(desiredURL))
	c.Assert(err, qt.IsNil)
	defer publicConnection.Close()
	c.Assert(publicConnection.Info().Dialect, qt.Equals, test.expectedDialect)

	for _, ddl := range test.staleDDL {
		_, err = adminDB.ExecContext(ctx, fmt.Sprintf(ddl, devName))
		c.Assert(err, qt.IsNil)
	}
	_, err = adminDB.ExecContext(
		ctx,
		fmt.Sprintf("CREATE TABLE `%s`.`dependency_parent` (id BIGINT PRIMARY KEY)", devName),
	)
	c.Assert(err, qt.IsNil)
	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE VIEW `%s`.`external_parent_view` AS SELECT id FROM `%s`.`dependency_parent`",
		externalName,
		devName,
	))
	c.Assert(err, qt.IsNil)
	rejectionDir := c.TempDir()
	rejectionMigrationsDir := filepath.Join(rejectionDir, "migrations")
	output, err := runPtah(ctx, rejectionDir, binaryPath,
		"migrate", "diff",
		"--to", desiredURL,
		"--dev-url", devURL,
		"--dir", "file://"+rejectionMigrationsDir,
		"must_reject_external_view",
	)
	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "views from other databases reference it")
	c.Assert(mySQLTableCount(c, ctx, adminDB, devName, "dependency_parent"), qt.Equals, 1)
	c.Assert(mySQLTableCount(c, ctx, adminDB, externalName, "external_parent_view"), qt.Equals, 1)
	_, err = adminDB.ExecContext(ctx, fmt.Sprintf("DROP VIEW `%s`.`external_parent_view`", externalName))
	c.Assert(err, qt.IsNil)

	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(`
CREATE TABLE %[1]s.external_child (
	id BIGINT PRIMARY KEY,
	parent_id BIGINT,
	CONSTRAINT fk_external_parent
		FOREIGN KEY (parent_id) REFERENCES %[2]s.dependency_parent(id)
)`, sqlident.Quote(platform.MySQL, externalName), sqlident.Quote(platform.MySQL, devName)))
	c.Assert(err, qt.IsNil)

	foreignKeyRejectionDir := c.TempDir()
	foreignKeyRejectionMigrationsDir := filepath.Join(foreignKeyRejectionDir, "migrations")
	output, err = runPtah(ctx, foreignKeyRejectionDir, binaryPath,
		"migrate", "diff",
		"--to", desiredURL,
		"--dev-url", devURL,
		"--dir", "file://"+foreignKeyRejectionMigrationsDir,
		"must_reject_external_foreign_key",
	)
	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "foreign key constraints from other databases reference it")
	c.Assert(mySQLTableCount(c, ctx, adminDB, devName, "dependency_parent"), qt.Equals, 1)
	c.Assert(mySQLTableCount(c, ctx, adminDB, externalName, "external_child"), qt.Equals, 1)

	_, err = adminDB.ExecContext(ctx, fmt.Sprintf("DROP DATABASE `%s`", externalName))
	c.Assert(err, qt.IsNil)
	_, err = adminDB.ExecContext(ctx, fmt.Sprintf("DROP TABLE `%s`.`dependency_parent`", devName))
	c.Assert(err, qt.IsNil)

	workDir := c.TempDir()
	migrationsDir := filepath.Join(workDir, "migrations")
	output, err = runPtah(ctx, workDir, binaryPath,
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
	c.Assert(mySQLUserObjectCount(c, ctx, adminDB, devName), qt.Equals, 0)

	applyURL := replaceMySQLDatabaseName(c, adminURL, applyName)
	output, err = runPtah(ctx, workDir, binaryPath,
		"migrate", "apply",
		"--url", applyURL,
		"--dir", "file://"+migrationsDir,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))
	c.Assert(mySQLTableCount(c, ctx, adminDB, applyName, "desired_database_items"), qt.Equals, 1)

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
	c.Assert(mySQLUserObjectCount(c, ctx, adminDB, devName), qt.Equals, 0)
}

func asMySQLURL(rawURL string) string {
	withoutScheme := strings.TrimPrefix(strings.TrimPrefix(rawURL, "mysql://"), "mariadb://")
	return "mysql://" + withoutScheme
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

func mySQLUserObjectCount(
	c *qt.C,
	ctx context.Context,
	db *sql.DB,
	database string,
) int {
	c.Helper()
	var count int
	err := db.QueryRowContext(
		ctx,
		`SELECT SUM(object_count)
		FROM (
			SELECT COUNT(*) AS object_count
			FROM information_schema.tables
			WHERE table_schema = ?
			UNION ALL
			SELECT COUNT(*)
			FROM information_schema.routines
			WHERE routine_schema = ?
			UNION ALL
			SELECT COUNT(*)
			FROM information_schema.events
			WHERE event_schema = ?
		) AS user_objects`,
		database, database, database,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}
