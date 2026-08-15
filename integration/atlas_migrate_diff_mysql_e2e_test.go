//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	mysqlschema "go.5x5.cz/ptah/internal/dbschema/mysql"
	"go.5x5.cz/ptah/internal/sqlident"
)

type mySQLMigrateDiffCase struct {
	name            string
	adminDSNEnv     string
	adminURLEnv     string
	expectedDialect string
	staleDDL        []string
}

type mySQLCleanupRaceCase struct {
	name               string
	externalDDL        string
	externalObjectName string
}

type mySQLUnlockGate struct {
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
}

type mySQLUnlockGateConnector struct {
	driver.Connector
	gate *mySQLUnlockGate
}

type mySQLUnlockGateConn struct {
	driver.Conn
	gate *mySQLUnlockGate
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

func TestMySQLFamilyLockedDropRejectsConcurrentDependenciesE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	engines := []struct {
		config mySQLMigrateDiffCase
		races  []mySQLCleanupRaceCase
	}{
		{
			config: mySQLMigrateDiffCase{
				name:        "mysql",
				adminDSNEnv: "MYSQL_ADMIN_TEST_DSN",
			},
			races: []mySQLCleanupRaceCase{
				{
					name: "external view",
					externalDDL: "CREATE VIEW `%[1]s`.`racing_view` AS " +
						"SELECT id FROM `%[2]s`.`dependency_parent`",
					externalObjectName: "racing_view",
				},
				{
					name: "external foreign key",
					externalDDL: "CREATE TABLE `%[1]s`.`racing_child` (" +
						"id BIGINT PRIMARY KEY, parent_id BIGINT, " +
						"CONSTRAINT `fk_racing_parent` FOREIGN KEY (parent_id) " +
						"REFERENCES `%[2]s`.`dependency_parent` (id))",
					externalObjectName: "racing_child",
				},
			},
		},
		{
			config: mySQLMigrateDiffCase{
				name:        "mariadb",
				adminDSNEnv: "MARIADB_ADMIN_TEST_DSN",
			},
			races: []mySQLCleanupRaceCase{
				{
					name: "external view",
					externalDDL: "CREATE VIEW `%[1]s`.`racing_view` AS " +
						"SELECT id FROM `%[2]s`.`dependency_parent`",
					externalObjectName: "racing_view",
				},
			},
		},
	}

	for _, engine := range engines {
		t.Run(engine.config.name, func(t *testing.T) {
			c := qt.New(t)
			for _, race := range engine.races {
				c.Run(race.name, func(c *qt.C) {
					runMySQLLockedDropRace(c, ctx, engine.config, race)
				})
			}
		})
	}
}

func TestMariaDBLockedDropFailsClosedForConcurrentForeignKeyE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	runMariaDBLockedDropForeignKeyFailClosed(c, ctx)
}

func TestMySQLFamilyDropViewUnderExplicitLockIsRejectedE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	engines := []mySQLMigrateDiffCase{
		{
			name:        "mysql",
			adminDSNEnv: "MYSQL_ADMIN_TEST_DSN",
		},
		{
			name:        "mariadb",
			adminDSNEnv: "MARIADB_ADMIN_TEST_DSN",
		},
	}

	for _, engine := range engines {
		c.Run(engine.name, func(c *qt.C) {
			runMySQLLockedViewDrop(c, ctx, engine)
		})
	}
}

func TestMySQLFamilyProtectedViewDropWinsMetadataLockHandoffE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	engines := []mySQLMigrateDiffCase{
		{
			name:            "mysql",
			adminDSNEnv:     "MYSQL_ADMIN_TEST_DSN",
			expectedDialect: platform.MySQL,
		},
		{
			name:            "mariadb",
			adminDSNEnv:     "MARIADB_ADMIN_TEST_DSN",
			expectedDialect: platform.MariaDB,
		},
	}

	for _, engine := range engines {
		c.Run(engine.name, func(c *qt.C) {
			runMySQLProtectedViewDropHandoff(c, ctx, engine)
		})
	}
}

func runMySQLLockedDropRace(
	c *qt.C,
	ctx context.Context,
	engine mySQLMigrateDiffCase,
	race mySQLCleanupRaceCase,
) {
	c.Helper()
	adminDSN := requireIntegrationEnvironment(c, engine.adminDSNEnv)
	adminDB, err := sql.Open("mysql", adminDSN)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()
	c.Assert(adminDB.PingContext(ctx), qt.IsNil)

	suffix := time.Now().UnixNano()
	devName := fmt.Sprintf("ptah_cleanup_race_dev_%d", suffix)
	externalName := fmt.Sprintf("ptah_cleanup_race_external_%d", suffix)
	createMySQLDatabase(c, ctx, adminDB, devName)
	defer dropMySQLDatabase(c, context.Background(), adminDB, devName)
	createMySQLDatabase(c, ctx, adminDB, externalName)
	defer dropMySQLDatabase(c, context.Background(), adminDB, externalName)

	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE `%s`.`dependency_parent` (id BIGINT PRIMARY KEY)",
		devName,
	))
	c.Assert(err, qt.IsNil)

	lockConn, err := adminDB.Conn(ctx)
	c.Assert(err, qt.IsNil)
	defer lockConn.Close()
	defer func() {
		_, unlockErr := lockConn.ExecContext(context.Background(), "UNLOCK TABLES")
		c.Check(unlockErr, qt.IsNil)
	}()

	lockSQL := fmt.Sprintf("LOCK TABLES `%s`.`dependency_parent` WRITE", devName)
	_, err = lockConn.ExecContext(ctx, lockSQL)
	c.Assert(err, qt.IsNil)

	externalSQL := fmt.Sprintf(race.externalDDL, externalName, devName)
	externalDone := make(chan error, 1)
	go func() {
		_, createErr := adminDB.ExecContext(ctx, externalSQL)
		externalDone <- createErr
	}()
	c.Assert(waitForMySQLMetadataWait(ctx, adminDB, externalSQL), qt.IsNil)

	dropSQL := fmt.Sprintf("DROP TABLE `%s`.`dependency_parent`", devName)
	_, err = lockConn.ExecContext(ctx, dropSQL)
	c.Assert(err, qt.IsNil)
	_, err = lockConn.ExecContext(ctx, "UNLOCK TABLES")
	c.Assert(err, qt.IsNil)
	c.Assert(<-externalDone, qt.IsNotNil)
	c.Assert(mySQLTableCount(c, ctx, adminDB, devName, "dependency_parent"), qt.Equals, 0)
	c.Assert(mySQLTableCount(c, ctx, adminDB, externalName, race.externalObjectName), qt.Equals, 0)
}

func runMySQLLockedViewDrop(
	c *qt.C,
	ctx context.Context,
	engine mySQLMigrateDiffCase,
) {
	c.Helper()
	adminDSN := requireIntegrationEnvironment(c, engine.adminDSNEnv)
	adminDB, err := sql.Open("mysql", adminDSN)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()
	c.Assert(adminDB.PingContext(ctx), qt.IsNil)

	database := fmt.Sprintf("ptah_locked_view_drop_%d", time.Now().UnixNano())
	createMySQLDatabase(c, ctx, adminDB, database)
	defer dropMySQLDatabase(c, context.Background(), adminDB, database)
	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE `%s`.`source_table` (id BIGINT PRIMARY KEY)",
		database,
	))
	c.Assert(err, qt.IsNil)
	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE VIEW `%s`.`managed_view` AS SELECT id FROM `%s`.`source_table`",
		database,
		database,
	))
	c.Assert(err, qt.IsNil)

	lockConn, err := adminDB.Conn(ctx)
	c.Assert(err, qt.IsNil)
	defer lockConn.Close()
	_, err = lockConn.ExecContext(ctx, fmt.Sprintf(
		"LOCK TABLES `%s`.`managed_view` WRITE",
		database,
	))
	c.Assert(err, qt.IsNil)
	_, err = lockConn.ExecContext(ctx, fmt.Sprintf(
		"DROP VIEW `%s`.`managed_view`",
		database,
	))
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "active locked tables")
	_, err = lockConn.ExecContext(ctx, "UNLOCK TABLES")
	c.Assert(err, qt.IsNil)
	c.Assert(mySQLTableCount(c, ctx, adminDB, database, "managed_view"), qt.Equals, 1)
}

func runMySQLProtectedViewDropHandoff(
	c *qt.C,
	ctx context.Context,
	engine mySQLMigrateDiffCase,
) {
	c.Helper()
	adminDSN := requireIntegrationEnvironment(c, engine.adminDSNEnv)
	adminDB, err := sql.Open("mysql", adminDSN)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()
	c.Assert(adminDB.PingContext(ctx), qt.IsNil)

	suffix := time.Now().UnixNano()
	devName := fmt.Sprintf("ptah_view_handoff_dev_%d", suffix)
	externalName := fmt.Sprintf("ptah_view_handoff_external_%d", suffix)
	createMySQLDatabase(c, ctx, adminDB, devName)
	defer dropMySQLDatabase(c, context.Background(), adminDB, devName)
	createMySQLDatabase(c, ctx, adminDB, externalName)
	defer dropMySQLDatabase(c, context.Background(), adminDB, externalName)
	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE `%s`.`source_table` (id BIGINT PRIMARY KEY)",
		devName,
	))
	c.Assert(err, qt.IsNil)
	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE VIEW `%[1]s`.`managed_view` AS SELECT id FROM `%[1]s`.`source_table`",
		devName,
	))
	c.Assert(err, qt.IsNil)

	config, err := mysqldriver.ParseDSN(adminDSN)
	c.Assert(err, qt.IsNil)
	config.DBName = devName
	connector, err := mysqldriver.NewConnector(config)
	c.Assert(err, qt.IsNil)
	gate := newMySQLUnlockGate()
	cleanupDB := sql.OpenDB(&mySQLUnlockGateConnector{
		Connector: connector,
		gate:      gate,
	})
	defer cleanupDB.Close()
	defer gate.releaseUnlock()

	writer := mysqlschema.NewMySQLWriter(cleanupDB, devName, engine.expectedDialect)
	cleanupDone := make(chan error, 1)
	go func() {
		cleanupDone <- writer.DropAllTables(ctx)
	}()

	dropSQL := fmt.Sprintf("DROP VIEW IF EXISTS `%s`.`managed_view`", devName)
	c.Assert(waitForMySQLUnlockGate(ctx, gate.started), qt.IsNil)
	c.Assert(waitForMySQLMetadataWait(ctx, adminDB, dropSQL), qt.IsNil)

	//nolint:gosec // G201: generated catalog identifiers are emitted only through identifier quoting.
	competitorSQL := fmt.Sprintf(
		"CREATE VIEW %s AS SELECT id FROM %s",
		sqlident.Qualified(platform.MySQL, externalName, "competing_view"),
		sqlident.Qualified(platform.MySQL, devName, "managed_view"),
	)
	competitorDone := make(chan error, 1)
	go func() {
		_, createErr := adminDB.ExecContext(ctx, competitorSQL)
		competitorDone <- createErr
	}()
	c.Assert(waitForMySQLMetadataWait(ctx, adminDB, competitorSQL), qt.IsNil)

	gate.releaseUnlock()
	c.Assert(<-cleanupDone, qt.IsNil)
	c.Assert(<-competitorDone, qt.IsNotNil)
	c.Assert(mySQLUserObjectCount(c, ctx, adminDB, devName), qt.Equals, 0)
	c.Assert(mySQLTableCount(c, ctx, adminDB, externalName, "competing_view"), qt.Equals, 0)
}

func runMariaDBLockedDropForeignKeyFailClosed(c *qt.C, ctx context.Context) {
	c.Helper()
	adminDSN := requireIntegrationEnvironment(c, "MARIADB_ADMIN_TEST_DSN")
	adminDB, err := sql.Open("mysql", adminDSN)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()
	c.Assert(adminDB.PingContext(ctx), qt.IsNil)

	suffix := time.Now().UnixNano()
	devName := fmt.Sprintf("ptah_locked_fk_dev_%d", suffix)
	externalName := fmt.Sprintf("ptah_locked_fk_external_%d", suffix)
	createMySQLDatabase(c, ctx, adminDB, devName)
	defer dropMySQLDatabase(c, context.Background(), adminDB, devName)
	createMySQLDatabase(c, ctx, adminDB, externalName)
	defer dropMySQLDatabase(c, context.Background(), adminDB, externalName)
	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE `%s`.`dependency_parent` (id BIGINT PRIMARY KEY)",
		devName,
	))
	c.Assert(err, qt.IsNil)

	lockConn, err := adminDB.Conn(ctx)
	c.Assert(err, qt.IsNil)
	defer lockConn.Close()
	_, err = lockConn.ExecContext(ctx, fmt.Sprintf(
		"LOCK TABLES `%s`.`dependency_parent` WRITE",
		devName,
	))
	c.Assert(err, qt.IsNil)
	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE `%[1]s`.`racing_child` ("+
			"id BIGINT PRIMARY KEY, parent_id BIGINT, "+
			"CONSTRAINT `fk_racing_parent` FOREIGN KEY (parent_id) "+
			"REFERENCES `%[2]s`.`dependency_parent` (id))",
		externalName,
		devName,
	))
	c.Assert(err, qt.IsNil)

	_, err = lockConn.ExecContext(ctx, fmt.Sprintf(
		"DROP TABLE IF EXISTS `%s`.`dependency_parent`",
		devName,
	))
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "foreign key constraint fails")
	_, err = lockConn.ExecContext(ctx, "UNLOCK TABLES")
	c.Assert(err, qt.IsNil)
	c.Assert(mySQLTableCount(c, ctx, adminDB, devName, "dependency_parent"), qt.Equals, 1)
	c.Assert(mySQLTableCount(c, ctx, adminDB, externalName, "racing_child"), qt.Equals, 1)
	c.Assert(mySQLForeignKeyCount(c, ctx, adminDB, externalName, "fk_racing_parent"), qt.Equals, 1)
}

func waitForMySQLMetadataWait(
	ctx context.Context,
	db *sql.DB,
	statement string,
) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		var count int
		err := db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM information_schema.processlist
			WHERE state = 'Waiting for table metadata lock'
			  AND info = ?
		`, statement).Scan(&count)
		if err != nil {
			return fmt.Errorf("inspect MySQL metadata wait: %w", err)
		}
		if count > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for MySQL metadata lock on %q: %w", statement, ctx.Err())
		case <-ticker.C:
		}
	}
}

func newMySQLUnlockGate() *mySQLUnlockGate {
	return &mySQLUnlockGate{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (gate *mySQLUnlockGate) releaseUnlock() {
	gate.releaseOnce.Do(func() {
		close(gate.release)
	})
}

func (c *mySQLUnlockGateConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &mySQLUnlockGateConn{
		Conn: conn,
		gate: c.gate,
	}, nil
}

func (conn *mySQLUnlockGateConn) ExecContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Result, error) {
	execer, ok := conn.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	if strings.EqualFold(strings.TrimSpace(query), "UNLOCK TABLES") {
		conn.gate.startedOnce.Do(func() {
			close(conn.gate.started)
		})
		select {
		case <-conn.gate.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return execer.ExecContext(ctx, query, args)
}

func (conn *mySQLUnlockGateConn) QueryContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Rows, error) {
	queryer, ok := conn.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return queryer.QueryContext(ctx, query, args)
}

func waitForMySQLUnlockGate(ctx context.Context, started <-chan struct{}) error {
	select {
	case <-started:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for protected view-drop unlock: %w", ctx.Err())
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

	limitedUser := fmt.Sprintf("ptah_limited_%d", suffix)
	const limitedPassword = "Ptah842!Limited"
	createMySQLUser(c, ctx, adminDB, limitedUser, limitedPassword)
	defer dropMySQLUser(c, context.Background(), adminDB, limitedUser)
	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(
		"GRANT ALL PRIVILEGES ON `%s`.* TO '%s'@'%%'",
		devName,
		limitedUser,
	))
	c.Assert(err, qt.IsNil)

	limitedRejectionDir := c.TempDir()
	limitedMigrationsDir := filepath.Join(limitedRejectionDir, "migrations")
	limitedDevURL := replaceMySQLCredentials(
		c,
		devURL,
		limitedUser,
		limitedPassword,
	)
	output, err := runPtah(ctx, limitedRejectionDir, binaryPath,
		"migrate", "diff",
		"--to", desiredURL,
		"--dev-url", limitedDevURL,
		"--dir", "file://"+limitedMigrationsDir,
		"must_reject_limited_metadata",
	)
	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "global SELECT")
	c.Assert(output, qt.Contains, "complete metadata visibility")
	c.Assert(mySQLTableCount(c, ctx, adminDB, devName, "dependency_parent"), qt.Equals, 1)

	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE VIEW `%s`.`external_parent_view` AS SELECT id FROM `%s`.`dependency_parent`",
		externalName,
		devName,
	))
	c.Assert(err, qt.IsNil)
	rejectionDir := c.TempDir()
	rejectionMigrationsDir := filepath.Join(rejectionDir, "migrations")
	output, err = runPtah(ctx, rejectionDir, binaryPath,
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

	managedViewCleanupDir := c.TempDir()
	managedViewMigrationsDir := filepath.Join(managedViewCleanupDir, "migrations")
	output, err = runPtah(ctx, managedViewCleanupDir, binaryPath,
		"migrate", "diff",
		"--to", desiredURL,
		"--dev-url", devURL,
		"--dir", "file://"+managedViewMigrationsDir,
		"clean_managed_view",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))
	c.Assert(mySQLUserObjectCount(c, ctx, adminDB, devName), qt.Equals, 0)

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

func replaceMySQLCredentials(
	c *qt.C,
	rawURL,
	username,
	password string,
) string {
	c.Helper()
	schemeEnd := strings.Index(rawURL, "://")
	c.Assert(schemeEnd >= 0, qt.IsTrue)
	authorityStart := schemeEnd + len("://")
	pathOffset := strings.Index(rawURL[authorityStart:], "/")
	c.Assert(pathOffset >= 0, qt.IsTrue)
	pathStart := authorityStart + pathOffset
	authority := rawURL[authorityStart:pathStart]
	at := strings.LastIndex(authority, "@")
	c.Assert(at >= 0, qt.IsTrue)
	credentials := url.UserPassword(username, password).String()
	if strings.Contains(rawURL, "@tcp(") {
		credentials = username + ":" + password
	}
	return rawURL[:authorityStart] + credentials + "@" + authority[at+1:] + rawURL[pathStart:]
}

func createMySQLUser(
	c *qt.C,
	ctx context.Context,
	db *sql.DB,
	username,
	password string,
) {
	c.Helper()
	_, err := db.ExecContext(ctx, fmt.Sprintf(
		"CREATE USER '%s'@'%%' IDENTIFIED BY '%s'",
		username,
		password,
	))
	c.Assert(err, qt.IsNil)
}

func dropMySQLUser(c *qt.C, ctx context.Context, db *sql.DB, username string) {
	c.Helper()
	_, err := db.ExecContext(ctx, fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", username))
	c.Check(err, qt.IsNil)
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

func mySQLForeignKeyCount(
	c *qt.C,
	ctx context.Context,
	db *sql.DB,
	database,
	constraint string,
) int {
	c.Helper()
	var count int
	err := db.QueryRowContext(
		ctx,
		`SELECT COUNT(*)
		FROM information_schema.referential_constraints
		WHERE constraint_schema = ?
		  AND constraint_name = ?`,
		database,
		constraint,
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
