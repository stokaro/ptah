package migrator_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

const issue124ConcurrentRunners = 10

func TestMigrationAdvisoryLock_PostgresConcurrentRunners(t *testing.T) {
	dbURL := postgresTestURL(t)
	c := qt.New(t)
	ctx := context.Background()

	baseConn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = baseConn.Close() }()

	names := issue124Names(time.Now().UnixNano())
	cleanupIssue124(t, baseConn, names)
	defer cleanupIssue124(t, baseConn, names)

	migrations := issue124Migrations(names)
	errs := runIssue124ConcurrentMigrations(t, dbURL, func(conn *dbschema.DatabaseConnection) error {
		return issue124Migrator(conn, names.migrationsTable, migrations).MigrateUp(ctx)
	})
	c.Assert(errs, qt.HasLen, 0)

	c.Assert(tableExists(t, baseConn, names.itemsTable), qt.IsTrue)
	c.Assert(tableExists(t, baseConn, names.logTable), qt.IsTrue)
	assertIssue124State(t, baseConn, names, 2, 1)

	errs = runIssue124ConcurrentMigrations(t, dbURL, func(conn *dbschema.DatabaseConnection) error {
		return issue124Migrator(conn, names.migrationsTable, migrations).MigrateDownTo(ctx, 0)
	})
	c.Assert(errs, qt.HasLen, 0)

	c.Assert(tableExists(t, baseConn, names.itemsTable), qt.IsFalse)
	c.Assert(tableExists(t, baseConn, names.logTable), qt.IsFalse)
	assertIssue124State(t, baseConn, names, 0, 0)
}

func TestMigrationAdvisoryLock_PostgresTimeoutIntegration(t *testing.T) {
	dbURL := postgresTestURL(t)
	c := qt.New(t)
	ctx := context.Background()

	baseConn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = baseConn.Close() }()

	names := issue124Names(time.Now().UnixNano())
	cleanupIssue124(t, baseConn, names)
	defer cleanupIssue124(t, baseConn, names)

	lockConn, err := baseConn.Conn(ctx)
	c.Assert(err, qt.IsNil)
	defer func() { _ = lockConn.Close() }()

	_, err = lockConn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", int64(-7752083082818440098))
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = lockConn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", int64(-7752083082818440098))
	}()

	err = issue124Migrator(baseConn, names.migrationsTable, issue124Migrations(names)).
		WithMigrationLockTimeout(100 * time.Millisecond).
		MigrateUp(ctx)

	c.Assert(err, qt.IsNotNil)
	c.Assert(migrator.IsMigrationLockTimeout(err), qt.IsTrue)
}

func TestMigrationAdvisoryLock_MySQLDefaultTimeoutIntegration(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runIssue124AdvisoryLockDefaultTimeoutIntegration(t, dbURL)
}

func TestMigrationAdvisoryLock_MariaDBDefaultTimeoutIntegration(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runIssue124AdvisoryLockDefaultTimeoutIntegration(t, dbURL)
}

func TestMigrationAdvisoryLock_SQLServerDefaultTimeoutIntegration(t *testing.T) {
	dbURL := sqlServerTestURL(t)
	runIssue124AdvisoryLockDefaultTimeoutIntegration(t, dbURL)
}

func TestMigrationAdvisoryLock_SQLServerTimeoutIntegration(t *testing.T) {
	dbURL := sqlServerTestURL(t)
	c := qt.New(t)
	ctx := context.Background()

	baseConn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = baseConn.Close() }()

	names := issue124Names(time.Now().UnixNano())
	cleanupIssue124(t, baseConn, names)
	defer cleanupIssue124(t, baseConn, names)

	lockConn, err := baseConn.Conn(ctx)
	c.Assert(err, qt.IsNil)
	defer func() { _ = lockConn.Close() }()

	c.Assert(acquireSQLServerTestMigrationLock(ctx, lockConn), qt.IsNil)
	defer func() {
		_ = releaseSQLServerTestMigrationLock(context.Background(), lockConn)
	}()

	err = issue124Migrator(baseConn, names.migrationsTable, issue124Migrations(names)).
		WithMigrationLockTimeout(100 * time.Millisecond).
		MigrateUp(ctx)

	c.Assert(err, qt.IsNotNil)
	c.Assert(migrator.IsMigrationLockTimeout(err), qt.IsTrue)
}

type issue124TestNames struct {
	migrationsTable string
	itemsTable      string
	logTable        string
}

func issue124Names(suffix int64) issue124TestNames {
	return issue124TestNames{
		migrationsTable: fmt.Sprintf("schema_migrations_issue_124_%d", suffix),
		itemsTable:      fmt.Sprintf("ptah_issue_124_items_%d", suffix),
		logTable:        fmt.Sprintf("ptah_issue_124_log_%d", suffix),
	}
}

func issue124Migrator(
	conn *dbschema.DatabaseConnection,
	migrationsTable string,
	migrations []*migrator.Migration,
) *migrator.Migrator {
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migrations...)).
		WithMigrationsTable("", migrationsTable).
		WithMigrationLockTimeout(10 * time.Second)
}

func issue124Migrations(names issue124TestNames) []*migrator.Migration {
	return []*migrator.Migration{
		migrator.CreateMigrationFromSQL(
			1,
			"create issue 124 items",
			fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.itemsTable),
			fmt.Sprintf("DROP TABLE %s", names.itemsTable),
		),
		migrator.CreateMigrationFromSQL(
			2,
			"create issue 124 log",
			fmt.Sprintf(
				"CREATE TABLE %s (id INTEGER PRIMARY KEY); INSERT INTO %s (id) VALUES (1)",
				names.logTable,
				names.logTable,
			),
			fmt.Sprintf("DROP TABLE %s", names.logTable),
		),
	}
}

func runIssue124ConcurrentMigrations(
	t *testing.T,
	dbURL string,
	run func(*dbschema.DatabaseConnection) error,
) []error {
	t.Helper()

	start := make(chan struct{})
	errCh := make(chan error, issue124ConcurrentRunners)
	var wg sync.WaitGroup

	for range issue124ConcurrentRunners {
		wg.Go(func() {
			conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
			if err != nil {
				errCh <- err
				return
			}
			defer func() { _ = conn.Close() }()

			<-start
			if err := run(conn); err != nil {
				errCh <- err
			}
		})
	}

	close(start)
	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	return errs
}

func assertIssue124State(
	t *testing.T,
	conn *dbschema.DatabaseConnection,
	names issue124TestNames,
	wantMigrations int,
	wantLogRows int,
) {
	t.Helper()

	var migrationRows int
	err := conn.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM %s", names.migrationsTable)).
		Scan(&migrationRows)
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, migrationRows, qt.Equals, wantMigrations)

	var logRows int
	if wantLogRows > 0 {
		err = conn.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM %s", names.logTable)).
			Scan(&logRows)
		qt.Assert(t, err, qt.IsNil)
	}
	qt.Assert(t, logRows, qt.Equals, wantLogRows)
}

func cleanupIssue124(t *testing.T, conn *dbschema.DatabaseConnection, names issue124TestNames) {
	t.Helper()

	for _, statement := range []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s", names.migrationsTable),
		fmt.Sprintf("DROP TABLE IF EXISTS %s", names.logTable),
		fmt.Sprintf("DROP TABLE IF EXISTS %s", names.itemsTable),
	} {
		_, _ = conn.ExecContext(context.Background(), statement)
	}
}

func runIssue124AdvisoryLockDefaultTimeoutIntegration(t *testing.T, dbURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	names := issue124Names(time.Now().UnixNano())
	cleanupIssue124(t, conn, names)
	defer cleanupIssue124(t, conn, names)

	err = issue124Migrator(conn, names.migrationsTable, issue124Migrations(names)).MigrateUp(ctx)
	c.Assert(err, qt.IsNil)

	var logRows int
	err = conn.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", names.logTable)).Scan(&logRows)
	c.Assert(err, qt.IsNil)
	c.Assert(logRows, qt.Equals, 1)
}

func mySQLFamilyTestURL(t *testing.T, dialect string, envNames ...string) string {
	t.Helper()

	for _, envName := range envNames {
		dbURL := os.Getenv(envName)
		if dbURL == "" {
			continue
		}
		if !strings.HasPrefix(dbURL, dialect+"://") {
			t.Skipf("%s URL required for %s advisory lock integration test", dialect, dialect)
		}
		return dbURL
	}

	t.Skipf("%s not set", strings.Join(envNames, " or "))
	return ""
}

func sqlServerTestURL(t *testing.T) string {
	t.Helper()

	dbURL := os.Getenv("PTAH_SQLSERVER_TEST_URL")
	if dbURL == "" {
		t.Skip("PTAH_SQLSERVER_TEST_URL not set")
	}
	if !strings.HasPrefix(dbURL, "sqlserver://") && !strings.HasPrefix(dbURL, "mssql://") {
		t.Skip("sqlserver URL required for SQL Server advisory lock integration test")
	}
	return dbURL
}

func acquireSQLServerTestMigrationLock(ctx context.Context, conn interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) error {
	var result int
	if err := conn.QueryRowContext(ctx, `
DECLARE @result INT;
EXEC @result = sys.sp_getapplock
    @Resource = @p1,
    @LockMode = 'Exclusive',
    @LockOwner = 'Session',
    @LockTimeout = 0;
SELECT @result;`, "ptah_migrate").Scan(&result); err != nil {
		return err
	}
	if result < 0 {
		return fmt.Errorf("sqlserver test sp_getapplock failed with return code %d", result)
	}
	return nil
}

func releaseSQLServerTestMigrationLock(ctx context.Context, conn interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) error {
	var result int
	if err := conn.QueryRowContext(ctx, `
DECLARE @result INT;
EXEC @result = sys.sp_releaseapplock
    @Resource = @p1,
    @LockOwner = 'Session';
SELECT @result;`, "ptah_migrate").Scan(&result); err != nil {
		return err
	}
	if result < 0 {
		return fmt.Errorf("sqlserver test sp_releaseapplock failed with return code %d", result)
	}
	return nil
}
