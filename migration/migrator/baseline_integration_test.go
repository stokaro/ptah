package migrator_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestBaseline_PostgresRecordsAppliedWithoutExecutingSQL(t *testing.T) {
	dbURL := postgresTestURL(t)
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	names := issue269Names(time.Now().UnixNano())
	cleanupIssue269(t, conn, names)
	defer cleanupIssue269(t, conn, names)

	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.itemsTable))
	c.Assert(err, qt.IsNil)

	err = issue269Migrator(conn, names).
		BaselineWithOptions(ctx, migrator.BaselineOptions{Version: 2})
	c.Assert(err, qt.IsNil)

	c.Assert(tableExists(t, conn, names.itemsTable), qt.IsTrue)
	c.Assert(tableExists(t, conn, names.logTable), qt.IsFalse)
	assertIssue269Revisions(t, conn, names, []issue269Revision{
		{Version: 1, State: "applied", Applied: 1, Total: 1},
		{Version: 2, State: "applied", Applied: 1, Total: 1},
	})

	status, err := issue269Migrator(conn, names).GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{1, 2})
	c.Assert(status.PendingMigrations, qt.DeepEquals, []int64{3})
}

func TestBaseline_PostgresSecondRunRequiresForce(t *testing.T) {
	dbURL := postgresTestURL(t)
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	names := issue269Names(time.Now().UnixNano())
	cleanupIssue269(t, conn, names)
	defer cleanupIssue269(t, conn, names)

	err = issue269Migrator(conn, names).
		BaselineWithOptions(ctx, migrator.BaselineOptions{Version: 1})
	c.Assert(err, qt.IsNil)

	err = issue269Migrator(conn, names).
		BaselineWithOptions(ctx, migrator.BaselineOptions{Version: 2})
	c.Assert(err, qt.ErrorMatches, "schema migrations table is not empty.*")

	err = issue269Migrator(conn, names).
		BaselineWithOptions(ctx, migrator.BaselineOptions{Version: 2, Force: true})
	c.Assert(err, qt.IsNil)
	assertIssue269Revisions(t, conn, names, []issue269Revision{
		{Version: 1, State: "applied", Applied: 1, Total: 1},
		{Version: 2, State: "applied", Applied: 1, Total: 1},
	})
}

func TestBaseline_PostgresForceRejectsHistoryAboveBaseline(t *testing.T) {
	dbURL := postgresTestURL(t)
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	names := issue269Names(time.Now().UnixNano())
	cleanupIssue269(t, conn, names)
	defer cleanupIssue269(t, conn, names)

	err = issue269Migrator(conn, names).
		BaselineWithOptions(ctx, migrator.BaselineOptions{Version: 3})
	c.Assert(err, qt.IsNil)

	err = issue269Migrator(conn, names).
		BaselineWithOptions(ctx, migrator.BaselineOptions{Version: 2, Force: true})
	c.Assert(err, qt.ErrorMatches, "schema migrations table contains revisions above baseline version 2.*")
	assertIssue269Revisions(t, conn, names, []issue269Revision{
		{Version: 1, State: "applied", Applied: 1, Total: 1},
		{Version: 2, State: "applied", Applied: 1, Total: 1},
		{Version: 3, State: "applied", Applied: 1, Total: 1},
	})
}

func TestBaseline_MySQLRecordsAppliedWithoutExecutingSQL(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runIssue269MySQLFamilyBaselineIntegration(t, dbURL)
}

func TestBaseline_MariaDBRecordsAppliedWithoutExecutingSQL(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runIssue269MySQLFamilyBaselineIntegration(t, dbURL)
}

type issue269TestNames struct {
	migrationsTable string
	itemsTable      string
	logTable        string
}

type issue269Revision struct {
	Version int64
	State   string
	Applied int
	Total   int
}

func issue269Names(suffix int64) issue269TestNames {
	return issue269TestNames{
		migrationsTable: fmt.Sprintf("schema_migrations_issue_269_%d", suffix),
		itemsTable:      fmt.Sprintf("ptah_issue_269_items_%d", suffix),
		logTable:        fmt.Sprintf("ptah_issue_269_log_%d", suffix),
	}
}

func issue269Migrator(conn *dbschema.DatabaseConnection, names issue269TestNames) *migrator.Migrator {
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(issue269Migrations(names)...)).
		WithMigrationsTable("", names.migrationsTable)
}

func issue269Migrations(names issue269TestNames) []*migrator.Migration {
	return []*migrator.Migration{
		migrator.CreateMigrationFromSQL(
			1,
			"create issue 269 items",
			fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.itemsTable),
			fmt.Sprintf("DROP TABLE %s", names.itemsTable),
		),
		migrator.CreateMigrationFromSQL(
			2,
			"create issue 269 log",
			fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.logTable),
			fmt.Sprintf("DROP TABLE %s", names.logTable),
		),
		migrator.CreateMigrationFromSQL(
			3,
			"add issue 269 marker",
			fmt.Sprintf("ALTER TABLE %s ADD COLUMN marker INTEGER", names.itemsTable),
			fmt.Sprintf("ALTER TABLE %s DROP COLUMN marker", names.itemsTable),
		),
	}
}

func runIssue269MySQLFamilyBaselineIntegration(t *testing.T, dbURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	names := issue269Names(time.Now().UnixNano())
	cleanupIssue269(t, conn, names)
	defer cleanupIssue269(t, conn, names)

	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.itemsTable))
	c.Assert(err, qt.IsNil)

	err = issue269Migrator(conn, names).
		BaselineWithOptions(ctx, migrator.BaselineOptions{Version: 2})
	c.Assert(err, qt.IsNil)

	c.Assert(mysqlFamilyTableExists(t, conn, names.itemsTable), qt.IsTrue)
	c.Assert(mysqlFamilyTableExists(t, conn, names.logTable), qt.IsFalse)
	assertIssue269Revisions(t, conn, names, []issue269Revision{
		{Version: 1, State: "applied", Applied: 1, Total: 1},
		{Version: 2, State: "applied", Applied: 1, Total: 1},
	})
}

func assertIssue269Revisions(
	t *testing.T,
	conn *dbschema.DatabaseConnection,
	names issue269TestNames,
	want []issue269Revision,
) {
	t.Helper()

	var count int
	err := conn.QueryRowContext(
		context.Background(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", names.migrationsTable),
	).Scan(&count)
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, count, qt.Equals, len(want))

	for _, wantRevision := range want {
		var got issue269Revision
		err = conn.QueryRowContext(
			context.Background(),
			sqlutil.Rebind(
				conn.Info().Dialect,
				fmt.Sprintf("SELECT version, state, applied, total FROM %s WHERE version = ?", names.migrationsTable),
			),
			wantRevision.Version,
		).Scan(&got.Version, &got.State, &got.Applied, &got.Total)
		qt.Assert(t, err, qt.IsNil)
		qt.Assert(t, got, qt.DeepEquals, wantRevision)
	}
}

func cleanupIssue269(t *testing.T, conn *dbschema.DatabaseConnection, names issue269TestNames) {
	t.Helper()

	for _, statement := range []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s", names.migrationsTable),
		fmt.Sprintf("DROP TABLE IF EXISTS %s", names.logTable),
		fmt.Sprintf("DROP TABLE IF EXISTS %s", names.itemsTable),
	} {
		_, _ = conn.ExecContext(context.Background(), statement)
	}
}

func mysqlFamilyTableExists(t *testing.T, conn *dbschema.DatabaseConnection, tableName string) bool {
	t.Helper()

	var exists int
	err := conn.QueryRowContext(
		context.Background(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
		tableName,
	).Scan(&exists)
	qt.Assert(t, err, qt.IsNil)
	return exists > 0
}
