package migrator_test

import (
	"context"
	"os"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestOutOfOrderMigrationsPostgresIntegration(t *testing.T) {
	dbURL := postgresTestURL(t)
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()
	cleanupIssue261(t, conn)
	defer cleanupIssue261(t, conn)

	applyIssue261Migrations(t, conn)

	mig := issue261Migrator(conn, issue261AllMigrations()...)
	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, 5)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int{1, 2, 5})
	c.Assert(status.PendingMigrations, qt.DeepEquals, []int{3})
	c.Assert(status.OutOfOrderMigrations, qt.DeepEquals, []int{3})

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNotNil)
	var outOfOrderErr *migrator.OutOfOrderError
	c.Assert(err, qt.ErrorAs, &outOfOrderErr)
	c.Assert(tableExists(t, conn, "ptah_issue_261_v3"), qt.IsFalse)

	err = mig.MigrateTo(ctx, 3)
	c.Assert(err, qt.ErrorMatches, `target version 3 is below current version 5 but is not applied`)
	c.Assert(tableExists(t, conn, "ptah_issue_261_v5"), qt.IsTrue)
	c.Assert(tableExists(t, conn, "ptah_issue_261_v3"), qt.IsFalse)

	err = mig.WithExecOrder(migrator.ExecOrderLinearSkip).MigrateUp(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(tableExists(t, conn, "ptah_issue_261_v3"), qt.IsFalse)

	err = mig.MigrateDownTo(ctx, 2)
	c.Assert(err, qt.IsNil)
	c.Assert(tableExists(t, conn, "ptah_issue_261_v5"), qt.IsFalse)
	c.Assert(tableExists(t, conn, "ptah_issue_261_v3"), qt.IsFalse)

	cleanupIssue261(t, conn)
	applyIssue261Migrations(t, conn)

	err = issue261Migrator(conn, issue261AllMigrations()...).
		WithExecOrder(migrator.ExecOrderNonLinear).
		MigrateUp(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(tableExists(t, conn, "ptah_issue_261_v3"), qt.IsTrue)

	finalStatus, err := issue261Migrator(conn, issue261AllMigrations()...).GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(finalStatus.AppliedMigrations, qt.DeepEquals, []int{1, 2, 3, 5})
	c.Assert(finalStatus.PendingMigrations, qt.HasLen, 0)
}

func postgresTestURL(t *testing.T) string {
	t.Helper()

	dbURL := os.Getenv("POSTGRES_TEST_DSN")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("POSTGRES_TEST_DSN or TEST_DATABASE_URL not set")
	}
	if !strings.HasPrefix(dbURL, "postgres://") && !strings.HasPrefix(dbURL, "postgresql://") {
		t.Skip("PostgreSQL URL required for out-of-order migration integration test")
	}
	return dbURL
}

func issue261Migrator(conn *dbschema.DatabaseConnection, migrations ...*migrator.Migration) *migrator.Migrator {
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migrations...)).
		WithMigrationsTable("", "schema_migrations_issue_261")
}

func applyIssue261Migrations(t *testing.T, conn *dbschema.DatabaseConnection) {
	t.Helper()

	err := issue261Migrator(conn, issue261AppliedMigrations()...).MigrateUp(context.Background())
	qt.Assert(t, err, qt.IsNil)
}

func issue261AppliedMigrations() []*migrator.Migration {
	return []*migrator.Migration{
		issue261Migration(1, "v1", "CREATE TABLE ptah_issue_261_v1 (id INTEGER PRIMARY KEY)", "DROP TABLE ptah_issue_261_v1"),
		issue261Migration(2, "v2", "CREATE TABLE ptah_issue_261_v2 (id INTEGER PRIMARY KEY)", "DROP TABLE ptah_issue_261_v2"),
		issue261Migration(5, "v5", "CREATE TABLE ptah_issue_261_v5 (id INTEGER PRIMARY KEY)", "DROP TABLE ptah_issue_261_v5"),
	}
}

func issue261AllMigrations() []*migrator.Migration {
	return []*migrator.Migration{
		issue261Migration(1, "v1", "CREATE TABLE ptah_issue_261_v1 (id INTEGER PRIMARY KEY)", "DROP TABLE ptah_issue_261_v1"),
		issue261Migration(2, "v2", "CREATE TABLE ptah_issue_261_v2 (id INTEGER PRIMARY KEY)", "DROP TABLE ptah_issue_261_v2"),
		issue261Migration(3, "v3", "CREATE TABLE ptah_issue_261_v3 (id INTEGER PRIMARY KEY)", "DROP TABLE ptah_issue_261_never_applied_must_not_exist"),
		issue261Migration(5, "v5", "CREATE TABLE ptah_issue_261_v5 (id INTEGER PRIMARY KEY)", "DROP TABLE ptah_issue_261_v5"),
	}
}

func issue261Migration(version int, description, upSQL, downSQL string) *migrator.Migration {
	return migrator.CreateMigrationFromSQL(version, description, upSQL, downSQL)
}

func cleanupIssue261(t *testing.T, conn *dbschema.DatabaseConnection) {
	t.Helper()

	for _, statement := range []string{
		"DROP TABLE IF EXISTS schema_migrations_issue_261",
		"DROP TABLE IF EXISTS ptah_issue_261_v5",
		"DROP TABLE IF EXISTS ptah_issue_261_v3",
		"DROP TABLE IF EXISTS ptah_issue_261_v2",
		"DROP TABLE IF EXISTS ptah_issue_261_v1",
	} {
		_, _ = conn.ExecContext(context.Background(), statement)
	}
}

func tableExists(t *testing.T, conn *dbschema.DatabaseConnection, tableName string) bool {
	t.Helper()

	var exists bool
	err := conn.QueryRowContext(
		context.Background(),
		"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)",
		tableName,
	).Scan(&exists)
	qt.Assert(t, err, qt.IsNil)
	return exists
}
