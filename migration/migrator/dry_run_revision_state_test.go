package migrator_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

func openDryRunRevisionSQLite(t *testing.T) *dbschema.DatabaseConnection {
	t.Helper()
	conn, err := dbschema.ConnectToDatabase(
		context.Background(),
		"sqlite://"+filepath.Join(t.TempDir(), "revision-state.db"),
	)
	qt.Assert(t, err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func revisionStateMigrations() (first, second *migrator.Migration) {
	return migrator.CreateMigrationFromSQL(
			1,
			"create_users",
			"CREATE TABLE dry_run_users (id INTEGER PRIMARY KEY);",
			"DROP TABLE dry_run_users;",
		), migrator.CreateMigrationFromSQL(
			2,
			"create_posts",
			"CREATE TABLE dry_run_posts (id INTEGER PRIMARY KEY);",
			"DROP TABLE dry_run_posts;",
		)
}

func TestDryRunRevisionState_LegacyPtahTableIsReadWithoutUpgrade(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := openDryRunRevisionSQLite(t)
	_, err := conn.ExecContext(ctx, `CREATE TABLE schema_migrations (
		version BIGINT PRIMARY KEY,
		description TEXT NOT NULL,
		applied_at TIMESTAMP NOT NULL
	)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, `INSERT INTO schema_migrations (version, description, applied_at)
		VALUES (1, 'create_users', CURRENT_TIMESTAMP)`)
	c.Assert(err, qt.IsNil)
	conn.SchemaWriter().SetDryRun(true)
	one, two := revisionStateMigrations()
	dryRun := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(one, two))

	status, err := dryRun.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{1})
	c.Assert(status.PendingMigrations, qt.DeepEquals, []int64{2})
	c.Assert(dryRun.MigrateUp(ctx), qt.IsNil)

	var columnCount int
	err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('schema_migrations')").Scan(&columnCount)
	c.Assert(err, qt.IsNil)
	c.Assert(columnCount, qt.Equals, 3)
}

func TestDryRunRevisionState_PartialPtahTableFailsWithLayoutDiagnostic(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := openDryRunRevisionSQLite(t)
	_, err := conn.ExecContext(ctx, `CREATE TABLE schema_migrations (
		version BIGINT PRIMARY KEY,
		description TEXT NOT NULL,
		applied_at TIMESTAMP NOT NULL,
		state VARCHAR(32) NOT NULL DEFAULT 'applied'
	)`)
	c.Assert(err, qt.IsNil)
	conn.SchemaWriter().SetDryRun(true)
	one, _ := revisionStateMigrations()
	dryRun := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(one))

	status, err := dryRun.GetMigrationStatus(ctx)
	c.Assert(err, qt.ErrorMatches, `.*failed to initialize migrations table: failed to inspect migrations table layout: incomplete migrations metadata layout: missing columns applied, total, error, error_stmt, execution_time_ms, checksum`)
	c.Assert(status, qt.IsNil)

	var columnCount int
	err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_table_info('schema_migrations')").Scan(&columnCount)
	c.Assert(err, qt.IsNil)
	c.Assert(columnCount, qt.Equals, 4)
}

func TestDryRunRevisionState_SQLiteMetadataLookupMatchesIdentifierCase(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := openDryRunRevisionSQLite(t)
	one, _ := revisionStateMigrations()
	writer := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(one)).
		WithMigrationsTable("", "Schema_Migrations")
	c.Assert(writer.MigrateUp(ctx), qt.IsNil)
	conn.SchemaWriter().SetDryRun(true)
	reader := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(one)).
		WithMigrationsTable("", "schema_migrations")

	status, err := reader.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{1})
	c.Assert(status.PendingMigrations, qt.HasLen, 0)
}

func TestDryRunRevisionState_SQLiteAttachedMetadataSchema(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := openDryRunRevisionSQLite(t)
	auxPath := filepath.Join(t.TempDir(), "revision-state-aux.db")
	_, err := conn.ExecContext(ctx, "ATTACH DATABASE ? AS aux", auxPath)
	c.Assert(err, qt.IsNil)
	one, _ := revisionStateMigrations()
	writer := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(one)).
		WithMigrationsTable("aux", "Schema_Migrations")
	c.Assert(writer.MigrateUp(ctx), qt.IsNil)
	conn.SchemaWriter().SetDryRun(true)
	reader := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(one)).
		WithMigrationsTable("aux", "schema_migrations")

	status, err := reader.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{1})
	c.Assert(status.PendingMigrations, qt.HasLen, 0)
}

func TestDryRunRevisionState_FreshDatabaseRemainsUnmodified(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := openDryRunRevisionSQLite(t)
	conn.SchemaWriter().SetDryRun(true)
	one, _ := revisionStateMigrations()
	dryRun := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(one))

	status, err := dryRun.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(0))
	c.Assert(status.AppliedMigrations, qt.HasLen, 0)
	c.Assert(status.PendingMigrations, qt.DeepEquals, []int64{1})
	c.Assert(dryRun.MigrateUp(ctx), qt.IsNil)

	var tableCount int
	err = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'schema_migrations'`).Scan(&tableCount)
	c.Assert(err, qt.IsNil)
	c.Assert(tableCount, qt.Equals, 0)
}

func TestDryRunRevisionState_DirtyRevisionStillBlocksApply(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := openDryRunRevisionSQLite(t)
	one, two := revisionStateMigrations()
	writer := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(one))
	c.Assert(writer.MigrateUp(ctx), qt.IsNil)
	_, err := conn.ExecContext(ctx, "UPDATE schema_migrations SET state = 'failed' WHERE version = 1")
	c.Assert(err, qt.IsNil)
	conn.SchemaWriter().SetDryRun(true)
	dryRun := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(one, two))

	err = dryRun.MigrateUp(ctx)
	var dirty *migrator.DirtyMigrationError
	c.Assert(err, qt.ErrorAs, &dirty)
	c.Assert(dirty.Revision.Version, qt.Equals, int64(1))
}

func TestDryRunRevisionState_ChecksumMismatchStillBlocksApply(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := openDryRunRevisionSQLite(t)
	one, _ := revisionStateMigrations()
	writer := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(one))
	c.Assert(writer.MigrateUp(ctx), qt.IsNil)
	conn.SchemaWriter().SetDryRun(true)
	changed := migrator.CreateMigrationFromSQL(
		1,
		"create_users",
		"CREATE TABLE dry_run_users (id INTEGER PRIMARY KEY, name TEXT);",
		"DROP TABLE dry_run_users;",
	)
	dryRun := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(changed))

	err := dryRun.MigrateUp(ctx)
	var mismatch *migrator.ChecksumMismatchError
	c.Assert(err, qt.ErrorAs, &mismatch)
	c.Assert(mismatch.Version, qt.Equals, int64(1))
}
