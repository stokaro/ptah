package migrator_test

import (
	"context"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestCreateMigrationFromSQL_PostgresFailureRollsBackStatements(t *testing.T) {
	dbURL := postgresTestURL(t)
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()
	cleanupIssue262(t, conn)
	defer cleanupIssue262(t, conn)

	_, err = conn.ExecContext(ctx, "CREATE TABLE ptah_issue_262_users (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)

	badMigration := migrator.CreateMigrationFromSQL(1, "add status",
		`ALTER TABLE ptah_issue_262_users ADD COLUMN status TEXT;
		 ALTER TABLE ptah_issue_262_users ADD CONSTRAINT ptah_issue_262_status_ck CHECK (missing_column > 0);`,
		`ALTER TABLE ptah_issue_262_users DROP COLUMN status;`)
	mig := issue262Migrator(conn, badMigration)

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNotNil)
	c.Assert(columnExists(t, conn, "ptah_issue_262_users", "status"), qt.IsFalse)
	c.Assert(issue262MigrationRecordCount(t, conn), qt.Equals, 0)

	fixedMigration := migrator.CreateMigrationFromSQL(1, "add status",
		`ALTER TABLE ptah_issue_262_users ADD COLUMN status TEXT;
		 ALTER TABLE ptah_issue_262_users ADD CONSTRAINT ptah_issue_262_status_ck CHECK (status IS NULL OR status <> '');`,
		`ALTER TABLE ptah_issue_262_users DROP COLUMN status;`)
	err = issue262Migrator(conn, fixedMigration).MigrateUp(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(columnExists(t, conn, "ptah_issue_262_users", "status"), qt.IsTrue)
	c.Assert(issue262MigrationRecordCount(t, conn), qt.Equals, 1)
}

func TestMigratorDryRun_PostgresDoesNotCreateMetadataOrMigrationObjects(t *testing.T) {
	dbURL := postgresTestURL(t)
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()
	cleanupIssue262(t, conn)
	defer cleanupIssue262(t, conn)

	conn.Writer().SetDryRun(true)
	migration := migrator.CreateMigrationFromSQL(1, "create dry run table",
		`CREATE TABLE ptah_issue_262_dry_run (id INTEGER PRIMARY KEY);`,
		`DROP TABLE ptah_issue_262_dry_run;`)
	mig := issue262Migrator(conn, migration).WithMigrationsTable("ptah_issue_262", "schema_migrations")

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, 0)
	c.Assert(status.PendingMigrations, qt.DeepEquals, []int{1})

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(schemaExists(t, conn, "ptah_issue_262"), qt.IsFalse)
	c.Assert(tableExists(t, conn, "ptah_issue_262_dry_run"), qt.IsFalse)

	err = mig.MigrateDownTo(ctx, 0)
	c.Assert(err, qt.IsNil)
	c.Assert(schemaExists(t, conn, "ptah_issue_262"), qt.IsFalse)
	c.Assert(tableExists(t, conn, "ptah_issue_262_dry_run"), qt.IsFalse)
}

func TestNoTransactionDirective_PostgresEnumValueCanBeUsedInSameMigration(t *testing.T) {
	dbURL := postgresTestURL(t)
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()
	cleanupIssue262(t, conn)
	defer cleanupIssue262(t, conn)

	fsys := fstest.MapFS{
		"0000000001_initial_enum.up.sql": &fstest.MapFile{
			Data: []byte(`CREATE TYPE ptah_issue_262_mood AS ENUM ('sad');
CREATE TABLE ptah_issue_262_enum_items (id INTEGER PRIMARY KEY);`),
		},
		"0000000001_initial_enum.down.sql": &fstest.MapFile{
			Data: []byte(`DROP TABLE ptah_issue_262_enum_items;
DROP TYPE ptah_issue_262_mood;`),
		},
		"0000000002_add_enum_value.up.sql": &fstest.MapFile{
			Data: []byte(`-- +ptah no_transaction
ALTER TYPE ptah_issue_262_mood ADD VALUE 'ok';
ALTER TABLE ptah_issue_262_enum_items ADD COLUMN mood ptah_issue_262_mood NOT NULL DEFAULT 'ok';`),
		},
		"0000000002_add_enum_value.down.sql": &fstest.MapFile{
			Data: []byte(`-- +ptah no_transaction
ALTER TABLE ptah_issue_262_enum_items DROP COLUMN mood;`),
		},
	}

	mig, err := migrator.NewFSMigrator(conn, fsys)
	c.Assert(err, qt.IsNil)
	mig = mig.WithMigrationsTable("", "schema_migrations_issue_262")

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(columnExists(t, conn, "ptah_issue_262_enum_items", "mood"), qt.IsTrue)
	c.Assert(issue262MigrationRecordCount(t, conn), qt.Equals, 2)
}

func issue262Migrator(conn *dbschema.DatabaseConnection, migrations ...*migrator.Migration) *migrator.Migrator {
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migrations...)).
		WithMigrationsTable("", "schema_migrations_issue_262")
}

func cleanupIssue262(t *testing.T, conn *dbschema.DatabaseConnection) {
	t.Helper()

	for _, statement := range []string{
		"DROP SCHEMA IF EXISTS ptah_issue_262 CASCADE",
		"DROP TABLE IF EXISTS schema_migrations_issue_262",
		"DROP TABLE IF EXISTS ptah_issue_262_dry_run",
		"DROP TABLE IF EXISTS ptah_issue_262_enum_items",
		"DROP TABLE IF EXISTS ptah_issue_262_users",
		"DROP TYPE IF EXISTS ptah_issue_262_mood",
	} {
		_, _ = conn.ExecContext(context.Background(), statement)
	}
}

func columnExists(t *testing.T, conn *dbschema.DatabaseConnection, tableName, columnName string) bool {
	t.Helper()

	var exists bool
	err := conn.QueryRowContext(
		context.Background(),
		`SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = $1
			  AND column_name = $2
		)`,
		tableName,
		columnName,
	).Scan(&exists)
	qt.Assert(t, err, qt.IsNil)
	return exists
}

func issue262MigrationRecordCount(t *testing.T, conn *dbschema.DatabaseConnection) int {
	t.Helper()

	if !tableExists(t, conn, "schema_migrations_issue_262") {
		return 0
	}
	var count int
	err := conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM schema_migrations_issue_262").Scan(&count)
	qt.Assert(t, err, qt.IsNil)
	return count
}

func schemaExists(t *testing.T, conn *dbschema.DatabaseConnection, schemaName string) bool {
	t.Helper()

	var exists bool
	err := conn.QueryRowContext(
		context.Background(),
		"SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
		schemaName,
	).Scan(&exists)
	qt.Assert(t, err, qt.IsNil)
	return exists
}
