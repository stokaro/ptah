package migrator_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestDirtyMigrationState_PostgresFailureRollsBackAndBlocksRetry(t *testing.T) {
	dbURL := postgresTestURL(t)
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	names := issue265Names("postgres")
	cleanupIssue265(t, conn, names)
	defer cleanupIssue265(t, conn, names)

	mig := issue265Migrator(conn, names, issue265PostgresMigrations(names)...)
	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, "(?s).*failed to apply migration 2.*missing_table.*")

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Version, qt.Equals, int64(2))
	c.Assert(status.DirtyRevision.State, qt.Equals, "failed")
	c.Assert(status.DirtyRevision.Applied, qt.Equals, 0)
	c.Assert(status.DirtyRevision.Total, qt.Equals, 2)
	c.Assert(status.DirtyRevision.ErrorStatement, qt.Contains, "missing_table")
	c.Assert(tableExists(t, conn, names.partialTable), qt.IsFalse)

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNotNil)
	c.Assert(migrator.IsDirtyMigration(err), qt.IsTrue)
}

func TestDirtyMigrationState_MySQLRepairReachesHead(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runIssue265MySQLFamilyRepair(t, dbURL, "mysql")
}

func TestDirtyMigrationState_MariaDBRepairReachesHead(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runIssue265MySQLFamilyRepair(t, dbURL, "mariadb")
}

func TestMigrationChecksumMismatch_PostgresIntegration(t *testing.T) {
	dbURL := postgresTestURL(t)
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	names := issue265Names("checksum")
	cleanupIssue265(t, conn, names)
	defer cleanupIssue265(t, conn, names)

	err = issue265Migrator(conn, names, migrator.CreateMigrationFromSQL(
		1,
		"initial",
		fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.baseTable),
		fmt.Sprintf("DROP TABLE %s", names.baseTable),
	)).MigrateUp(ctx)
	c.Assert(err, qt.IsNil)

	changed := issue265Migrator(conn, names, migrator.CreateMigrationFromSQL(
		1,
		"initial",
		fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, changed TEXT)", names.baseTable),
		fmt.Sprintf("DROP TABLE %s", names.baseTable),
	))
	err = changed.MigrateUp(ctx)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "checksum mismatch")
}

type issue265TestNames struct {
	migrationsTable string
	baseTable       string
	partialTable    string
	headTable       string
}

func issue265Names(prefix string) issue265TestNames {
	suffix := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	return issue265TestNames{
		migrationsTable: "schema_migrations_issue_265_" + suffix,
		baseTable:       "ptah_issue_265_base_" + suffix,
		partialTable:    "ptah_issue_265_partial_" + suffix,
		headTable:       "ptah_issue_265_head_" + suffix,
	}
}

func issue265Migrator(
	conn *dbschema.DatabaseConnection,
	names issue265TestNames,
	migrations ...*migrator.Migration,
) *migrator.Migrator {
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migrations...)).
		WithMigrationsTable("", names.migrationsTable).
		WithMigrationLockTimeout(10 * time.Second)
}

func issue265PostgresMigrations(names issue265TestNames) []*migrator.Migration {
	return []*migrator.Migration{
		migrator.CreateMigrationFromSQL(
			1,
			"base",
			fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.baseTable),
			fmt.Sprintf("DROP TABLE %s", names.baseTable),
		),
		migrator.CreateMigrationFromSQL(
			2,
			"failing partial",
			fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY); SELECT * FROM missing_table", names.partialTable),
			fmt.Sprintf("DROP TABLE %s", names.partialTable),
		),
	}
}

func issue265MySQLFamilyMigrations(names issue265TestNames) []*migrator.Migration {
	return []*migrator.Migration{
		migrator.CreateMigrationFromSQL(
			1,
			"base",
			fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.baseTable),
			fmt.Sprintf("DROP TABLE %s", names.baseTable),
		),
		migrator.CreateMigrationFromSQL(
			2,
			"half applied",
			fmt.Sprintf("ALTER TABLE %s ADD COLUMN nickname VARCHAR(64); UPDATE missing_table SET value = 1", names.baseTable),
			fmt.Sprintf("ALTER TABLE %s DROP COLUMN nickname", names.baseTable),
		),
		migrator.CreateMigrationFromSQL(
			3,
			"head",
			fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.headTable),
			fmt.Sprintf("DROP TABLE %s", names.headTable),
		),
	}
}

func runIssue265MySQLFamilyRepair(t *testing.T, dbURL, dialect string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	names := issue265Names(dialect)
	cleanupIssue265(t, conn, names)
	defer cleanupIssue265(t, conn, names)

	migrations := issue265MySQLFamilyMigrations(names)
	mig := issue265Migrator(conn, names, migrations...)
	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, "(?s).*failed to apply migration 2.*missing_table.*")

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Version, qt.Equals, int64(2))
	c.Assert(status.DirtyRevision.Applied, qt.Equals, 1)
	c.Assert(status.DirtyRevision.Total, qt.Equals, 2)
	c.Assert(issue265ColumnExists(t, conn, names.baseTable, "nickname"), qt.IsTrue)

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNotNil)
	c.Assert(migrator.IsDirtyMigration(err), qt.IsTrue)

	err = mig.RepairMigration(ctx, migrator.RepairMigrationOptions{Version: 2})
	c.Assert(err, qt.IsNil)

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(issue265TableExists(t, conn, names.headTable), qt.IsTrue)
}

func issue265ColumnExists(t *testing.T, conn *dbschema.DatabaseConnection, table, column string) bool {
	t.Helper()

	var count int
	err := conn.QueryRowContext(
		context.Background(),
		"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?",
		table,
		column,
	).Scan(&count)
	qt.Assert(t, err, qt.IsNil)
	return count > 0
}

func issue265TableExists(t *testing.T, conn *dbschema.DatabaseConnection, tableName string) bool {
	t.Helper()

	var count int
	err := conn.QueryRowContext(
		context.Background(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
		tableName,
	).Scan(&count)
	qt.Assert(t, err, qt.IsNil)
	return count > 0
}

func cleanupIssue265(t *testing.T, conn *dbschema.DatabaseConnection, names issue265TestNames) {
	t.Helper()

	for _, statement := range []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s", names.migrationsTable),
		fmt.Sprintf("DROP TABLE IF EXISTS %s", names.headTable),
		fmt.Sprintf("DROP TABLE IF EXISTS %s", names.partialTable),
		fmt.Sprintf("DROP TABLE IF EXISTS %s", names.baseTable),
	} {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		_, _ = conn.ExecContext(context.Background(), statement)
	}
}
