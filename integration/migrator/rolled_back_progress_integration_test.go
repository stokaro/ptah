//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/migration/migrator"
)

type issue887StatementInterceptor struct {
	called bool
}

func (*issue887StatementInterceptor) ValidateDirectives(map[string]string) error {
	return nil
}

func (i *issue887StatementInterceptor) ExecuteStatement(
	context.Context,
	*dbschema.DatabaseConnection,
	string,
	map[string]string,
) (bool, error) {
	i.called = true
	return true, nil
}

// TestRolledBackProgress_MySQLDataOnlyBodyResumesFromTheTop is the end-to-end
// guard for stokaro/ptah#887 on a server whose DDL commits itself.
//
// A tx-mode file body of plain DML that fails part way is rolled back whole,
// but the failure used to be recorded as `applied = 1`. The revision then
// claimed a committed prefix that no longer existed, the retry resumed at
// statement two, and the first statement was never applied by any run while the
// migration reported itself complete. The assertion that matters is the row
// count after the retry, not the counter: a resume that skips a statement is
// indistinguishable from a correct one until the data is counted.
func TestRolledBackProgress_MySQLDataOnlyBodyResumesFromTheTop(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRolledBackDataOnlyBody(t, dbURL, "mysql")
}

func TestRolledBackProgress_MariaDBDataOnlyBodyResumesFromTheTop(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRolledBackDataOnlyBody(t, dbURL, "mariadb")
}

// TestRolledBackProgress_MySQLFailingDDLKeepsUnknownOutcome verifies that a
// witness committed before a failing DDL statement is not downgraded to an
// ordinary failure. The prefix is durable, but the statement outcome remains
// unknown and automatic retry must stop before it can repeat user SQL.
func TestRolledBackProgress_MySQLFailingDDLKeepsUnknownOutcome(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRolledBackCommittedDDLPrefix(t, dbURL, "mysql")
}

func TestRolledBackProgress_MariaDBFailingDDLKeepsUnknownOutcome(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRolledBackCommittedDDLPrefix(t, dbURL, "mariadb")
}

func TestRolledBackProgress_MySQLDDLThenDMLKeepsTheWholeDurablePrefix(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRolledBackDDLThenDML(t, dbURL, "mysql", migrator.RevisionTableFormatAtlas)
}

func TestRolledBackProgress_MariaDBDDLThenDMLKeepsTheWholeDurablePrefix(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRolledBackDDLThenDML(t, dbURL, "mariadb", migrator.RevisionTableFormatAtlas)
}

func TestRolledBackProgress_MySQLNativeRevisionKeepsTheWholeDurablePrefix(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRolledBackDDLThenDML(t, dbURL, "mysql_native", migrator.RevisionTableFormatPtah)
}

func TestRolledBackProgress_MariaDBNativeRevisionKeepsTheWholeDurablePrefix(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRolledBackDDLThenDML(t, dbURL, "mariadb_native", migrator.RevisionTableFormatPtah)
}

func TestRolledBackProgress_MySQLRejectsTransactionControl(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRejectsTransactionControl(t, dbURL, "mysql")
}

func TestRolledBackProgress_MariaDBRejectsTransactionControl(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRejectsTransactionControl(t, dbURL, "mariadb")
}

func TestRolledBackProgress_MySQLRejectsChangingTargetDatabase(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRejectsChangingTargetDatabase(t, dbURL, "mysql")
}

func TestRolledBackProgress_MariaDBRejectsChangingTargetDatabase(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRejectsChangingTargetDatabase(t, dbURL, "mariadb")
}

func TestRolledBackProgress_MySQLReplaysCommittedSessionState(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRolledBackSessionStateReplay(t, dbURL, "mysql")
}

func TestRolledBackProgress_MariaDBReplaysCommittedSessionState(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRolledBackSessionStateReplay(t, dbURL, "mariadb")
}

func TestRolledBackProgress_MySQLAtlasDownRecordsTheDurablePrefix(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRolledBackDownProgress(t, dbURL, "mysql_down_atlas", migrator.RevisionTableFormatAtlas)
}

func TestRolledBackProgress_MariaDBAtlasDownRecordsTheDurablePrefix(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRolledBackDownProgress(t, dbURL, "mariadb_down_atlas", migrator.RevisionTableFormatAtlas)
}

func TestRolledBackProgress_MySQLNativeDownRecordsTheDurablePrefix(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRolledBackDownProgress(t, dbURL, "mysql_down_native", migrator.RevisionTableFormatPtah)
}

func TestRolledBackProgress_MariaDBNativeDownRecordsTheDurablePrefix(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRolledBackDownProgress(t, dbURL, "mariadb_down_native", migrator.RevisionTableFormatPtah)
}

func TestRolledBackProgress_MySQLRejectsNonTransactionalMetadata(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRejectsNonTransactionalMetadata(t, dbURL, "mysql_myisam")
}

func TestRolledBackProgress_MariaDBRejectsNonTransactionalMetadata(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRejectsNonTransactionalMetadata(t, dbURL, "mariadb_myisam")
}

func TestRolledBackProgress_MySQLRejectsNonTransactionalNativeMetadataBeforeUpgrade(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRejectsNonTransactionalNativeMetadataBeforeUpgrade(t, dbURL, "mysql_native_myisam")
}

func TestRolledBackProgress_MariaDBRejectsNonTransactionalNativeMetadataBeforeUpgrade(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRejectsNonTransactionalNativeMetadataBeforeUpgrade(t, dbURL, "mariadb_native_myisam")
}

func TestRolledBackProgress_MySQLRejectsNonTransactionalTargetTable(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRejectsNonTransactionalTargetTable(t, dbURL, "mysql_target_myisam")
}

func TestRolledBackProgress_MariaDBRejectsNonTransactionalTargetTable(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRejectsNonTransactionalTargetTable(t, dbURL, "mariadb_target_myisam")
}

func TestRolledBackProgress_MySQLRejectsCreatingNonTransactionalTargetTable(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRejectsCreatingNonTransactionalTargetTable(t, dbURL, "mysql_create_myisam")
}

func TestRolledBackProgress_MariaDBRejectsCreatingNonTransactionalTargetTable(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRejectsCreatingNonTransactionalTargetTable(t, dbURL, "mariadb_create_myisam")
}

func TestRolledBackProgress_MariaDBRejectsAlterStorageEngineMyISAM(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRejectsMariaDBAlterStorageEngine(
		t,
		dbURL,
		"mariadb_stg_myisam",
		"MyISAM",
		`.*migration 1 statement 1 selects non-transactional storage engine MyISAM; tx-mode file requires InnoDB on MySQL-family databases`,
	)
}

func TestRolledBackProgress_MariaDBRejectsAlterStorageEngineDefault(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRejectsMariaDBAlterStorageEngine(
		t,
		dbURL,
		"mariadb_stg_default",
		"DEFAULT",
		`.*migration 1 statement 1 selects storage engine DEFAULT, whose effective engine can differ from the verified session default; select InnoDB explicitly or use tx-mode none`,
	)
}

func TestRolledBackProgress_MySQLRejectsDefaultStorageEngineReset(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRejectsDefaultStorageEngineReset(t, dbURL, "mysql_default_engine")
}

func TestRolledBackProgress_MariaDBRejectsDefaultStorageEngineReset(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRejectsDefaultStorageEngineReset(t, dbURL, "mariadb_default_engine")
}

func TestRolledBackProgress_MySQLRejectsUnsafeSQLModeChange(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRejectsUnsafeSQLModeChange(t, dbURL, "mysql_unsafe_sql_mode")
}

func TestRolledBackProgress_MariaDBRejectsUnsafeSQLModeChange(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRejectsUnsafeSQLModeChange(t, dbURL, "mariadb_unsafe_sql_mode")
}

func TestRolledBackProgress_MySQLRejectsUnsafeInitialSQLMode(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRejectsUnsafeInitialSQLMode(t, dbURL, "mysql_initial_sql_mode")
}

func TestRolledBackProgress_MariaDBRejectsUnsafeInitialSQLMode(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRejectsUnsafeInitialSQLMode(t, dbURL, "mariadb_initial_sql_mode")
}

func TestRolledBackProgress_MySQLRejectsANSIQuotesSQLModeChange(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRejectsANSIQuotesSQLModeChange(t, dbURL, "mysql_ansi_myisam")
}

func TestRolledBackProgress_MariaDBRejectsANSIQuotesSQLModeChange(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRejectsANSIQuotesSQLModeChange(t, dbURL, "mariadb_ansi_myisam")
}

func TestRolledBackProgress_MariaDBRejectsMSSQLSQLModeChange(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRejectsMSSQLSQLModeChange(t, dbURL, "mariadb_mssql_myisam")
}

func TestRolledBackProgress_MySQLRejectsANSIQuotesInitialSQLMode(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRejectsInitialSQLMode(t, dbURL, "mysql_initial_ansi", "%27ANSI_QUOTES%27", "ANSI_QUOTES")
}

func TestRolledBackProgress_MariaDBRejectsANSIQuotesInitialSQLMode(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRejectsInitialSQLMode(t, dbURL, "mariadb_initial_ansi", "%27ANSI_QUOTES%27", "ANSI_QUOTES")
}

func TestRolledBackProgress_MariaDBRejectsMSSQLInitialSQLMode(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRejectsInitialSQLMode(t, dbURL, "mariadb_initial_mssql", "%27MSSQL%27", "MSSQL")
}

func TestRolledBackProgress_MySQLIsolatesTemporaryTablesBetweenAttempts(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runIsolatesTemporaryTablesBetweenAttempts(t, dbURL, "mysql_tmp")
}

func TestRolledBackProgress_MariaDBIsolatesTemporaryTablesBetweenAttempts(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runIsolatesTemporaryTablesBetweenAttempts(t, dbURL, "mariadb_tmp")
}

func TestRolledBackProgress_MySQLRejectsTemporaryMetadataShadow(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRejectsTemporaryMetadataShadow(t, dbURL, "mysql_shadow")
}

func TestRolledBackProgress_MariaDBRejectsTemporaryMetadataShadow(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRejectsTemporaryMetadataShadow(t, dbURL, "mariadb_shadow")
}

func TestRolledBackProgress_MySQLRejectsStaleTemporaryMetadataShadowBeforeDirtyCheck(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRejectsStaleTemporaryMetadataShadowBeforeDirtyCheck(t, dbURL, "mysql_dirty_shadow")
}

func TestRolledBackProgress_MariaDBRejectsStaleTemporaryMetadataShadowBeforeDirtyCheck(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRejectsStaleTemporaryMetadataShadowBeforeDirtyCheck(t, dbURL, "mariadb_dirty_shadow")
}

func TestRolledBackProgress_MySQLRejectsStaleTemporaryMetadataShadowBeforePlanning(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRejectsStaleTemporaryMetadataShadowBeforePlanning(t, dbURL, "mysql_stale_shadow")
}

func TestRolledBackProgress_MariaDBRejectsStaleTemporaryMetadataShadowBeforePlanning(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRejectsStaleTemporaryMetadataShadowBeforePlanning(t, dbURL, "mariadb_stale_shadow")
}

func TestRolledBackProgress_MySQLRejectsInheritedStorageEngine(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	runRejectsInheritedStorageEngine(t, dbURL, "mysql_create_like")
}

func TestRolledBackProgress_MariaDBRejectsInheritedStorageEngine(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB)
	runRejectsInheritedStorageEngine(t, dbURL, "mariadb_create_like")
}

func TestRolledBackProgress_MySQLRejectsUnwitnessedExecutionBoundaries(t *testing.T) {
	targetURL := mySQLFamilyScratchDatabaseURL(t, "mysql", dbtarget.MySQLAdmin, "ptah_887_scope")
	runRejectsUnwitnessedExecutionBoundaries(t, targetURL, targetURL, "mysql")
}

func TestRolledBackProgress_MariaDBRejectsUnwitnessedExecutionBoundaries(t *testing.T) {
	targetURL := mySQLFamilyScratchDatabaseURL(t, "mariadb", dbtarget.MariaDBAdmin, "ptah_887_scope")
	runRejectsUnwitnessedExecutionBoundaries(t, targetURL, targetURL, "mariadb")
}

func TestRolledBackProgress_MySQLWithoutTriggerPrivilegeFailsClosed(t *testing.T) {
	targetURL := mySQLFamilyScratchDatabaseURL(t, "mysql", dbtarget.MySQLAdmin, "ptah_887_privilege")
	runMySQLWithoutTriggerPrivilegeFailsClosed(t, targetURL)
}

func TestRolledBackProgress_MariaDBWithoutTriggerPrivilegeStillRejectsTriggeredRelation(t *testing.T) {
	targetURL := mySQLFamilyScratchDatabaseURL(t, "mariadb", dbtarget.MariaDBAdmin, "ptah_887_privilege")
	runMariaDBWithoutTriggerPrivilegeStillRejectsTriggeredRelation(t, targetURL)
}

func TestRolledBackProgress_MySQLDefaultRoleTriggerPrivilegeIsAccepted(t *testing.T) {
	targetURL := mySQLFamilyScratchDatabaseURL(t, "mysql", dbtarget.MySQLAdmin, "ptah_887_role")
	runMySQLDefaultRoleTriggerPrivilegeIsAccepted(t, targetURL)
}

func TestRolledBackProgress_MySQLRejectsFilesystemWritesBeforeSideEffect(t *testing.T) {
	targetURL := mySQLFamilyScratchDatabaseURL(t, "mysql", dbtarget.MySQLAdmin, "ptah_887_files")
	runMySQLRejectsFilesystemWritesBeforeSideEffect(t, targetURL)
}

func TestRolledBackProgress_MySQLEscapedDatabaseGrantIsAccepted(t *testing.T) {
	adminURL := mySQLFamilyTestURL(t, "mysql", dbtarget.MySQLAdmin)
	runMySQLEscapedDatabaseGrantIsAccepted(t, adminURL)
}

// dbURL and adminURL must select the same target database. adminURL names the
// privileged connection used to provision neighboring realms and database
// objects; it is never the server's mysql system-database URL.
func runRejectsUnwitnessedExecutionBoundaries(t *testing.T, dbURL, adminURL, dialect string) {
	t.Helper()

	t.Run("executable comment", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)

		names := issue887Names(dialect + "_exec_comment")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		_, err = conn.ExecContext(ctx, fmt.Sprintf(
			"CREATE TABLE %s (id INTEGER PRIMARY KEY, note VARCHAR(64))", names.ledgerTable,
		))
		c.Assert(err, qt.IsNil)
		migration := migrator.CreateMigrationFromSQL(
			1,
			"executable comment",
			fmt.Sprintf("/*! SET autocommit = 0 */; INSERT INTO %s VALUES (1, 'one')", names.ledgerTable),
			fmt.Sprintf("DELETE FROM %s", names.ledgerTable),
		)

		err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*MySQL-family executable comments can bypass transaction-witness validation.*`)
		c.Assert(issue887LedgerCount(t, conn, names), qt.Equals, int64(0))
		c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
	})

	t.Run("opaque up function", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)

		names := issue887Names(dialect + "_opaque_up")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		called := false
		migration := &migrator.Migration{
			Version:     1,
			Description: "opaque up",
			Up: func(context.Context, *dbschema.DatabaseConnection) error {
				called = true
				return nil
			},
			Down: migrator.NoopMigrationFunc,
		}

		err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*cannot run an opaque up function in tx-mode file.*`)
		c.Assert(called, qt.IsFalse)
		c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
	})

	t.Run("opaque down function", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)

		names := issue887Names(dialect + "_opaque_down")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		upSQL := fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.createdTable)
		initial := migrator.CreateMigrationFromSQL(
			1,
			"opaque down",
			upSQL,
			fmt.Sprintf("DROP TABLE %s", names.createdTable),
		)
		c.Assert(issue887Migrator(conn, names, initial).MigrateUp(ctx), qt.IsNil)
		called := false
		opaque := &migrator.Migration{
			Version:     1,
			Description: "opaque down",
			UpSQL:       upSQL,
			Up:          migrator.NoopMigrationFunc,
			Down: func(context.Context, *dbschema.DatabaseConnection) error {
				called = true
				return nil
			},
		}

		err = issue887Migrator(conn, names, opaque).MigrateDown(ctx)
		c.Assert(err, qt.ErrorMatches, `.*cannot run an opaque down function in tx-mode file.*`)
		c.Assert(called, qt.IsFalse)
		c.Assert(issue887TableCount(t, conn, names.createdTable), qt.Equals, int64(1))
		c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(1))
	})

	t.Run("statement interceptor", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)

		names := issue887Names(dialect + "_interceptor")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		interceptor := &issue887StatementInterceptor{}
		migration, err := migrator.NewMigrationFromSQLFilesWithInterceptor(
			1,
			"interceptor",
			"migration.up.sql",
			"migration.down.sql",
			fstest.MapFS{
				"migration.up.sql":   &fstest.MapFile{Data: []byte("SELECT 1")},
				"migration.down.sql": &fstest.MapFile{Data: []byte("SELECT 1")},
			},
			interceptor,
		)
		c.Assert(err, qt.IsNil)

		err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*cannot run a statement interceptor for the up direction in tx-mode file.*`)
		c.Assert(interceptor.called, qt.IsFalse)
		c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
	})

	// An interceptor is refused in both directions, and the message names the
	// direction rather than leaning on a stray article. A rollback body reaches
	// the same execution path as an apply body, so an interceptor can run SQL
	// outside the witness on the way down exactly as it can on the way up.
	t.Run("statement interceptor down", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)

		names := issue887Names(dialect + "_dint")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		upSQL := fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.createdTable)
		downSQL := fmt.Sprintf("DROP TABLE %s", names.createdTable)
		initial := migrator.CreateMigrationFromSQL(1, "interceptor down", upSQL, downSQL)
		c.Assert(issue887Migrator(conn, names, initial).MigrateUp(ctx), qt.IsNil)

		interceptor := &issue887StatementInterceptor{}
		intercepted, err := migrator.NewMigrationFromSQLFilesWithInterceptor(
			1,
			"interceptor down",
			"migration.up.sql",
			"migration.down.sql",
			fstest.MapFS{
				"migration.up.sql":   &fstest.MapFile{Data: []byte(upSQL)},
				"migration.down.sql": &fstest.MapFile{Data: []byte(downSQL)},
			},
			interceptor,
		)
		c.Assert(err, qt.IsNil)

		err = issue887Migrator(conn, names, intercepted).MigrateDown(ctx)
		c.Assert(err, qt.ErrorMatches, `.*cannot run a statement interceptor for the down direction in tx-mode file.*`)
		c.Assert(interceptor.called, qt.IsFalse)
		c.Assert(issue887TableCount(t, conn, names.createdTable), qt.Equals, int64(1))
		c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(1))
	})

	t.Run("nested SQL", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)

		names := issue887Names(dialect + "_call")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		migration := migrator.CreateMigrationFromSQL(1, "nested SQL", "CALL missing_procedure()", "SELECT 1")

		err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*nested or dynamic SQL cannot be tied to Ptah's transaction witness.*`)
		c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
	})

	t.Run("DEFINER function before indirect writer", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		adminConn, err := dbschema.ConnectToDatabase(ctx, adminURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, adminConn)

		names := issue887Names(dialect + "_definer")
		cleanupIssue887(t, adminConn, names)
		defer cleanupIssue887(t, adminConn, names)
		eventName := fmt.Sprintf("ptah887event%d", time.Now().UnixNano())
		defer issue887DropEvent(t, adminConn, eventName)
		migration := migrator.CreateMigrationFromSQL(
			1,
			"DEFINER function before indirect writer",
			fmt.Sprintf(
				"CREATE DEFINER=CURRENT_USER() EVENT %s ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 HOUR DO SELECT 1",
				eventName,
			),
			"SELECT 1",
		)

		err = issue887Migrator(adminConn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*defines an indirect database writer that Ptah cannot validate before execution.*`)
		c.Assert(issue887EventCount(t, adminConn, eventName), qt.Equals, int64(0))
		c.Assert(issue887RevisionCount(t, adminConn, names), qt.Equals, int64(0))
	})

	t.Run("filesystem writes", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)

		names := issue887Names(dialect + "_filesystem")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		cases := []struct {
			name      string
			statement string
		}{
			{
				name:      "OUTFILE",
				statement: fmt.Sprintf("SELECT 'blocked' INTO OUTFILE '/tmp/%s.txt'", names.createdTable),
			},
			{
				name:      "DUMPFILE",
				statement: fmt.Sprintf("SELECT 'blocked' INTO DUMPFILE '/tmp/%s.bin'", names.createdTable),
			},
		}

		for _, test := range cases {
			t.Run(test.name, func(t2 *testing.T) {
				c := qt.New(t2)
				migration := migrator.CreateMigrationFromSQL(1, test.name, test.statement, "SELECT 1")

				err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
				c.Assert(err, qt.ErrorMatches, `.*writes a file outside Ptah's migration transaction.*`)
				c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
			})
		}
	})

	t.Run("cross-database CREATE INDEX", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)
		adminConn, err := dbschema.ConnectToDatabase(ctx, adminURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, adminConn)

		names := issue887Names(dialect + "_xidxc")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		externalDatabase := fmt.Sprintf("ptah887external%d", time.Now().UnixNano())
		indexName := fmt.Sprintf("ptah887idx%d", time.Now().UnixNano())
		_, err = adminConn.ExecContext(ctx, "CREATE DATABASE "+externalDatabase)
		c.Assert(err, qt.IsNil)
		defer issue887DropDatabase(t, adminConn, externalDatabase)
		_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
			"CREATE TABLE %s.external_jobs (id INTEGER PRIMARY KEY)", externalDatabase,
		))
		c.Assert(err, qt.IsNil)
		migration := migrator.CreateMigrationFromSQL(
			1,
			"cross-database CREATE INDEX",
			fmt.Sprintf("CREATE INDEX %s ON %s.external_jobs (id)", indexName, externalDatabase),
			fmt.Sprintf("DROP INDEX %s ON %s.external_jobs", indexName, externalDatabase),
		)

		err = issue887Migrator(adminConn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*references database .* outside the selected database.*`)
		c.Assert(issue887IndexCount(t, adminConn, externalDatabase, "external_jobs", indexName), qt.Equals, int64(0))
		c.Assert(issue887RevisionCount(t, adminConn, names), qt.Equals, int64(0))
	})

	t.Run("cross-database DROP INDEX", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)
		adminConn, err := dbschema.ConnectToDatabase(ctx, adminURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, adminConn)

		names := issue887Names(dialect + "_xidxd")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		externalDatabase := fmt.Sprintf("ptah887external%d", time.Now().UnixNano())
		indexName := fmt.Sprintf("ptah887idx%d", time.Now().UnixNano())
		_, err = adminConn.ExecContext(ctx, "CREATE DATABASE "+externalDatabase)
		c.Assert(err, qt.IsNil)
		defer issue887DropDatabase(t, adminConn, externalDatabase)
		_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
			"CREATE TABLE %s.external_jobs (id INTEGER PRIMARY KEY, note VARCHAR(64))", externalDatabase,
		))
		c.Assert(err, qt.IsNil)
		_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
			"CREATE INDEX %s ON %s.external_jobs (note)", indexName, externalDatabase,
		))
		c.Assert(err, qt.IsNil)
		migration := migrator.CreateMigrationFromSQL(
			1,
			"cross-database DROP INDEX",
			fmt.Sprintf("DROP INDEX %s ON %s.external_jobs", indexName, externalDatabase),
			fmt.Sprintf("CREATE INDEX %s ON %s.external_jobs (note)", indexName, externalDatabase),
		)

		err = issue887Migrator(adminConn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*references database .* outside the selected database.*`)
		c.Assert(issue887IndexCount(t, adminConn, externalDatabase, "external_jobs", indexName), qt.Equals, int64(1))
		c.Assert(issue887RevisionCount(t, adminConn, names), qt.Equals, int64(0))
	})

	t.Run("cross-database foreign key", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)
		adminConn, err := dbschema.ConnectToDatabase(ctx, adminURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, adminConn)

		names := issue887Names(dialect + "_xref")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		externalDatabase := fmt.Sprintf("ptah887external%d", time.Now().UnixNano())
		_, err = adminConn.ExecContext(ctx, "CREATE DATABASE "+externalDatabase)
		c.Assert(err, qt.IsNil)
		defer issue887DropDatabase(t, adminConn, externalDatabase)
		_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
			"CREATE TABLE %s.parents (id INTEGER PRIMARY KEY)", externalDatabase,
		))
		c.Assert(err, qt.IsNil)
		migration := migrator.CreateMigrationFromSQL(
			1,
			"cross-database foreign key",
			fmt.Sprintf(
				"CREATE TABLE %s (id INTEGER PRIMARY KEY, parent_id INTEGER, "+
					"FOREIGN KEY (parent_id) REFERENCES %s.parents (id))",
				names.createdTable,
				externalDatabase,
			),
			fmt.Sprintf("DROP TABLE %s", names.createdTable),
		)

		err = issue887Migrator(adminConn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*references database .* outside the selected database.*`)
		c.Assert(issue887TableCount(t, adminConn, names.createdTable), qt.Equals, int64(0))
		c.Assert(issue887RevisionCount(t, adminConn, names), qt.Equals, int64(0))
	})

	t.Run("cross-database privilege target", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)
		adminConn, err := dbschema.ConnectToDatabase(ctx, adminURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, adminConn)

		names := issue887Names(dialect + "_xgrant")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		externalDatabase := fmt.Sprintf("ptah887external%d", time.Now().UnixNano())
		_, err = adminConn.ExecContext(ctx, "CREATE DATABASE "+externalDatabase)
		c.Assert(err, qt.IsNil)
		defer issue887DropDatabase(t, adminConn, externalDatabase)
		username, _ := issue887CreateUser(t, adminConn)
		defer issue887DropUser(t, adminConn, username)
		migration := migrator.CreateMigrationFromSQL(
			1,
			"cross-database privilege target",
			fmt.Sprintf("GRANT TRIGGER ON %s.* TO '%s'@'%%'", externalDatabase, username),
			fmt.Sprintf("REVOKE TRIGGER ON %s.* FROM '%s'@'%%'", externalDatabase, username),
		)

		err = issue887Migrator(adminConn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*references database .* outside the selected database.*`)
		c.Assert(
			issue887SchemaPrivilegeCount(t, adminConn, externalDatabase, username, "TRIGGER"),
			qt.Equals,
			int64(0),
		)
		c.Assert(issue887RevisionCount(t, adminConn, names), qt.Equals, int64(0))
	})

	t.Run("cross-database CREATE TABLE IF NOT EXISTS", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)
		adminConn, err := dbschema.ConnectToDatabase(ctx, adminURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, adminConn)

		names := issue887Names(dialect + "_xcreate")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		externalDatabase := fmt.Sprintf("ptah887external%d", time.Now().UnixNano())
		_, err = adminConn.ExecContext(ctx, "CREATE DATABASE "+externalDatabase)
		c.Assert(err, qt.IsNil)
		defer issue887DropDatabase(t, adminConn, externalDatabase)
		migration := migrator.CreateMigrationFromSQL(
			1,
			"cross-database CREATE TABLE IF NOT EXISTS",
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.external_jobs (id INTEGER PRIMARY KEY)", externalDatabase),
			fmt.Sprintf("DROP TABLE IF EXISTS %s.external_jobs", externalDatabase),
		)

		err = issue887Migrator(adminConn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*references database .* outside the selected database.*`)
		c.Assert(issue887TableCountInSchema(t, adminConn, externalDatabase, "external_jobs"), qt.Equals, int64(0))
		c.Assert(issue887RevisionCount(t, adminConn, names), qt.Equals, int64(0))
	})

	t.Run("cross-database DROP TABLE IF EXISTS", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)
		adminConn, err := dbschema.ConnectToDatabase(ctx, adminURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, adminConn)

		names := issue887Names(dialect + "_xdrop")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		externalDatabase := fmt.Sprintf("ptah887external%d", time.Now().UnixNano())
		_, err = adminConn.ExecContext(ctx, "CREATE DATABASE "+externalDatabase)
		c.Assert(err, qt.IsNil)
		defer issue887DropDatabase(t, adminConn, externalDatabase)
		_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
			"CREATE TABLE %s.external_jobs (id INTEGER PRIMARY KEY)", externalDatabase,
		))
		c.Assert(err, qt.IsNil)
		migration := migrator.CreateMigrationFromSQL(
			1,
			"cross-database DROP TABLE IF EXISTS",
			fmt.Sprintf("DROP TABLE IF EXISTS %s.external_jobs", externalDatabase),
			fmt.Sprintf("CREATE TABLE %s.external_jobs (id INTEGER PRIMARY KEY)", externalDatabase),
		)

		err = issue887Migrator(adminConn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*references database .* outside the selected database.*`)
		c.Assert(issue887TableCountInSchema(t, adminConn, externalDatabase, "external_jobs"), qt.Equals, int64(1))
		c.Assert(issue887RevisionCount(t, adminConn, names), qt.Equals, int64(0))
	})

	t.Run("cross-database TRUNCATE", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)
		adminConn, err := dbschema.ConnectToDatabase(ctx, adminURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, adminConn)

		names := issue887Names(dialect + "_xtrunc")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		externalDatabase := fmt.Sprintf("ptah887external%d", time.Now().UnixNano())
		_, err = adminConn.ExecContext(ctx, "CREATE DATABASE "+externalDatabase)
		c.Assert(err, qt.IsNil)
		defer issue887DropDatabase(t, adminConn, externalDatabase)
		_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
			"CREATE TABLE %s.external_jobs (id INTEGER PRIMARY KEY) ENGINE=MyISAM", externalDatabase,
		))
		c.Assert(err, qt.IsNil)
		_, err = adminConn.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s.external_jobs VALUES (1)", externalDatabase))
		c.Assert(err, qt.IsNil)
		migration := migrator.CreateMigrationFromSQL(
			1,
			"cross-database TRUNCATE",
			fmt.Sprintf("TRUNCATE %s.external_jobs", externalDatabase),
			"SELECT 1",
		)

		err = issue887Migrator(adminConn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*references database .* outside the selected database.*`)
		c.Assert(
			issue887ScalarCount(t, adminConn, fmt.Sprintf("SELECT COUNT(*) FROM %s.external_jobs", externalDatabase)),
			qt.Equals,
			int64(1),
		)
		c.Assert(issue887RevisionCount(t, adminConn, names), qt.Equals, int64(0))
	})

	t.Run("cross-database routine privilege target", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)
		adminConn, err := dbschema.ConnectToDatabase(ctx, adminURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, adminConn)

		names := issue887Names(dialect + "_xrgrant")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		externalDatabase := fmt.Sprintf("ptah887external%d", time.Now().UnixNano())
		_, err = adminConn.ExecContext(ctx, "CREATE DATABASE "+externalDatabase)
		c.Assert(err, qt.IsNil)
		defer issue887DropDatabase(t, adminConn, externalDatabase)
		_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
			"CREATE FUNCTION %s.next_job_id() RETURNS INT DETERMINISTIC RETURN 7", externalDatabase,
		))
		c.Assert(err, qt.IsNil)
		username, _ := issue887CreateUser(t, adminConn)
		defer issue887DropUser(t, adminConn, username)
		migration := migrator.CreateMigrationFromSQL(
			1,
			"cross-database routine privilege target",
			fmt.Sprintf("GRANT EXECUTE ON FUNCTION %s.next_job_id TO '%s'@'%%'", externalDatabase, username),
			fmt.Sprintf("REVOKE EXECUTE ON FUNCTION %s.next_job_id FROM '%s'@'%%'", externalDatabase, username),
		)

		err = issue887Migrator(adminConn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*references database .* outside the selected database.*`)
		c.Assert(
			issue887RoutinePrivilegeCount(t, adminConn, externalDatabase, "next_job_id", username, "EXECUTE"),
			qt.Equals,
			int64(0),
		)
		c.Assert(issue887RevisionCount(t, adminConn, names), qt.Equals, int64(0))
	})

	t.Run("cross-database RENAME TABLE", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)
		adminConn, err := dbschema.ConnectToDatabase(ctx, adminURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, adminConn)

		names := issue887Names(dialect + "_cross_db_rename")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		externalDatabase := fmt.Sprintf("ptah887external%d", time.Now().UnixNano())
		_, err = adminConn.ExecContext(ctx, "CREATE DATABASE "+externalDatabase)
		c.Assert(err, qt.IsNil)
		defer issue887DropDatabase(t, adminConn, externalDatabase)
		_, err = conn.ExecContext(ctx, fmt.Sprintf(
			"CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.createdTable,
		))
		c.Assert(err, qt.IsNil)
		migration := migrator.CreateMigrationFromSQL(
			1,
			"cross-database RENAME TABLE",
			fmt.Sprintf("RENAME TABLE %s TO %s.external_jobs", names.createdTable, externalDatabase),
			fmt.Sprintf("RENAME TABLE %s.external_jobs TO %s", externalDatabase, names.createdTable),
		)

		err = issue887Migrator(adminConn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*references database .* outside the selected database.*`)
		c.Assert(issue887TableCount(t, conn, names.createdTable), qt.Equals, int64(1))
		c.Assert(issue887TableCountInSchema(t, adminConn, externalDatabase, "external_jobs"), qt.Equals, int64(0))
		c.Assert(issue887RevisionCount(t, adminConn, names), qt.Equals, int64(0))
	})

	t.Run("cross-database table", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)
		adminConn, err := dbschema.ConnectToDatabase(ctx, adminURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, adminConn)

		names := issue887Names(dialect + "_cross_db")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		externalDatabase := fmt.Sprintf("ptah887external%d", time.Now().UnixNano())
		_, err = adminConn.ExecContext(ctx, "CREATE DATABASE "+externalDatabase)
		c.Assert(err, qt.IsNil)
		defer issue887DropDatabase(t, adminConn, externalDatabase)
		_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
			"CREATE TABLE %s.external_jobs (id INTEGER PRIMARY KEY) ENGINE=MyISAM", externalDatabase,
		))
		c.Assert(err, qt.IsNil)
		migration := migrator.CreateMigrationFromSQL(
			1,
			"cross database",
			fmt.Sprintf("INSERT INTO %s.external_jobs VALUES (1)", externalDatabase),
			fmt.Sprintf("DELETE FROM %s.external_jobs", externalDatabase),
		)

		err = issue887Migrator(adminConn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*references database .* outside the selected database.*`)
		c.Assert(
			issue887ScalarCount(t, adminConn, fmt.Sprintf("SELECT COUNT(*) FROM %s.external_jobs", externalDatabase)),
			qt.Equals,
			int64(0),
		)
		c.Assert(issue887RevisionCount(t, adminConn, names), qt.Equals, int64(0))
	})

	t.Run("database creation", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)
		adminConn, err := dbschema.ConnectToDatabase(ctx, adminURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, adminConn)

		names := issue887Names(dialect + "_create_db")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		databaseName := fmt.Sprintf("ptah887created%d", time.Now().UnixNano())
		defer issue887DropDatabase(t, adminConn, databaseName)
		migration := migrator.CreateMigrationFromSQL(
			1,
			"database creation",
			"CREATE DATABASE "+databaseName,
			"DROP DATABASE "+databaseName,
		)

		err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*changes state outside Ptah's migration transaction.*`)
		c.Assert(
			issue887ScalarCount(
				t,
				adminConn,
				fmt.Sprintf("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '%s'", databaseName),
			),
			qt.Equals,
			int64(0),
		)
		c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
	})

	t.Run("triggered relation", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)
		adminConn, err := dbschema.ConnectToDatabase(ctx, adminURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, adminConn)

		names := issue887Names(dialect + "_trigger")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		triggerName := fmt.Sprintf("ptah887trigger%d", time.Now().UnixNano())
		_, err = conn.ExecContext(ctx, fmt.Sprintf(
			"CREATE TABLE %s (id INTEGER PRIMARY KEY, note VARCHAR(64))", names.ledgerTable,
		))
		c.Assert(err, qt.IsNil)
		_, err = conn.ExecContext(ctx, fmt.Sprintf(
			"CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.blockerTable,
		))
		c.Assert(err, qt.IsNil)
		_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
			"CREATE TRIGGER %s AFTER INSERT ON %s FOR EACH ROW INSERT INTO %s (id) VALUES (NEW.id)",
			triggerName,
			names.ledgerTable,
			names.blockerTable,
		))
		c.Assert(err, qt.IsNil)
		defer issue887DropTrigger(t, adminConn, triggerName)
		migration := migrator.CreateMigrationFromSQL(
			1,
			"triggered relation",
			fmt.Sprintf("INSERT INTO %s VALUES (1, 'one')", names.ledgerTable),
			fmt.Sprintf("DELETE FROM %s", names.ledgerTable),
		)

		err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*relation .* has indirect behavior that Ptah cannot tie to the transaction witness.*`)
		c.Assert(issue887LedgerCount(t, conn, names), qt.Equals, int64(0))
		c.Assert(issue887ScalarCount(t, conn, "SELECT COUNT(*) FROM "+names.blockerTable), qt.Equals, int64(0))
		c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
	})

	t.Run("stored function", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)
		adminConn, err := dbschema.ConnectToDatabase(ctx, adminURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, adminConn)

		names := issue887Names(dialect + "_routine")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		routineName := fmt.Sprintf("ptah887routine%d", time.Now().UnixNano())
		_, err = conn.ExecContext(ctx, fmt.Sprintf(
			"CREATE TABLE %s (id INTEGER PRIMARY KEY, note VARCHAR(64))", names.ledgerTable,
		))
		c.Assert(err, qt.IsNil)
		_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
			"CREATE FUNCTION %s() RETURNS INT DETERMINISTIC RETURN 7", routineName,
		))
		c.Assert(err, qt.IsNil)
		defer issue887DropFunction(t, adminConn, routineName)
		migration := migrator.CreateMigrationFromSQL(
			1,
			"stored function",
			fmt.Sprintf("INSERT INTO %s VALUES (%s(), 'one')", names.ledgerTable, routineName),
			fmt.Sprintf("DELETE FROM %s", names.ledgerTable),
		)

		err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*routine .* can execute SQL outside Ptah's transaction witness.*`)
		c.Assert(issue887LedgerCount(t, conn, names), qt.Equals, int64(0))
		c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
	})

	t.Run("secret redaction", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer issue887CloseConnection(t, conn)

		names := issue887Names(dialect + "_secret")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		secret := fmt.Sprintf("ptah-%s-%d", dialect, time.Now().UnixNano())
		migration := migrator.CreateMigrationFromSQL(
			1,
			"secret",
			"SET PASSWORD = '"+secret+"'",
			"SELECT 1",
		)

		err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Not(qt.Contains), secret)
		c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
	})
}

func runMySQLWithoutTriggerPrivilegeFailsClosed(t *testing.T, adminURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	adminConn, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, adminConn)

	username, password := issue887CreateUser(t, adminConn)
	defer issue887DropUser(t, adminConn, username)
	issue887GrantBasePrivileges(t, adminConn, "mysql", username)

	limitedURL := issue887ReplaceMySQLCredentials(t, adminURL, username, password)
	conn, err := dbschema.ConnectToDatabase(ctx, limitedURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names("mysql_hidden_trigger")
	cleanupIssue887(t, adminConn, names)
	defer cleanupIssue887(t, adminConn, names)
	triggerName := fmt.Sprintf("ptah887trigger%d", time.Now().UnixNano())
	_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY, note VARCHAR(64))", names.ledgerTable,
	))
	c.Assert(err, qt.IsNil)
	_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TRIGGER %s BEFORE INSERT ON %s FOR EACH ROW SET NEW.note = 'triggered'",
		triggerName,
		names.ledgerTable,
	))
	c.Assert(err, qt.IsNil)
	defer issue887DropTrigger(t, adminConn, triggerName)
	migration := migrator.CreateMigrationFromSQL(
		1,
		"hidden trigger catalog",
		fmt.Sprintf("INSERT INTO %s VALUES (1, 'one')", names.ledgerTable),
		fmt.Sprintf("DELETE FROM %s", names.ledgerTable),
	)

	err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `.*tx-mode file requires the TRIGGER privilege at database or global scope.*`)
	c.Assert(issue887LedgerCount(t, adminConn, names), qt.Equals, int64(0))
	c.Assert(issue887RevisionCount(t, adminConn, names), qt.Equals, int64(0))
}

func runMariaDBWithoutTriggerPrivilegeStillRejectsTriggeredRelation(t *testing.T, adminURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	adminConn, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, adminConn)

	username, password := issue887CreateUser(t, adminConn)
	defer issue887DropUser(t, adminConn, username)
	issue887GrantBasePrivileges(t, adminConn, "mariadb", username)

	limitedURL := issue887ReplaceMySQLCredentials(t, adminURL, username, password)
	conn, err := dbschema.ConnectToDatabase(ctx, limitedURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names("mariadb_hidden_trigger")
	cleanupIssue887(t, adminConn, names)
	defer cleanupIssue887(t, adminConn, names)
	triggerName := fmt.Sprintf("ptah887trigger%d", time.Now().UnixNano())
	_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY, note VARCHAR(64))", names.ledgerTable,
	))
	c.Assert(err, qt.IsNil)
	_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TRIGGER %s BEFORE INSERT ON %s FOR EACH ROW SET NEW.note = 'triggered'",
		triggerName,
		names.ledgerTable,
	))
	c.Assert(err, qt.IsNil)
	defer issue887DropTrigger(t, adminConn, triggerName)
	migration := migrator.CreateMigrationFromSQL(
		1,
		"visible trigger catalog",
		fmt.Sprintf("INSERT INTO %s VALUES (1, 'one')", names.ledgerTable),
		fmt.Sprintf("DELETE FROM %s", names.ledgerTable),
	)

	err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `.*relation .* has indirect behavior that Ptah cannot tie to the transaction witness.*`)
	c.Assert(issue887LedgerCount(t, adminConn, names), qt.Equals, int64(0))
	c.Assert(issue887RevisionCount(t, adminConn, names), qt.Equals, int64(0))
}

func runMySQLDefaultRoleTriggerPrivilegeIsAccepted(t *testing.T, adminURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	adminConn, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, adminConn)

	username, password := issue887CreateUser(t, adminConn)
	defer issue887DropUser(t, adminConn, username)
	issue887GrantBasePrivileges(t, adminConn, "mysql", username)
	roleName := fmt.Sprintf("r887_%d", time.Now().UnixNano())
	_, err = adminConn.ExecContext(ctx, fmt.Sprintf("CREATE ROLE '%s'@'%%'", roleName))
	c.Assert(err, qt.IsNil)
	defer issue887DropRole(t, adminConn, roleName)
	_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
		"GRANT TRIGGER ON %s.* TO '%s'@'%%'",
		sqlident.Quote("mysql", adminConn.Info().Schema),
		roleName,
	))
	c.Assert(err, qt.IsNil)
	_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
		"GRANT '%s'@'%%' TO '%s'@'%%'", roleName, username,
	))
	c.Assert(err, qt.IsNil)
	_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
		"SET DEFAULT ROLE '%s'@'%%' TO '%s'@'%%'", roleName, username,
	))
	c.Assert(err, qt.IsNil)

	limitedURL := issue887ReplaceMySQLCredentials(t, adminURL, username, password)
	conn, err := dbschema.ConnectToDatabase(ctx, limitedURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names("mysql_default_role")
	cleanupIssue887(t, adminConn, names)
	defer cleanupIssue887(t, adminConn, names)
	_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY, note VARCHAR(64))", names.ledgerTable,
	))
	c.Assert(err, qt.IsNil)
	triggerName := fmt.Sprintf("ptah887trigger%d", time.Now().UnixNano())
	_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TRIGGER %s BEFORE INSERT ON %s FOR EACH ROW SET NEW.note = 'triggered'",
		triggerName,
		names.ledgerTable,
	))
	c.Assert(err, qt.IsNil)
	defer issue887DropTrigger(t, adminConn, triggerName)
	migration := migrator.CreateMigrationFromSQL(
		1,
		"default role trigger privilege",
		fmt.Sprintf("INSERT INTO %s VALUES (1, 'one')", names.ledgerTable),
		fmt.Sprintf("DELETE FROM %s", names.ledgerTable),
	)

	err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `.*relation .* has indirect behavior that Ptah cannot tie to the transaction witness.*`)
	c.Assert(issue887LedgerCount(t, adminConn, names), qt.Equals, int64(0))
	c.Assert(issue887RevisionCount(t, adminConn, names), qt.Equals, int64(0))
}

func runMySQLRejectsFilesystemWritesBeforeSideEffect(t *testing.T, adminURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names("mysql_files")
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)
	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY, note VARCHAR(64))", names.ledgerTable,
	))
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'one')", names.ledgerTable))
	c.Assert(err, qt.IsNil)

	var secureFileDir string
	err = conn.QueryRowContext(ctx, "SELECT @@secure_file_priv").Scan(&secureFileDir)
	c.Assert(err, qt.IsNil)
	c.Assert(secureFileDir, qt.Not(qt.Equals), "")
	filePrefix := strings.TrimRight(secureFileDir, "/") + "/" + names.createdTable
	tests := []struct {
		name      string
		statement string
		path      string
	}{
		{
			name:      "SELECT OUTFILE",
			statement: fmt.Sprintf("SELECT 'blocked' INTO OUTFILE '%s.txt'", filePrefix),
			path:      filePrefix + ".txt",
		},
		{
			name:      "SELECT DUMPFILE",
			statement: fmt.Sprintf("SELECT 'blocked' INTO DUMPFILE '%s.bin'", filePrefix),
			path:      filePrefix + ".bin",
		},
		{
			name:      "TABLE OUTFILE",
			statement: fmt.Sprintf("TABLE %s INTO OUTFILE '%s.tbl'", names.ledgerTable, filePrefix),
			path:      filePrefix + ".tbl",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			migration := migrator.CreateMigrationFromSQL(1, test.name, test.statement, "SELECT 1")

			err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
			c.Assert(err, qt.ErrorMatches, `.*writes a file outside Ptah's migration transaction.*`)
			c.Assert(issue887FileMissing(t, conn, test.path), qt.IsTrue)
			c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
		})
	}
}

func runMySQLEscapedDatabaseGrantIsAccepted(t *testing.T, adminURL string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	adminConn, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, adminConn)

	database := fmt.Sprintf("p887_test_%d", time.Now().UnixNano())
	_, err = adminConn.ExecContext(ctx, "CREATE DATABASE "+database)
	c.Assert(err, qt.IsNil)
	defer issue887DropDatabase(t, adminConn, database)
	username, password := issue887CreateUser(t, adminConn)
	defer issue887DropUser(t, adminConn, username)
	_, err = adminConn.ExecContext(ctx, fmt.Sprintf(
		"GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%%'",
		sqlident.Quote("mysql", database),
		username,
	))
	c.Assert(err, qt.IsNil)

	limitedURL := issue887ReplaceMySQLDatabase(
		t,
		issue887ReplaceMySQLCredentials(t, adminURL, username, password),
		database,
	)
	conn, err := dbschema.ConnectToDatabase(ctx, limitedURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names("mysql_escaped_grant")
	migration := migrator.CreateMigrationFromSQL(
		1,
		"escaped database grant",
		fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY) ENGINE=InnoDB", names.createdTable),
		fmt.Sprintf("DROP TABLE %s", names.createdTable),
	)
	err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(issue887TableCount(t, conn, names.createdTable), qt.Equals, int64(1))
	c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(1))
}

func runRolledBackDataOnlyBody(t *testing.T, dbURL, dialect string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(dialect + "_dml")
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY, note VARCHAR(64))", names.ledgerTable,
	))
	c.Assert(err, qt.IsNil)

	body := fmt.Sprintf(
		"INSERT INTO %[1]s (id, note) VALUES (1, 'one');\n"+
			"INSERT INTO %[1]s (id, note) SELECT 2, 'two' FROM %[2]s;\n"+
			"INSERT INTO %[1]s (id, note) VALUES (3, 'three');\n",
		names.ledgerTable, names.blockerTable,
	)
	migration := migrator.CreateMigrationFromSQL(1, "data only", body,
		fmt.Sprintf("DELETE FROM %s", names.ledgerTable))

	failing := issue887Migrator(conn, names, migration)
	err = failing.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{DiscardRolledBackFailure: true})
	c.Assert(err, qt.IsNotNil)
	c.Assert(issue887LedgerCount(t, conn, names), qt.Equals, int64(0))
	// Nothing survived the rollback, so nothing may be recorded as committed:
	// the Atlas-shaped surface expresses that by leaving no revision at all.
	c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))

	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.blockerTable,
	))
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id) VALUES (7)", names.blockerTable))
	c.Assert(err, qt.IsNil)

	retried := issue887Migrator(conn, names, migration)
	err = retried.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{
		AllowDirty:               true,
		DiscardRolledBackFailure: true,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(issue887LedgerCount(t, conn, names), qt.Equals, int64(3))
	c.Assert(issue887LedgerNotes(t, conn, names), qt.DeepEquals, []string{"one", "three", "two"})
}

func runRolledBackCommittedDDLPrefix(t *testing.T, dbURL, dialect string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(dialect + "_ddl")
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY, note VARCHAR(64))", names.ledgerTable,
	))
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.blockerTable,
	))
	c.Assert(err, qt.IsNil)

	body := fmt.Sprintf(
		"INSERT INTO %[1]s (id, note) VALUES (1, 'one');\n"+
			"CREATE TABLE %[2]s (id INTEGER PRIMARY KEY);\n"+
			"INSERT INTO %[1]s (id, note) VALUES (3, 'three');\n",
		names.ledgerTable, names.blockerTable,
	)
	migration := migrator.CreateMigrationFromSQL(1, "ddl prefix", body,
		fmt.Sprintf("DELETE FROM %s", names.ledgerTable))

	failing := issue887Migrator(conn, names, migration)
	err = failing.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{DiscardRolledBackFailure: true})
	c.Assert(err, qt.IsNotNil)
	// MySQL committed the INSERT when it reached the CREATE TABLE, so the row is
	// there and the revision has to say so.
	c.Assert(issue887LedgerCount(t, conn, names), qt.Equals, int64(1))
	c.Assert(issue887AppliedCount(t, conn, names), qt.Equals, int64(1))
	c.Assert(issue887RevisionError(t, conn, names), qt.Equals, "statement execution outcome is unknown after process interruption")

	_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", names.blockerTable))
	c.Assert(err, qt.IsNil)

	retried := issue887Migrator(conn, names, migration)
	err = retried.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{
		AllowDirty:               true,
		DiscardRolledBackFailure: true,
	})
	c.Assert(err, qt.ErrorMatches, `migration 1 cannot resume automatically: the outcome of statement 2 is unknown.*`)
	// The retry did not enter the body and therefore did not repeat the committed
	// INSERT or run the statement after the failed DDL.
	c.Assert(issue887LedgerCount(t, conn, names), qt.Equals, int64(1))
	c.Assert(issue887LedgerNotes(t, conn, names), qt.DeepEquals, []string{"one"})
}

func runRolledBackDDLThenDML(
	t *testing.T,
	dbURL,
	dialect string,
	revisionFormat migrator.RevisionTableFormat,
) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(dialect + "_ddl_dml")
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY, note VARCHAR(64))", names.ledgerTable,
	))
	c.Assert(err, qt.IsNil)

	body := fmt.Sprintf(
		"CREATE TABLE %[1]s (id INTEGER PRIMARY KEY);\n"+
			"INSERT INTO %[2]s (id, note) VALUES (1, 'one');\n"+
			"INSERT INTO %[3]s (id) VALUES (7);\n",
		names.createdTable, names.ledgerTable, names.blockerTable,
	)
	migration := migrator.CreateMigrationFromSQL(1, "ddl then dml", body,
		fmt.Sprintf("DROP TABLE %s", names.createdTable))

	failing := issue887MigratorWithFormat(conn, names, revisionFormat, migration)
	err = failing.MigrateUp(ctx)
	c.Assert(err, qt.IsNotNil)
	c.Assert(issue887MetadataEngine(t, conn, names), qt.Equals, "InnoDB")
	c.Assert(issue887LedgerCount(t, conn, names), qt.Equals, int64(1))
	c.Assert(issue887AppliedCount(t, conn, names), qt.Equals, int64(2))

	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.blockerTable,
	))
	c.Assert(err, qt.IsNil)

	retried := issue887MigratorWithFormat(conn, names, revisionFormat, migration)
	err = retried.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{AllowDirty: true})
	c.Assert(err, qt.IsNil)
	c.Assert(issue887LedgerCount(t, conn, names), qt.Equals, int64(1))
	c.Assert(issue887ScalarCount(t, conn, fmt.Sprintf("SELECT COUNT(*) FROM %s", names.blockerTable)), qt.Equals, int64(1))
}

// runRejectsTransactionControl drives the primary transaction-control arm of
// the tx-mode file preflight through MigrateUp on a live server.
//
// The witness contract holds only while one InnoDB transaction covers both the
// body and the revision row, so a body statement that ends that transaction
// breaks it. `COMMIT AND CHAIN` and `ROLLBACK AND CHAIN` are the cases worth
// naming: they end the transaction and immediately open another one, so the
// session still looks like it is inside a transaction even though everything
// before them has already been made durable or thrown away. A classifier that
// asks "is this a bare COMMIT or ROLLBACK?" instead of "does this control the
// transaction?" lets both through, MigrateUp then returns nil for a run that
// applied only the tail of the body, and the revision row calls the migration
// complete. The counted notes are what separate that from a correct run: the
// returned error alone cannot, because the broken version returns none.
func runRejectsTransactionControl(t *testing.T, dbURL, dialect string) {
	t.Helper()

	tests := []struct {
		name    string
		control string
	}{
		{name: "autocommit", control: "SET autocommit = 0"},
		{name: "begin", control: "BEGIN"},
		{name: "commit", control: "COMMIT"},
		{name: "rollback", control: "ROLLBACK"},
		{name: "commit and chain", control: "COMMIT AND CHAIN"},
		{name: "rollback and chain", control: "ROLLBACK AND CHAIN"},
		{name: "start transaction", control: "START TRANSACTION"},
		{name: "savepoint", control: "SAVEPOINT ptah_887_control"},
		{name: "release savepoint", control: "RELEASE SAVEPOINT ptah_887_control"},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
			c.Assert(err, qt.IsNil)
			defer issue887CloseConnection(t, conn)

			names := issue887Names(fmt.Sprintf("%s_txcontrol%d", dialect, index))
			cleanupIssue887(t, conn, names)
			defer cleanupIssue887(t, conn, names)
			_, err = conn.ExecContext(ctx, fmt.Sprintf(
				"CREATE TABLE %s (id INTEGER PRIMARY KEY, note VARCHAR(64))", names.ledgerTable,
			))
			c.Assert(err, qt.IsNil)

			body := fmt.Sprintf(
				"INSERT INTO %[1]s (id, note) VALUES (1, 'one');\n"+
					"%[2]s;\n"+
					"INSERT INTO %[1]s (id, note) VALUES (2, 'two');\n",
				names.ledgerTable, test.control,
			)
			migration := migrator.CreateMigrationFromSQL(1, "transaction control", body,
				fmt.Sprintf("DELETE FROM %s", names.ledgerTable))

			err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
			// Checks, not assertions: a classifier that misses this control
			// reports success and loses the first note, and both symptoms are
			// worth seeing in the same failure.
			c.Check(err, qt.ErrorMatches,
				`.*migration 1 cannot run tx-mode file statement 2 because it controls transaction state.*`)
			c.Check(issue887LedgerNotes(t, conn, names), qt.DeepEquals, []string{})
			c.Check(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
		})
	}
}

func runRejectsChangingTargetDatabase(t *testing.T, dbURL, dialect string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(dialect + "_use")
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	body := fmt.Sprintf(
		"CREATE TABLE %[1]s (id INTEGER PRIMARY KEY);\n"+
			"USE %[2]s;\n",
		names.createdTable, conn.Info().Schema,
	)
	migration := migrator.CreateMigrationFromSQL(1, "change database", body,
		fmt.Sprintf("DROP TABLE %s", names.createdTable))

	mig := issue887Migrator(conn, names, migration)
	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `.*migration 1 cannot run tx-mode file statement 2 because it changes the target database; select the database in the connection URL so Ptah can verify its storage engines`)
	c.Assert(issue887TableCount(t, conn, names.createdTable), qt.Equals, int64(0))
	c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
}

func runRolledBackSessionStateReplay(t *testing.T, dbURL, dialect string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(dialect + "_session_replay")
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	body := fmt.Sprintf(
		"SET SESSION time_zone = '+02:00';\n"+
			"CREATE TABLE %[1]s (id INTEGER PRIMARY KEY, observed_epoch BIGINT NOT NULL);\n"+
			"INSERT INTO %[1]s (id, observed_epoch) VALUES (1, UNIX_TIMESTAMP('2000-01-01 02:00:00'));\n"+
			"INSERT INTO %[2]s (id, observed_epoch) VALUES (7, UNIX_TIMESTAMP('2000-01-01 02:00:00'));\n",
		names.createdTable, names.blockerTable,
	)
	migration := migrator.CreateMigrationFromSQL(1, "session replay", body,
		fmt.Sprintf("DROP TABLE %s", names.createdTable))

	failing := issue887Migrator(conn, names, migration)
	err = failing.MigrateUp(ctx)
	c.Assert(err, qt.IsNotNil)
	c.Assert(issue887AppliedCount(t, conn, names), qt.Equals, int64(3))

	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY, observed_epoch BIGINT NOT NULL)", names.blockerTable,
	))
	c.Assert(err, qt.IsNil)

	retried := issue887Migrator(conn, names, migration)
	err = retried.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{AllowDirty: true})
	c.Assert(err, qt.IsNil)
	c.Assert(issue887ScalarCount(t, conn, fmt.Sprintf("SELECT COUNT(*) FROM %s", names.createdTable)), qt.Equals, int64(1))
	c.Assert(
		issue887ScalarCount(t, conn, fmt.Sprintf("SELECT observed_epoch FROM %s WHERE id = 7", names.blockerTable)),
		qt.Equals,
		int64(946684800),
	)
}

func runRolledBackDownProgress(
	t *testing.T,
	dbURL,
	prefix string,
	revisionFormat migrator.RevisionTableFormat,
) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(prefix)
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY, note VARCHAR(64))", names.ledgerTable,
	))
	c.Assert(err, qt.IsNil)

	upSQL := fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.createdTable)
	downSQL := fmt.Sprintf(
		"DROP TABLE %[1]s;\n"+
			"INSERT INTO %[2]s (id, note) VALUES (1, 'one');\n"+
			"INSERT INTO %[3]s (id) VALUES (7);\n",
		names.createdTable, names.ledgerTable, names.blockerTable,
	)
	migration := migrator.CreateMigrationFromSQL(1, "down progress", upSQL, downSQL)
	mig := issue887MigratorWithFormat(conn, names, revisionFormat, migration)

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.IsNil)
	err = mig.MigrateDown(ctx)
	c.Assert(err, qt.IsNotNil)

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Direction, qt.Equals, migrator.MigrationDirectionDown)
	c.Assert(status.DirtyRevision.Applied, qt.Equals, 2)
	c.Assert(issue887LedgerCount(t, conn, names), qt.Equals, int64(1))
}

func runRejectsNonTransactionalMetadata(t *testing.T, dbURL, prefix string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(prefix)
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (version VARCHAR(191) PRIMARY KEY) ENGINE=MyISAM", names.revisionsTable,
	))
	c.Assert(err, qt.IsNil)

	migration := migrator.CreateMigrationFromSQL(1, "metadata engine", "SELECT 1", "SELECT 1")
	mig := issue887Migrator(conn, names, migration)
	conn.SchemaWriter().SetDryRun(true)
	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `.*migrations metadata table .* must use InnoDB to track MySQL-family implicit commits; found MyISAM`)
	conn.SchemaWriter().SetDryRun(false)
	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `.*migrations metadata table .* must use InnoDB to track MySQL-family implicit commits; found MyISAM`)
}

func runRejectsNonTransactionalNativeMetadataBeforeUpgrade(t *testing.T, dbURL, prefix string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(prefix)
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	_, err = conn.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s (
version BIGINT PRIMARY KEY,
description TEXT NOT NULL,
applied_at TIMESTAMP NOT NULL
) ENGINE=MyISAM`, names.revisionsTable))
	c.Assert(err, qt.IsNil)

	migration := migrator.CreateMigrationFromSQL(1, "native metadata engine", "SELECT 1", "SELECT 1")
	mig := issue887MigratorWithFormat(conn, names, migrator.RevisionTableFormatPtah, migration)
	conn.SchemaWriter().SetDryRun(true)
	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `.*migrations metadata table .* must use InnoDB to track MySQL-family implicit commits; found MyISAM`)
	c.Assert(issue887ColumnCount(t, conn, names.revisionsTable, "state"), qt.Equals, int64(0))

	conn.SchemaWriter().SetDryRun(false)
	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `.*migrations metadata table .* must use InnoDB to track MySQL-family implicit commits; found MyISAM`)
	c.Assert(issue887ColumnCount(t, conn, names.revisionsTable, "state"), qt.Equals, int64(0))
}

func runRejectsNonTransactionalTargetTable(t *testing.T, dbURL, prefix string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(prefix)
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY) ENGINE=MyISAM", names.ledgerTable,
	))
	c.Assert(err, qt.IsNil)

	migration := migrator.CreateMigrationFromSQL(
		1,
		"target engine",
		fmt.Sprintf("INSERT INTO %s (id) VALUES (1)", names.ledgerTable),
		fmt.Sprintf("DELETE FROM %s", names.ledgerTable),
	)
	mig := issue887Migrator(conn, names, migration)
	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `.*tx-mode file requires InnoDB target tables on MySQL-family databases; table .* uses MyISAM`)
	c.Assert(issue887ScalarCount(t, conn, fmt.Sprintf("SELECT COUNT(*) FROM %s", names.ledgerTable)), qt.Equals, int64(0))
	c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
}

func runRejectsCreatingNonTransactionalTargetTable(t *testing.T, dbURL, prefix string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(prefix)
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	migration := migrator.CreateMigrationFromSQL(
		1,
		"create target engine",
		fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY) ENGINE=MyISAM", names.createdTable),
		fmt.Sprintf("DROP TABLE %s", names.createdTable),
	)
	mig := issue887Migrator(conn, names, migration)
	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `.*migration 1 statement 1 selects non-transactional storage engine MyISAM; tx-mode file requires InnoDB on MySQL-family databases`)
	c.Assert(issue887TableCount(t, conn, names.createdTable), qt.Equals, int64(0))
	c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
}

func runRejectsMariaDBAlterStorageEngine(t *testing.T, dbURL, prefix, engine, wantErr string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(prefix)
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)
	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY) ENGINE=InnoDB",
		names.createdTable,
	))
	c.Assert(err, qt.IsNil)

	migration := migrator.CreateMigrationFromSQL(
		1,
		"alter storage engine",
		fmt.Sprintf("ALTER TABLE %s STORAGE ENGINE=%s", names.createdTable, engine),
		fmt.Sprintf("ALTER TABLE %s ENGINE=InnoDB", names.createdTable),
	)
	err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, wantErr)
	c.Assert(issue887TableEngine(t, conn, names.createdTable), qt.Equals, "InnoDB")
	c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
}

func runRejectsDefaultStorageEngineReset(t *testing.T, dbURL, prefix string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(prefix)
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	migration := migrator.CreateMigrationFromSQL(
		1,
		"reset target engine",
		fmt.Sprintf(
			"SET SESSION default_storage_engine = DEFAULT; CREATE TABLE %s (id INTEGER PRIMARY KEY)",
			names.createdTable,
		),
		fmt.Sprintf("DROP TABLE %s", names.createdTable),
	)
	err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `.*migration 1 statement 1 selects storage engine DEFAULT, whose effective engine can differ from the verified session default; select InnoDB explicitly or use tx-mode none`)
	c.Assert(issue887TableCount(t, conn, names.createdTable), qt.Equals, int64(0))
	c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
}

func runRejectsUnsafeSQLModeChange(t *testing.T, dbURL, prefix string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL+"?multiStatements=true")
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(prefix)
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	migration := migrator.CreateMigrationFromSQL(
		1,
		"unsafe sql mode",
		fmt.Sprintf(
			"SET SESSION `sql_mode` = 'NO_BACKSLASH_ESCAPES'; SELECT 'safe\\'; CREATE TABLE %s (id INTEGER PRIMARY KEY)",
			names.createdTable,
		),
		fmt.Sprintf("DROP TABLE %s", names.createdTable),
	)
	err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `.*migration 1 cannot run tx-mode file statement 1 because changing sql_mode can make the MySQL-family server disagree with Ptah's prevalidated statement boundaries; configure a stable session mode before migration or use tx-mode none`)
	c.Assert(issue887TableCount(t, conn, names.createdTable), qt.Equals, int64(0))
	c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
}

func runRejectsUnsafeInitialSQLMode(t *testing.T, dbURL, prefix string) {
	t.Helper()
	runRejectsInitialSQLMode(t, dbURL, prefix, "%27NO_BACKSLASH_ESCAPES%27", "NO_BACKSLASH_ESCAPES")
}

func runRejectsInitialSQLMode(t *testing.T, dbURL, prefix, encodedSQLMode, wantMode string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL+"?sql_mode="+encodedSQLMode)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(prefix)
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	migration := migrator.CreateMigrationFromSQL(
		1,
		"unsafe initial sql mode",
		fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.createdTable),
		fmt.Sprintf("DROP TABLE %s", names.createdTable),
	)
	err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `.*tx-mode file cannot validate MySQL-family statement boundaries while session sql_mode contains parser-changing mode `+wantMode+`; use tx-mode none`)
	c.Assert(issue887TableCount(t, conn, names.createdTable), qt.Equals, int64(0))
	c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
}

func runRejectsANSIQuotesSQLModeChange(t *testing.T, dbURL, prefix string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(prefix)
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	migration := migrator.CreateMigrationFromSQL(
		1,
		"create ANSI-quoted target engine",
		fmt.Sprintf(
			`SET SESSION sql_mode = 'ANSI_QUOTES'; CREATE TABLE "%s" (id INTEGER PRIMARY KEY) ENGINE=MyISAM`,
			names.createdTable,
		),
		fmt.Sprintf(`DROP TABLE "%s"`, names.createdTable),
	)
	err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `.*migration 1 cannot run tx-mode file statement 1 because changing sql_mode can make the MySQL-family server disagree with Ptah's prevalidated statement boundaries; configure a stable session mode before migration or use tx-mode none`)
	c.Assert(issue887TableCount(t, conn, names.createdTable), qt.Equals, int64(0))
	c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
}

func runRejectsMSSQLSQLModeChange(t *testing.T, dbURL, prefix string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL+"?multiStatements=true")
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(prefix)
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	migration := migrator.CreateMigrationFromSQL(
		1,
		"MSSQL-quoted target engine",
		fmt.Sprintf(
			"SET SESSION sql_mode = 'MSSQL'; CREATE TABLE [%s] (id INTEGER PRIMARY KEY) ENGINE=MyISAM",
			names.createdTable,
		),
		fmt.Sprintf("DROP TABLE [%s]", names.createdTable),
	)
	err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `.*migration 1 cannot run tx-mode file statement 1 because changing sql_mode can make the MySQL-family server disagree with Ptah's prevalidated statement boundaries; configure a stable session mode before migration or use tx-mode none`)
	c.Assert(issue887TableCount(t, conn, names.createdTable), qt.Equals, int64(0))
	c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
}

func runIsolatesTemporaryTablesBetweenAttempts(t *testing.T, dbURL, prefix string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(prefix)
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)
	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY) ENGINE=InnoDB",
		names.ledgerTable,
	))
	c.Assert(err, qt.IsNil)

	failingMigration := migrator.CreateMigrationFromSQL(
		1,
		"fail after writing a temporary table",
		fmt.Sprintf(
			"SET SESSION default_tmp_storage_engine = MyISAM; "+
				"CREATE TEMPORARY TABLE %s (id INTEGER PRIMARY KEY); "+
				"INSERT INTO %s VALUES (1); "+
				"INSERT INTO %s SELECT id FROM %s; "+
				"INSERT INTO %s VALUES (1)",
			names.createdTable,
			names.createdTable,
			names.ledgerTable,
			names.createdTable,
			names.createdTable,
		),
		fmt.Sprintf("DELETE FROM %s", names.ledgerTable),
	)
	err = issue887Migrator(conn, names, failingMigration).MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `(?s).*Duplicate entry '1'.*`)
	c.Assert(issue887LedgerCount(t, conn, names), qt.Equals, int64(0))
	c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(1))
	c.Assert(issue887AppliedCount(t, conn, names), qt.Equals, int64(0))

	retryMigration := migrator.CreateMigrationFromSQL(
		1,
		"retry on a fresh session",
		fmt.Sprintf(
			"SET SESSION default_tmp_storage_engine = MyISAM; "+
				"CREATE TEMPORARY TABLE %s (id INTEGER PRIMARY KEY); "+
				"INSERT INTO %s VALUES (1); "+
				"INSERT INTO %s SELECT id FROM %s",
			names.createdTable,
			names.createdTable,
			names.ledgerTable,
			names.createdTable,
		),
		fmt.Sprintf("DELETE FROM %s", names.ledgerTable),
	)
	err = issue887Migrator(conn, names, retryMigration).MigrateUpWithOptions(
		ctx,
		migrator.MigrateUpOptions{AllowDirty: true},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(issue887LedgerCount(t, conn, names), qt.Equals, int64(1))
	c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(1))
}

func runRejectsTemporaryMetadataShadow(t *testing.T, dbURL, prefix string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	runRejectsTemporaryMetadataShadowCase(
		t,
		conn,
		issue887Names(prefix+"_native"),
		migrator.RevisionTableFormatPtah,
		issue887NativeMetadataShadowSQL,
	)
	runRejectsTemporaryMetadataShadowCase(
		t,
		conn,
		issue887Names(prefix+"_atlas"),
		migrator.RevisionTableFormatAtlas,
		issue887AtlasMetadataShadowSQL,
	)
}

func runRejectsTemporaryMetadataShadowCase(
	t *testing.T,
	conn *dbschema.DatabaseConnection,
	names issue887TestNames,
	format migrator.RevisionTableFormat,
	shadowSQL func(string, issue887TestNames) string,
) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	migration := migrator.CreateMigrationFromSQL(
		1,
		"attempt to shadow migration metadata",
		shadowSQL(conn.Info().Schema, names),
		fmt.Sprintf("DROP TABLE %s", names.createdTable),
	)
	err := issue887MigratorWithFormat(conn, names, format, migration).MigrateUp(ctx)
	c.Assert(
		err,
		qt.ErrorMatches,
		`.*migration 1 cannot run tx-mode file statement 1 because it references Ptah's migration metadata table .*which is reserved for transaction-witness bookkeeping; choose another relation name`,
	)
	c.Assert(issue887TableCount(t, conn, names.createdTable), qt.Equals, int64(0))
	c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
}

func runRejectsStaleTemporaryMetadataShadowBeforeDirtyCheck(t *testing.T, dbURL, prefix string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(prefix)
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	migration := migrator.CreateMigrationFromSQL(
		1,
		"would read fake dirty metadata",
		fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY) ENGINE=InnoDB", names.createdTable),
		fmt.Sprintf("DROP TABLE %s", names.createdTable),
	)
	mig := issue887MigratorWithFormat(conn, names, migrator.RevisionTableFormatPtah, migration).WithoutMigrationLock()
	c.Assert(mig.Initialize(ctx), qt.IsNil)

	err = conn.WithSession(ctx, func(scoped *dbschema.DatabaseConnection) error {
		if _, execErr := scoped.ExecContext(ctx, issue887NativeTemporaryMetadataShadowTableSQL(names)); execErr != nil {
			return execErr
		}
		if _, execErr := scoped.ExecContext(ctx, issue887NativeTemporaryDirtyMetadataShadowInsertSQL(names)); execErr != nil {
			return execErr
		}
		return issue887MigratorWithFormat(
			scoped,
			names,
			migrator.RevisionTableFormatPtah,
			migration,
		).WithoutMigrationLock().MigrateUp(ctx)
	})
	c.Assert(
		err,
		qt.ErrorMatches,
		`.*pinned MySQL-family session contains temporary table .* that shadows Ptah's migration metadata; retry on a clean session.*`,
	)
	c.Assert(issue887TableCount(t, conn, names.createdTable), qt.Equals, int64(0))
	c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
}

func issue887NativeMetadataShadowSQL(_ string, names issue887TestNames) string {
	return fmt.Sprintf(`CREATE TEMPORARY TABLE %s (
version BIGINT PRIMARY KEY,
description TEXT NOT NULL,
applied_at TIMESTAMP NOT NULL,
state VARCHAR(32) NOT NULL,
applied INTEGER NOT NULL,
total INTEGER NOT NULL,
error TEXT NULL,
error_stmt TEXT NULL,
execution_time_ms BIGINT NOT NULL,
checksum VARCHAR(64) NOT NULL
) ENGINE=InnoDB;
INSERT INTO %s VALUES (1, 'shadow', CURRENT_TIMESTAMP, 'pending', 0, 3, NULL, NULL, 0, '');
CREATE TABLE %s (id INTEGER PRIMARY KEY) ENGINE=InnoDB`,
		names.revisionsTable,
		names.revisionsTable,
		names.createdTable,
	)
}

func issue887AtlasMetadataShadowSQL(schema string, names issue887TestNames) string {
	qualifiedMetadata := schema + "." + names.revisionsTable
	return fmt.Sprintf(`CREATE TEMPORARY TABLE %s (
version VARCHAR(255) PRIMARY KEY,
description TEXT NOT NULL,
type BIGINT NOT NULL,
applied BIGINT NOT NULL,
total BIGINT NOT NULL,
executed_at TIMESTAMP NOT NULL,
execution_time BIGINT NOT NULL,
error TEXT NULL,
error_stmt TEXT NULL,
hash VARCHAR(255) NOT NULL,
partial_hashes JSON NULL,
operator_version VARCHAR(255) NOT NULL
) ENGINE=InnoDB;
INSERT INTO %s VALUES ('1', 'shadow', 2, 0, 3, CURRENT_TIMESTAMP, 0, NULL, NULL, '', JSON_ARRAY(), 'ptah');
CREATE TABLE %s (id INTEGER PRIMARY KEY) ENGINE=InnoDB`,
		qualifiedMetadata,
		qualifiedMetadata,
		names.createdTable,
	)
}

func runRejectsStaleTemporaryMetadataShadowBeforePlanning(t *testing.T, dbURL, prefix string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(prefix)
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	migration := migrator.CreateMigrationFromSQL(
		1,
		"would create target after fake applied metadata",
		fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY) ENGINE=InnoDB", names.createdTable),
		fmt.Sprintf("DROP TABLE %s", names.createdTable),
	)
	mig := issue887MigratorWithFormat(conn, names, migrator.RevisionTableFormatPtah, migration).WithoutMigrationLock()
	c.Assert(mig.Initialize(ctx), qt.IsNil)

	err = conn.WithSession(ctx, func(scoped *dbschema.DatabaseConnection) error {
		if _, execErr := scoped.ExecContext(ctx, issue887NativeTemporaryMetadataShadowTableSQL(names)); execErr != nil {
			return execErr
		}
		if _, execErr := scoped.ExecContext(ctx, issue887NativeTemporaryAppliedMetadataShadowInsertSQL(names)); execErr != nil {
			return execErr
		}
		return issue887MigratorWithFormat(
			scoped,
			names,
			migrator.RevisionTableFormatPtah,
			migration,
		).WithoutMigrationLock().MigrateUp(ctx)
	})
	c.Assert(
		err,
		qt.ErrorMatches,
		`.*pinned MySQL-family session contains temporary table .* that shadows Ptah's migration metadata; retry on a clean session.*`,
	)
	c.Assert(issue887TableCount(t, conn, names.createdTable), qt.Equals, int64(0))
	c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
}

func issue887NativeTemporaryMetadataShadowTableSQL(names issue887TestNames) string {
	return fmt.Sprintf(`CREATE TEMPORARY TABLE %s (
version BIGINT PRIMARY KEY,
description TEXT NOT NULL,
applied_at TIMESTAMP NOT NULL,
state VARCHAR(32) NOT NULL,
applied INTEGER NOT NULL,
total INTEGER NOT NULL,
error TEXT NULL,
error_stmt TEXT NULL,
execution_time_ms BIGINT NOT NULL,
checksum VARCHAR(64) NOT NULL
) ENGINE=InnoDB`,
		names.revisionsTable,
	)
}

func issue887NativeTemporaryAppliedMetadataShadowInsertSQL(names issue887TestNames) string {
	return fmt.Sprintf(
		"INSERT INTO %s VALUES (1, 'stale shadow', CURRENT_TIMESTAMP, 'applied', 1, 1, NULL, NULL, 0, '')",
		names.revisionsTable,
	)
}

func issue887NativeTemporaryDirtyMetadataShadowInsertSQL(names issue887TestNames) string {
	return fmt.Sprintf(
		"INSERT INTO %s VALUES (1, 'dirty shadow', CURRENT_TIMESTAMP, 'pending', 0, 1, 'fake', '', 0, '')",
		names.revisionsTable,
	)
}

func runRejectsInheritedStorageEngine(t *testing.T, dbURL, prefix string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer issue887CloseConnection(t, conn)

	names := issue887Names(prefix)
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	migration := migrator.CreateMigrationFromSQL(
		1,
		"inherited target engine",
		fmt.Sprintf("CREATE TABLE %s LIKE %s", names.createdTable, names.blockerTable),
		fmt.Sprintf("DROP TABLE %s", names.createdTable),
	)
	mig := issue887Migrator(conn, names, migration)
	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `.*migration 1 cannot run tx-mode file statement 1 because CREATE TABLE LIKE inherits a storage engine that Ptah cannot prove is InnoDB; declare the table explicitly or use tx-mode none`)
	c.Assert(issue887TableCount(t, conn, names.createdTable), qt.Equals, int64(0))
	c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
}

type issue887TestNames struct {
	revisionsTable string
	ledgerTable    string
	blockerTable   string
	createdTable   string
}

func issue887Names(prefix string) issue887TestNames {
	suffix := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	return issue887TestNames{
		revisionsTable: "atlas_revisions_887_" + suffix,
		ledgerTable:    "ptah_887_ledger_" + suffix,
		blockerTable:   "ptah_887_blocker_" + suffix,
		createdTable:   "ptah_887_created_" + suffix,
	}
}

func issue887ReplaceMySQLCredentials(t *testing.T, rawURL, username, password string) string {
	c := qt.New(t)
	t.Helper()

	scheme, remainder, found := strings.Cut(rawURL, "://")
	c.Assert(found, qt.IsTrue)
	_, endpoint, found := strings.Cut(remainder, "@")
	c.Assert(found, qt.IsTrue)
	return fmt.Sprintf("%s://%s:%s@%s", scheme, username, password, endpoint)
}

func issue887ReplaceMySQLDatabase(t *testing.T, rawURL, database string) string {
	t.Helper()
	return mySQLFamilyURLWithDatabase(t, rawURL, database)
}

func issue887Migrator(
	conn *dbschema.DatabaseConnection,
	names issue887TestNames,
	migrations ...*migrator.Migration,
) *migrator.Migrator {
	return issue887MigratorWithFormat(conn, names, migrator.RevisionTableFormatAtlas, migrations...)
}

func issue887MigratorWithFormat(
	conn *dbschema.DatabaseConnection,
	names issue887TestNames,
	format migrator.RevisionTableFormat,
	migrations ...*migrator.Migration,
) *migrator.Migrator {
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migrations...)).
		WithMigrationsTable("", names.revisionsTable).
		WithRevisionTableFormat(format).
		WithTransactionMode(migrator.MigrationTxModeFile).
		WithMigrationLockTimeout(10 * time.Second)
}

func issue887LedgerCount(t *testing.T, conn *dbschema.DatabaseConnection, names issue887TestNames) int64 {
	t.Helper()

	return issue887ScalarCount(t, conn, fmt.Sprintf("SELECT COUNT(*) FROM %s", names.ledgerTable))
}

func issue887RevisionCount(t *testing.T, conn *dbschema.DatabaseConnection, names issue887TestNames) int64 {
	t.Helper()

	return issue887ScalarCount(t, conn, fmt.Sprintf("SELECT COUNT(*) FROM %s", names.revisionsTable))
}

func issue887AppliedCount(t *testing.T, conn *dbschema.DatabaseConnection, names issue887TestNames) int64 {
	t.Helper()

	return issue887ScalarCount(t, conn, fmt.Sprintf(
		"SELECT applied FROM %s WHERE version = '1'", names.revisionsTable,
	))
}

func issue887RevisionError(t *testing.T, conn *dbschema.DatabaseConnection, names issue887TestNames) string {
	c := qt.New(t)
	t.Helper()

	var failure string
	err := conn.QueryRowContext(
		context.Background(),
		fmt.Sprintf("SELECT error FROM %s WHERE version = '1'", names.revisionsTable),
	).Scan(&failure)
	c.Assert(err, qt.IsNil)
	return failure
}

func issue887MetadataEngine(t *testing.T, conn *dbschema.DatabaseConnection, names issue887TestNames) string {
	t.Helper()
	return issue887TableEngine(t, conn, names.revisionsTable)
}

func issue887TableEngine(t *testing.T, conn *dbschema.DatabaseConnection, table string) string {
	c := qt.New(t)
	t.Helper()

	var engine string
	err := conn.QueryRowContext(
		context.Background(),
		"SELECT engine FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		conn.Info().Schema,
		table,
	).Scan(&engine)
	c.Assert(err, qt.IsNil)
	return engine
}

func issue887TableCount(t *testing.T, conn *dbschema.DatabaseConnection, table string) int64 {
	c := qt.New(t)
	t.Helper()

	var count int64
	err := conn.QueryRowContext(
		context.Background(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		conn.Info().Schema,
		table,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func issue887ColumnCount(
	t *testing.T,
	conn *dbschema.DatabaseConnection,
	table,
	column string,
) int64 {
	c := qt.New(t)
	t.Helper()

	var count int64
	err := conn.QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM information_schema.columns
WHERE table_schema = ? AND table_name = ? AND column_name = ?`,
		conn.Info().Schema,
		table,
		column,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func issue887ScalarCount(t *testing.T, conn *dbschema.DatabaseConnection, query string) int64 {
	c := qt.New(t)
	t.Helper()

	var count int64
	err := conn.QueryRowContext(context.Background(), query).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func issue887LedgerNotes(t *testing.T, conn *dbschema.DatabaseConnection, names issue887TestNames) []string {
	c := qt.New(t)
	t.Helper()

	rows, err := conn.QueryContext(
		context.Background(),
		fmt.Sprintf("SELECT note FROM %s ORDER BY note", names.ledgerTable),
	)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()

	notes := []string{}
	for rows.Next() {
		var note string
		c.Assert(rows.Scan(&note), qt.IsNil)
		notes = append(notes, note)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return notes
}

func issue887CreateUser(t *testing.T, conn *dbschema.DatabaseConnection) (username, password string) {
	c := qt.New(t)
	t.Helper()

	username = fmt.Sprintf("p887_%d", time.Now().UnixNano())
	password = username + "_password"
	_, err := conn.ExecContext(context.Background(), fmt.Sprintf(
		"CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", username, password,
	))
	c.Assert(err, qt.IsNil)
	return username, password
}

func issue887GrantBasePrivileges(
	t *testing.T,
	conn *dbschema.DatabaseConnection,
	dialect,
	username string,
) {
	c := qt.New(t)
	t.Helper()

	_, err := conn.ExecContext(context.Background(), fmt.Sprintf(
		"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX ON %s.* TO '%s'@'%%'",
		sqlident.Quote(dialect, conn.Info().Schema),
		username,
	))
	c.Assert(err, qt.IsNil)
}

func issue887IndexCount(
	t *testing.T,
	conn *dbschema.DatabaseConnection,
	schema,
	table,
	index string,
) int64 {
	c := qt.New(t)
	t.Helper()

	var count int64
	err := conn.QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM information_schema.statistics
WHERE table_schema = ? AND table_name = ? AND index_name = ?`,
		schema,
		table,
		index,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func issue887EventCount(t *testing.T, conn *dbschema.DatabaseConnection, event string) int64 {
	c := qt.New(t)
	t.Helper()

	var count int64
	err := conn.QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM information_schema.events WHERE event_schema = ? AND event_name = ?`,
		conn.Info().Schema,
		event,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func issue887FileMissing(t *testing.T, conn *dbschema.DatabaseConnection, path string) bool {
	c := qt.New(t)
	t.Helper()

	var missing bool
	err := conn.QueryRowContext(context.Background(), "SELECT LOAD_FILE(?) IS NULL", path).Scan(&missing)
	c.Assert(err, qt.IsNil)
	return missing
}

func issue887TableCountInSchema(
	t *testing.T,
	conn *dbschema.DatabaseConnection,
	schema,
	table string,
) int64 {
	c := qt.New(t)
	t.Helper()

	var count int64
	err := conn.QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?`,
		schema,
		table,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func issue887SchemaPrivilegeCount(
	t *testing.T,
	conn *dbschema.DatabaseConnection,
	schema,
	username,
	privilege string,
) int64 {
	c := qt.New(t)
	t.Helper()

	var count int64
	err := conn.QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM information_schema.schema_privileges
WHERE table_schema = ? AND grantee = ? AND privilege_type = ?`,
		schema,
		fmt.Sprintf("'%s'@'%%'", username),
		privilege,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func issue887RoutinePrivilegeCount(
	t *testing.T,
	conn *dbschema.DatabaseConnection,
	schema,
	routine,
	username,
	privilege string,
) int64 {
	c := qt.New(t)
	t.Helper()

	var count int64
	err := conn.QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM mysql.procs_priv
WHERE Db = ? AND Routine_name = ? AND User = ? AND FIND_IN_SET(?, Proc_priv) > 0`,
		schema,
		routine,
		username,
		privilege,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func issue887CloseConnection(t *testing.T, conn *dbschema.DatabaseConnection) {
	c := qt.New(t)
	t.Helper()
	c.Check(conn.Close(), qt.IsNil)
}

func issue887DropDatabase(t *testing.T, conn *dbschema.DatabaseConnection, database string) {
	t.Helper()
	issue887CleanupSQL(t, conn, "DROP DATABASE IF EXISTS "+database)
}

func issue887DropTrigger(t *testing.T, conn *dbschema.DatabaseConnection, trigger string) {
	t.Helper()
	issue887CleanupSQL(t, conn, "DROP TRIGGER IF EXISTS "+trigger)
}

func issue887DropFunction(t *testing.T, conn *dbschema.DatabaseConnection, function string) {
	t.Helper()
	issue887CleanupSQL(t, conn, "DROP FUNCTION IF EXISTS "+function)
}

func issue887DropEvent(t *testing.T, conn *dbschema.DatabaseConnection, event string) {
	t.Helper()
	issue887CleanupSQL(t, conn, "DROP EVENT IF EXISTS "+event)
}

func issue887DropUser(t *testing.T, conn *dbschema.DatabaseConnection, username string) {
	t.Helper()
	issue887CleanupSQL(t, conn, fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", username))
}

func issue887DropRole(t *testing.T, conn *dbschema.DatabaseConnection, role string) {
	t.Helper()
	issue887CleanupSQL(t, conn, fmt.Sprintf("DROP ROLE IF EXISTS '%s'@'%%'", role))
}

func issue887CleanupSQL(t *testing.T, conn *dbschema.DatabaseConnection, query string) {
	c := qt.New(t)
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := conn.ExecContext(ctx, query)
	c.Check(err, qt.IsNil, qt.Commentf("cleanup query: %s", query))
}

func cleanupIssue887(t *testing.T, conn *dbschema.DatabaseConnection, names issue887TestNames) {
	t.Helper()

	for _, table := range []string{names.revisionsTable, names.blockerTable, names.createdTable, names.ledgerTable} {
		issue887CleanupSQL(t, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	}
}
