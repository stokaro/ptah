package migrator_test

import (
	"context"
	"fmt"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
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
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runRolledBackDataOnlyBody(t, dbURL, "mysql")
}

func TestRolledBackProgress_MariaDBDataOnlyBodyResumesFromTheTop(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runRolledBackDataOnlyBody(t, dbURL, "mariadb")
}

// TestRolledBackProgress_MySQLFailingDDLKeepsUnknownOutcome verifies that a
// witness committed before a failing DDL statement is not downgraded to an
// ordinary failure. The prefix is durable, but the statement outcome remains
// unknown and automatic retry must stop before it can repeat user SQL.
func TestRolledBackProgress_MySQLFailingDDLKeepsUnknownOutcome(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runRolledBackCommittedDDLPrefix(t, dbURL, "mysql")
}

func TestRolledBackProgress_MariaDBFailingDDLKeepsUnknownOutcome(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runRolledBackCommittedDDLPrefix(t, dbURL, "mariadb")
}

func TestRolledBackProgress_MySQLDDLThenDMLKeepsTheWholeDurablePrefix(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runRolledBackDDLThenDML(t, dbURL, "mysql", migrator.RevisionTableFormatAtlas)
}

func TestRolledBackProgress_MariaDBDDLThenDMLKeepsTheWholeDurablePrefix(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runRolledBackDDLThenDML(t, dbURL, "mariadb", migrator.RevisionTableFormatAtlas)
}

func TestRolledBackProgress_MySQLNativeRevisionKeepsTheWholeDurablePrefix(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runRolledBackDDLThenDML(t, dbURL, "mysql_native", migrator.RevisionTableFormatPtah)
}

func TestRolledBackProgress_MariaDBNativeRevisionKeepsTheWholeDurablePrefix(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runRolledBackDDLThenDML(t, dbURL, "mariadb_native", migrator.RevisionTableFormatPtah)
}

func TestRolledBackProgress_MySQLRejectsAutocommitControl(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runRejectsAutocommitControl(t, dbURL, "mysql")
}

func TestRolledBackProgress_MariaDBRejectsAutocommitControl(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runRejectsAutocommitControl(t, dbURL, "mariadb")
}

func TestRolledBackProgress_MySQLRejectsChangingTargetDatabase(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runRejectsChangingTargetDatabase(t, dbURL, "mysql")
}

func TestRolledBackProgress_MariaDBRejectsChangingTargetDatabase(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runRejectsChangingTargetDatabase(t, dbURL, "mariadb")
}

func TestRolledBackProgress_MySQLReplaysCommittedSessionState(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runRolledBackSessionStateReplay(t, dbURL, "mysql")
}

func TestRolledBackProgress_MariaDBReplaysCommittedSessionState(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runRolledBackSessionStateReplay(t, dbURL, "mariadb")
}

func TestRolledBackProgress_MySQLAtlasDownRecordsTheDurablePrefix(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runRolledBackDownProgress(t, dbURL, "mysql_down_atlas", migrator.RevisionTableFormatAtlas)
}

func TestRolledBackProgress_MariaDBAtlasDownRecordsTheDurablePrefix(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runRolledBackDownProgress(t, dbURL, "mariadb_down_atlas", migrator.RevisionTableFormatAtlas)
}

func TestRolledBackProgress_MySQLNativeDownRecordsTheDurablePrefix(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runRolledBackDownProgress(t, dbURL, "mysql_down_native", migrator.RevisionTableFormatPtah)
}

func TestRolledBackProgress_MariaDBNativeDownRecordsTheDurablePrefix(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runRolledBackDownProgress(t, dbURL, "mariadb_down_native", migrator.RevisionTableFormatPtah)
}

func TestRolledBackProgress_MySQLRejectsNonTransactionalMetadata(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runRejectsNonTransactionalMetadata(t, dbURL, "mysql_myisam")
}

func TestRolledBackProgress_MariaDBRejectsNonTransactionalMetadata(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runRejectsNonTransactionalMetadata(t, dbURL, "mariadb_myisam")
}

func TestRolledBackProgress_MySQLRejectsNonTransactionalNativeMetadataBeforeUpgrade(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runRejectsNonTransactionalNativeMetadataBeforeUpgrade(t, dbURL, "mysql_native_myisam")
}

func TestRolledBackProgress_MariaDBRejectsNonTransactionalNativeMetadataBeforeUpgrade(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runRejectsNonTransactionalNativeMetadataBeforeUpgrade(t, dbURL, "mariadb_native_myisam")
}

func TestRolledBackProgress_MySQLRejectsNonTransactionalTargetTable(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runRejectsNonTransactionalTargetTable(t, dbURL, "mysql_target_myisam")
}

func TestRolledBackProgress_MariaDBRejectsNonTransactionalTargetTable(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runRejectsNonTransactionalTargetTable(t, dbURL, "mariadb_target_myisam")
}

func TestRolledBackProgress_MySQLRejectsCreatingNonTransactionalTargetTable(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runRejectsCreatingNonTransactionalTargetTable(t, dbURL, "mysql_create_myisam")
}

func TestRolledBackProgress_MariaDBRejectsCreatingNonTransactionalTargetTable(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runRejectsCreatingNonTransactionalTargetTable(t, dbURL, "mariadb_create_myisam")
}

func TestRolledBackProgress_MySQLRejectsInheritedStorageEngine(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runRejectsInheritedStorageEngine(t, dbURL, "mysql_create_like")
}

func TestRolledBackProgress_MariaDBRejectsInheritedStorageEngine(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runRejectsInheritedStorageEngine(t, dbURL, "mariadb_create_like")
}

func TestRolledBackProgress_MySQLRejectsUnwitnessedExecutionBoundaries(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runRejectsUnwitnessedExecutionBoundaries(t, dbURL, "mysql")
}

func TestRolledBackProgress_MariaDBRejectsUnwitnessedExecutionBoundaries(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runRejectsUnwitnessedExecutionBoundaries(t, dbURL, "mariadb")
}

func runRejectsUnwitnessedExecutionBoundaries(t *testing.T, dbURL, dialect string) {
	t.Helper()

	t.Run("executable comment", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer func() { _ = conn.Close() }()

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
		defer func() { _ = conn.Close() }()

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
		defer func() { _ = conn.Close() }()

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
		defer func() { _ = conn.Close() }()

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
		c.Assert(err, qt.ErrorMatches, `.*cannot run an up statement interceptor in tx-mode file.*`)
		c.Assert(interceptor.called, qt.IsFalse)
		c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
	})

	t.Run("nested SQL", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer func() { _ = conn.Close() }()

		names := issue887Names(dialect + "_call")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		migration := migrator.CreateMigrationFromSQL(1, "nested SQL", "CALL missing_procedure()", "SELECT 1")

		err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*nested or dynamic SQL cannot be tied to Ptah's transaction witness.*`)
		c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
	})

	t.Run("cross-database table", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer func() { _ = conn.Close() }()

		names := issue887Names(dialect + "_cross_db")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		externalDatabase := fmt.Sprintf("ptah887external%d", time.Now().UnixNano())
		_, err = conn.ExecContext(ctx, "CREATE DATABASE "+externalDatabase)
		c.Assert(err, qt.IsNil)
		defer func() { _, _ = conn.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+externalDatabase) }()
		_, err = conn.ExecContext(ctx, fmt.Sprintf(
			"CREATE TABLE %s.external_jobs (id INTEGER PRIMARY KEY) ENGINE=MyISAM", externalDatabase,
		))
		c.Assert(err, qt.IsNil)
		migration := migrator.CreateMigrationFromSQL(
			1,
			"cross database",
			fmt.Sprintf("INSERT INTO %s.external_jobs VALUES (1)", externalDatabase),
			fmt.Sprintf("DELETE FROM %s.external_jobs", externalDatabase),
		)

		err = issue887Migrator(conn, names, migration).MigrateUp(ctx)
		c.Assert(err, qt.ErrorMatches, `.*references database .* outside the selected database.*`)
		c.Assert(
			issue887ScalarCount(t, conn, fmt.Sprintf("SELECT COUNT(*) FROM %s.external_jobs", externalDatabase)),
			qt.Equals,
			int64(0),
		)
		c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
	})

	t.Run("database creation", func(t *testing.T) {
		c := qt.New(t)
		ctx := context.Background()
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		c.Assert(err, qt.IsNil)
		defer func() { _ = conn.Close() }()

		names := issue887Names(dialect + "_create_db")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		databaseName := fmt.Sprintf("ptah887created%d", time.Now().UnixNano())
		defer func() { _, _ = conn.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+databaseName) }()
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
				conn,
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
		defer func() { _ = conn.Close() }()

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
		_, err = conn.ExecContext(ctx, fmt.Sprintf(
			"CREATE TRIGGER %s AFTER INSERT ON %s FOR EACH ROW INSERT INTO %s (id) VALUES (NEW.id)",
			triggerName,
			names.ledgerTable,
			names.blockerTable,
		))
		c.Assert(err, qt.IsNil)
		defer func() { _, _ = conn.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+triggerName) }()
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
		defer func() { _ = conn.Close() }()

		names := issue887Names(dialect + "_routine")
		cleanupIssue887(t, conn, names)
		defer cleanupIssue887(t, conn, names)
		routineName := fmt.Sprintf("ptah887routine%d", time.Now().UnixNano())
		_, err = conn.ExecContext(ctx, fmt.Sprintf(
			"CREATE TABLE %s (id INTEGER PRIMARY KEY, note VARCHAR(64))", names.ledgerTable,
		))
		c.Assert(err, qt.IsNil)
		_, err = conn.ExecContext(ctx, fmt.Sprintf(
			"CREATE FUNCTION %s() RETURNS INT DETERMINISTIC RETURN 7", routineName,
		))
		c.Assert(err, qt.IsNil)
		defer func() { _, _ = conn.ExecContext(context.Background(), "DROP FUNCTION IF EXISTS "+routineName) }()
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
		defer func() { _ = conn.Close() }()

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

func runRolledBackDataOnlyBody(t *testing.T, dbURL, dialect string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

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
	defer func() { _ = conn.Close() }()

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
	defer func() { _ = conn.Close() }()

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

func runRejectsAutocommitControl(t *testing.T, dbURL, dialect string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	names := issue887Names(dialect + "_autocommit_zero")
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY, note VARCHAR(64))", names.ledgerTable,
	))
	c.Assert(err, qt.IsNil)

	body := fmt.Sprintf(
		"CREATE TABLE %[1]s (id INTEGER PRIMARY KEY);\n"+
			"SET autocommit = 0;\n"+
			"INSERT INTO %[2]s (id, note) VALUES (1, 'one');\n",
		names.createdTable, names.ledgerTable,
	)
	migration := migrator.CreateMigrationFromSQL(1, "autocommit zero", body,
		fmt.Sprintf("DROP TABLE %s", names.createdTable))

	mig := issue887Migrator(conn, names, migration)
	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `.*migration 1 cannot run tx-mode file statement 2 because it controls transaction state; remove the transaction control and let Ptah manage the file transaction`)
	c.Assert(issue887LedgerCount(t, conn, names), qt.Equals, int64(0))
	c.Assert(issue887TableCount(t, conn, names.createdTable), qt.Equals, int64(0))
	c.Assert(issue887RevisionCount(t, conn, names), qt.Equals, int64(0))
}

func runRejectsChangingTargetDatabase(t *testing.T, dbURL, dialect string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

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
	defer func() { _ = conn.Close() }()

	names := issue887Names(dialect + "_session_replay")
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	body := fmt.Sprintf(
		"SET SESSION sql_mode = 'ANSI_QUOTES';\n"+
			"CREATE TABLE %[1]s (id INTEGER PRIMARY KEY);\n"+
			"INSERT INTO \"%[1]s\" (id) VALUES (1);\n"+
			"INSERT INTO \"%[2]s\" (id) VALUES (7);\n",
		names.createdTable, names.blockerTable,
	)
	migration := migrator.CreateMigrationFromSQL(1, "session replay", body,
		fmt.Sprintf("DROP TABLE %s", names.createdTable))

	failing := issue887Migrator(conn, names, migration)
	err = failing.MigrateUp(ctx)
	c.Assert(err, qt.IsNotNil)
	c.Assert(issue887AppliedCount(t, conn, names), qt.Equals, int64(3))

	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER PRIMARY KEY)", names.blockerTable,
	))
	c.Assert(err, qt.IsNil)

	retried := issue887Migrator(conn, names, migration)
	err = retried.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{AllowDirty: true})
	c.Assert(err, qt.IsNil)
	c.Assert(issue887ScalarCount(t, conn, fmt.Sprintf("SELECT COUNT(*) FROM %s", names.createdTable)), qt.Equals, int64(1))
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
	defer func() { _ = conn.Close() }()

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
	defer func() { _ = conn.Close() }()

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
	defer func() { _ = conn.Close() }()

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
	defer func() { _ = conn.Close() }()

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
	defer func() { _ = conn.Close() }()

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

func runRejectsInheritedStorageEngine(t *testing.T, dbURL, prefix string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

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
	t.Helper()

	var failure string
	err := conn.QueryRowContext(
		context.Background(),
		fmt.Sprintf("SELECT error FROM %s WHERE version = '1'", names.revisionsTable),
	).Scan(&failure)
	qt.Assert(t, err, qt.IsNil)
	return failure
}

func issue887MetadataEngine(t *testing.T, conn *dbschema.DatabaseConnection, names issue887TestNames) string {
	t.Helper()

	var engine string
	err := conn.QueryRowContext(
		context.Background(),
		"SELECT engine FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		conn.Info().Schema,
		names.revisionsTable,
	).Scan(&engine)
	qt.Assert(t, err, qt.IsNil)
	return engine
}

func issue887TableCount(t *testing.T, conn *dbschema.DatabaseConnection, table string) int64 {
	t.Helper()

	var count int64
	err := conn.QueryRowContext(
		context.Background(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		conn.Info().Schema,
		table,
	).Scan(&count)
	qt.Assert(t, err, qt.IsNil)
	return count
}

func issue887ColumnCount(
	t *testing.T,
	conn *dbschema.DatabaseConnection,
	table,
	column string,
) int64 {
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
	qt.Assert(t, err, qt.IsNil)
	return count
}

func issue887ScalarCount(t *testing.T, conn *dbschema.DatabaseConnection, query string) int64 {
	t.Helper()

	var count int64
	err := conn.QueryRowContext(context.Background(), query).Scan(&count)
	qt.Assert(t, err, qt.IsNil)
	return count
}

func issue887LedgerNotes(t *testing.T, conn *dbschema.DatabaseConnection, names issue887TestNames) []string {
	t.Helper()

	rows, err := conn.QueryContext(
		context.Background(),
		fmt.Sprintf("SELECT note FROM %s ORDER BY note", names.ledgerTable),
	)
	qt.Assert(t, err, qt.IsNil)
	defer func() { _ = rows.Close() }()

	notes := []string{}
	for rows.Next() {
		var note string
		qt.Assert(t, rows.Scan(&note), qt.IsNil)
		notes = append(notes, note)
	}
	qt.Assert(t, rows.Err(), qt.IsNil)
	return notes
}

func cleanupIssue887(t *testing.T, conn *dbschema.DatabaseConnection, names issue887TestNames) {
	t.Helper()

	for _, table := range []string{names.revisionsTable, names.blockerTable, names.createdTable, names.ledgerTable} {
		_, _ = conn.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	}
}
