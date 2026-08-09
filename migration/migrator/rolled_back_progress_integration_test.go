package migrator_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

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

// TestRolledBackProgress_MySQLCommittedDDLPrefixIsKept is the non-interference
// control for the test above. MySQL commits the transaction before it runs a
// DDL statement, so the statements ahead of a failing CREATE TABLE really are
// durable; forgetting that would make the retry repeat them.
func TestRolledBackProgress_MySQLCommittedDDLPrefixIsKept(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runRolledBackCommittedDDLPrefix(t, dbURL, "mysql")
}

// TestRolledBackProgress_MySQLDDLCarriesTheStatementsAfterIt is the guard for
// the first blocker on stokaro/ptah#1356.
//
// An implicit commit does not flush the open transaction, it ENDS it. Every
// statement after a DDL statement therefore runs with no transaction open and
// commits itself, and the ROLLBACK the failure triggers reaches none of them.
// Measured directly on both servers:
//
//	START TRANSACTION; INSERT INTO led VALUES (1,'one'); CREATE TABLE ddl1 (i INT);
//	INSERT INTO led VALUES (3,'three'); ROLLBACK; SELECT id,note FROM led ORDER BY id;
//	-> rows 1 and 3 both survive
//
// Counting the prefix only up to the DDL statement recorded applied=1 for the
// body below, where two statements had committed. The retry then resumed at
// statement two and inserted its row a second time, and the migration reported
// success. The ledger has no primary key on purpose: a repeat has to show up as
// a duplicated row rather than as an error, because that is how it shows up in
// production.
func TestRolledBackProgress_MySQLDDLCarriesTheStatementsAfterIt(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runRolledBackDDLCarriesTheRest(t, dbURL, "mysql")
}

func TestRolledBackProgress_MariaDBDDLCarriesTheStatementsAfterIt(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runRolledBackDDLCarriesTheRest(t, dbURL, "mariadb")
}

// TestRolledBackProgress_MySQLNonCommittingStatementKeepsNothing is the guard
// for the second blocker on stokaro/ptah#1356: a statement the classifier
// wrongly reports as committing.
//
// `UNLOCK TABLES` with no table locked was measured to commit nothing on both
// servers, and no table can be locked inside a migration transaction because a
// LOCK TABLES would already have ended it. Reporting it as committing recorded
// applied=2 for the body below, and the resume then skipped the INSERT that
// really had been rolled back — permanently, while reporting success. That is
// the same shape as `LOAD DATA`, which was also measured to commit nothing on
// both servers and is pinned in TestImplicitCommitEffectOf; this body uses
// UNLOCK TABLES because it needs no server-side data file to run.
func TestRolledBackProgress_MySQLNonCommittingStatementKeepsNothing(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mysql", "MYSQL_TEST_URL", "MYSQL_URL")
	runRolledBackNonCommittingStatement(t, dbURL, "mysql")
}

func TestRolledBackProgress_MariaDBNonCommittingStatementKeepsNothing(t *testing.T) {
	dbURL := mySQLFamilyTestURL(t, "mariadb", "MARIADB_TEST_URL", "MARIADB_URL")
	runRolledBackNonCommittingStatement(t, dbURL, "mariadb")
}

func runRolledBackDDLCarriesTheRest(t *testing.T, dbURL, dialect string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	names := issue887Names(dialect + "_carry")
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER, note VARCHAR(64))", names.ledgerTable,
	))
	c.Assert(err, qt.IsNil)

	body := fmt.Sprintf(
		"CREATE TABLE %[3]s (id INTEGER PRIMARY KEY);\n"+
			"INSERT INTO %[1]s (id, note) VALUES (1, 'one');\n"+
			"INSERT INTO %[1]s (id, note) SELECT 2, 'two' FROM %[2]s;\n",
		names.ledgerTable, names.blockerTable, names.createdTable,
	)
	migration := migrator.CreateMigrationFromSQL(1, "ddl carries the rest", body,
		fmt.Sprintf("DELETE FROM %s", names.ledgerTable))

	failing := issue887Migrator(conn, names, migration)
	err = failing.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{DiscardRolledBackFailure: true})
	c.Assert(err, qt.IsNotNil)
	// The CREATE ended the transaction, so the INSERT after it committed on its
	// own and the ROLLBACK could not reach it.
	c.Assert(issue887LedgerCount(t, conn, names), qt.Equals, int64(1))
	c.Assert(issue887TableExists(t, conn, names.createdTable), qt.IsTrue)
	c.Assert(issue887AppliedCount(t, conn, names), qt.Equals, int64(2))

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
	// Two rows, not three. Recording applied=1 resumed at the INSERT that had
	// already committed and left the ledger reading one, one, two.
	c.Assert(issue887LedgerCount(t, conn, names), qt.Equals, int64(2))
	c.Assert(issue887LedgerNotes(t, conn, names), qt.DeepEquals, []string{"one", "two"})
	c.Assert(issue887TableExists(t, conn, names.createdTable), qt.IsTrue)
}

func runRolledBackNonCommittingStatement(t *testing.T, dbURL, dialect string) {
	t.Helper()

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	names := issue887Names(dialect + "_nocommit")
	cleanupIssue887(t, conn, names)
	defer cleanupIssue887(t, conn, names)

	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER, note VARCHAR(64))", names.ledgerTable,
	))
	c.Assert(err, qt.IsNil)

	body := fmt.Sprintf(
		"INSERT INTO %[1]s (id, note) VALUES (1, 'one');\n"+
			"UNLOCK TABLES;\n"+
			"INSERT INTO %[1]s (id, note) SELECT 2, 'two' FROM %[2]s;\n",
		names.ledgerTable, names.blockerTable,
	)
	migration := migrator.CreateMigrationFromSQL(1, "non committing statement", body,
		fmt.Sprintf("DELETE FROM %s", names.ledgerTable))

	failing := issue887Migrator(conn, names, migration)
	err = failing.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{DiscardRolledBackFailure: true})
	c.Assert(err, qt.IsNotNil)
	// UNLOCK TABLES committed nothing, so the rollback took the INSERT with it
	// and no revision row may claim otherwise.
	c.Assert(issue887LedgerCount(t, conn, names), qt.Equals, int64(0))
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
	// Both rows. Recording applied=2 resumed past the INSERT the rollback had
	// undone, and no run ever applied it.
	c.Assert(issue887LedgerCount(t, conn, names), qt.Equals, int64(2))
	c.Assert(issue887LedgerNotes(t, conn, names), qt.DeepEquals, []string{"one", "two"})
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

	_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", names.blockerTable))
	c.Assert(err, qt.IsNil)

	retried := issue887Migrator(conn, names, migration)
	err = retried.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{
		AllowDirty:               true,
		DiscardRolledBackFailure: true,
	})
	c.Assert(err, qt.IsNil)
	// Two rows, not three: the committed INSERT must not have run twice.
	c.Assert(issue887LedgerCount(t, conn, names), qt.Equals, int64(2))
	c.Assert(issue887LedgerNotes(t, conn, names), qt.DeepEquals, []string{"one", "three"})
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
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migrations...)).
		WithMigrationsTable("", names.revisionsTable).
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
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

// issue887TableExists asserts against the catalog rather than the revision row.
// A committed prefix that the accounting claims and the schema does not have is
// exactly the failure these tests exist to catch.
func issue887TableExists(t *testing.T, conn *dbschema.DatabaseConnection, table string) bool {
	t.Helper()

	count := issue887ScalarCount(t, conn, fmt.Sprintf(
		"SELECT COUNT(*) FROM information_schema.tables "+
			"WHERE table_schema = DATABASE() AND table_name = '%s'", table,
	))
	return count == 1
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

	for _, table := range []string{
		names.revisionsTable, names.blockerTable, names.ledgerTable, names.createdTable,
	} {
		_, _ = conn.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	}
}
