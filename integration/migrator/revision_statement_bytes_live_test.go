//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// A migration statement is bytes its author wrote, and MySQL and MariaDB accept
// a byte that is not valid UTF-8 inside a literal bound for a binary column.
// The revision table's error_stmt column is utf8mb4 and answers Error 1366 for
// the same bytes, so recording the statement raw refuses a migration the server
// would have run (stokaro/ptah#3316). These tests need a live server because
// that refusal is the server's answer, not Ptah's.
type statementBytesNames struct {
	revisions string
	payloads  string
	absent    string
}

func newStatementBytesNames(dialect string) statementBytesNames {
	suffix := fmt.Sprintf("%s_%d_%d", dialect, os.Getpid(), time.Now().UnixNano())
	return statementBytesNames{
		revisions: "ptah_3316_rev_" + suffix,
		payloads:  "ptah_3316_pay_" + suffix,
		absent:    "ptah_3316_abs_" + suffix,
	}
}

func statementBytesConnection(t *testing.T, dbURL string, names statementBytesNames) *dbschema.DatabaseConnection {
	t.Helper()
	c := qt.New(t)

	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	t.Cleanup(func() { dropStatementBytesObjects(conn, names) })
	dropStatementBytesObjects(conn, names)
	return conn
}

func dropStatementBytesObjects(conn *dbschema.DatabaseConnection, names statementBytesNames) {
	for _, table := range []string{names.revisions, names.payloads, names.absent} {
		_, _ = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+table)
	}
}

func statementBytesMigrator(
	conn *dbschema.DatabaseConnection,
	names statementBytesNames,
	up string,
) *migrator.Migrator {
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "binary payload", up, "DROP TABLE IF EXISTS "+names.payloads),
	)).WithMigrationsTable("", names.revisions).WithMigrationLockTimeout(10 * time.Second)
}

// acceptedStatementBody carries the byte in the statement the server accepts,
// so the run measures the bookkeeping rather than the SQL.
func acceptedStatementBody(names statementBytesNames) string {
	return fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, payload VARBINARY(32) NOT NULL);\n"+
			"INSERT INTO %s (id, payload) VALUES (1, '\xff');",
		names.payloads, names.payloads,
	)
}

// refusedStatementBody carries the same byte in a statement the server refuses,
// so the failure row is what has to record it.
func refusedStatementBody(names statementBytesNames) string {
	return fmt.Sprintf(
		"CREATE TABLE %s (id INT PRIMARY KEY, payload VARBINARY(32) NOT NULL);\n"+
			"INSERT INTO %s (id, payload) VALUES (1, '\xff');",
		names.payloads, names.absent,
	)
}

// runStatementBytesApplies is the happy path: the server accepts every
// statement, so the migration reaches applied and the byte is in the table.
func runStatementBytesApplies(t *testing.T, dbURL, dialect string) {
	t.Helper()
	c := qt.New(t)

	names := newStatementBytesNames(dialect)
	conn := statementBytesConnection(t, dbURL, names)
	mig := statementBytesMigrator(conn, names, acceptedStatementBody(names))

	c.Assert(mig.MigrateUp(t.Context()), qt.IsNil)

	var stored string
	c.Assert(
		conn.QueryRowContext(t.Context(), fmt.Sprintf("SELECT HEX(payload) FROM %s WHERE id = 1", names.payloads)).
			Scan(&stored),
		qt.IsNil,
	)
	c.Assert(stored, qt.Equals, "FF")

	status, err := mig.GetMigrationStatus(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{1})
}

// runStatementBytesRecordsFailure is the failure path: the server refuses the
// statement, and the row an operator repairs from names it.
func runStatementBytesRecordsFailure(t *testing.T, dbURL, dialect string) {
	t.Helper()
	c := qt.New(t)

	names := newStatementBytesNames(dialect)
	conn := statementBytesConnection(t, dbURL, names)
	mig := statementBytesMigrator(conn, names, refusedStatementBody(names))

	err := mig.MigrateUp(t.Context())
	c.Assert(err, qt.IsNotNil)
	// The server's own answer, not a bookkeeping refusal: without the
	// rendering, the run stops on Error 1366 before this statement is sent.
	c.Assert(err.Error(), qt.Contains, names.absent)
	c.Assert(err.Error(), qt.Not(qt.Contains), "Error 1366")

	revisions, err := mig.GetRevisions(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 1)
	c.Assert(revisions[0].State, qt.Equals, "failed")
	// The executor hands on the statement without its terminating semicolon,
	// and the byte that is not valid UTF-8 is rendered as an escape.
	c.Assert(
		revisions[0].ErrorStatement,
		qt.Equals,
		fmt.Sprintf(`INSERT INTO %s (id, payload) VALUES (1, '\xFF')`, names.absent),
	)

	status, err := mig.GetMigrationStatus(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.ErrorStatement, qt.Contains, `'\xFF'`)
}

func TestRevisionStatementBytes_MySQLAppliesAStatementTheServerAccepts(t *testing.T) {
	runStatementBytesApplies(t, mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL), "mysql")
}

func TestRevisionStatementBytes_MariaDBAppliesAStatementTheServerAccepts(t *testing.T) {
	runStatementBytesApplies(t, mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB), "mariadb")
}

func TestRevisionStatementBytes_MySQLRecordsARefusedStatement(t *testing.T) {
	runStatementBytesRecordsFailure(t, mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL), "mysql")
}

func TestRevisionStatementBytes_MariaDBRecordsARefusedStatement(t *testing.T) {
	runStatementBytesRecordsFailure(t, mySQLFamilyTestURL(t, "mariadb", dbtarget.MariaDB), "mariadb")
}
