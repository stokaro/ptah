//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// foreignSchemaAccounts is an owner account whose schema the run names with
// WithMigrationsTable, and the run's own account, which may create tables
// there through the ANY privileges, with PTAH_ALLOW_FOREIGN_METADATA_TABLE
// set to 0.
type foreignSchemaAccounts struct {
	admin *dbschema.DatabaseConnection
	// conn is the connection run is on.
	conn  *dbschema.DatabaseConnection
	run   *migrator.Migrator
	owner string
	login string
}

func newForeignSchemaAccounts(c *qt.C) foreignSchemaAccounts {
	c.Helper()
	c.Setenv("PTAH_ALLOW_FOREIGN_METADATA_TABLE", "0")
	adminURL := dbtarget.URL(c, dbtarget.OracleAdmin)
	admin := connectOracleLock(c, adminURL)
	stamp := time.Now().UnixNano() % 1_000_000_000
	owner := oracleAccount(c, admin, fmt.Sprintf("ptah_fl_o_%d", stamp), fmt.Sprintf("GRANT CREATE SESSION TO ptah_fl_o_%d", stamp))
	login := oracleAccount(c, admin, fmt.Sprintf("ptah_fl_a_%d", stamp), fmt.Sprintf(
		"GRANT CREATE SESSION, CREATE TABLE, CREATE ANY TABLE, CREATE ANY INDEX, INSERT ANY TABLE, SELECT ANY TABLE TO ptah_fl_a_%d",
		stamp,
	))
	conn := connectOracleLock(c, oracleAccountURL(c, adminURL, login))
	run, err := migrator.NewFSMigrator(conn, oracleWidgets())
	c.Assert(err, qt.IsNil)
	return foreignSchemaAccounts{admin: admin, conn: conn, run: run.WithMigrationsTable(owner, ""), owner: owner, login: login}
}

// tables counts the tables of both accounts.
func (a foreignSchemaAccounts) tables(c *qt.C) string {
	c.Helper()
	return readOne(c, a.admin, `SELECT COUNT(*) FROM all_tables WHERE owner IN ('`+a.owner+`', '`+a.login+`')`)
}

// A migration lock that keeps its table in another account's schema writes to
// a table that account owns, which Ptah refuses for every other metadata table
// unless PTAH_ALLOW_FOREIGN_METADATA_TABLE accepts it. A table a DBA created
// and granted is refused before the lock writes its row, the run runs nothing,
// and the error names the lock table.
func TestOracleForeignLockTableLive_FailurePath(t *testing.T) {
	c := qt.New(t)
	accounts := newForeignSchemaAccounts(c)
	for _, statement := range []string{
		`CREATE TABLE "` + accounts.owner + `".ptah_locks (name VARCHAR2(1000) PRIMARY KEY)`,
		`GRANT SELECT, INSERT ON "` + accounts.owner + `".ptah_locks TO ` + accounts.login,
	} {
		_, err := accounts.admin.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}

	err := accounts.run.MigrateUp(context.Background())

	c.Assert(err, qt.ErrorIs, migrator.ErrForeignMetadataTable)
	c.Assert(err, qt.ErrorMatches,
		`(?s).*refusing to use metadata table "`+accounts.owner+`"\."PTAH_LOCKS": it is owned by "`+accounts.owner+`".*`)
	c.Assert(readOne(c, accounts.admin, `SELECT COUNT(*) FROM "`+accounts.owner+`".ptah_locks`), qt.Equals, "0")
	c.Assert(accounts.tables(c), qt.Equals, "1")
}

// On Oracle a schema is an account, and a table created in it is that
// account's whoever creates it. So a lock table that does not exist yet in
// another account's schema is refused before the lock creates it, even where
// CREATE ANY TABLE would let it: neither schema gains a table, and the error
// names the table the lock would have created.
func TestOracleAbsentForeignLockTableLive_FailurePath(t *testing.T) {
	c := qt.New(t)
	accounts := newForeignSchemaAccounts(c)

	err := accounts.run.MigrateUp(context.Background())

	assertAbsentForeignTableRefused(c, accounts, err, "PTAH_LOCKS")
}

// The revision table and its log follow the same rule. Without the migration
// lock the revision table is the first metadata table the run would create in
// the owner's schema, and it is refused before the CREATE.
func TestOracleAbsentForeignRevisionTableLive_FailurePath(t *testing.T) {
	c := qt.New(t)
	accounts := newForeignSchemaAccounts(c)

	err := accounts.run.WithoutMigrationLock().MigrateUp(context.Background())

	assertAbsentForeignTableRefused(c, accounts, err, "schema_migrations")
}

// assertAbsentForeignTableRefused asserts that err refuses to create table in
// the owner's schema, and that neither account's schema holds a table.
func assertAbsentForeignTableRefused(c *qt.C, accounts foreignSchemaAccounts, err error, table string) {
	c.Helper()
	c.Assert(err, qt.ErrorIs, migrator.ErrForeignMetadataTable)
	c.Assert(err, qt.ErrorMatches, `(?s).*refusing to create metadata table "`+accounts.owner+`"\."`+table+
		`": its schema belongs to "`+accounts.owner+`" and this connection runs as "`+accounts.login+`".*`)
	c.Assert(accounts.tables(c), qt.Equals, "0")
}

// A command that creates nothing is not refused for a revision table that does
// not exist yet in another account's schema: the status and a dry run of the
// run report an empty history. A refusal there says Ptah refuses to create a
// table the command was never going to create. Neither schema gains a table,
// and the run that would create one is refused, as
// TestOracleAbsentForeignRevisionTableLive_FailurePath holds.
//
// The layout question an adoption preflight asks is asked of a table that
// exists, and with none it reports the table missing rather than a refusal to
// create it.
func TestOracleAbsentForeignRevisionTableReadLive_HappyPath(t *testing.T) {
	c := qt.New(t)
	accounts := newForeignSchemaAccounts(c)
	accounts.conn.SchemaWriter().SetDryRun(true)
	reader := accounts.run.WithoutMigrationLock()

	status, statusErr := reader.GetMigrationStatus(context.Background())
	dryRunErr := reader.MigrateUp(context.Background())
	_, baseErr := reader.RevisionLayoutBase(context.Background())

	c.Assert(statusErr, qt.IsNil)
	c.Assert(status.PendingMigrations, qt.DeepEquals, []int64{1})
	c.Assert(dryRunErr, qt.IsNil)
	c.Assert(baseErr, qt.Not(qt.ErrorIs), migrator.ErrForeignMetadataTable)
	c.Assert(baseErr, qt.ErrorMatches, `(?s)failed to inspect migrations metadata columns: .*ORA-00942.*`)
	c.Assert(accounts.tables(c), qt.Equals, "0")
}
