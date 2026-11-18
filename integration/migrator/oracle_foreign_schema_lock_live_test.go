//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"slices"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// foreignRevisionsSchema is a revision table a DBA created in an account of
// its own, and an application account that records there with DML grants
// alone: it may create tables in its own schema, and in the owner's schema it
// may create nothing.
type foreignRevisionsSchema struct {
	admin  *dbschema.DatabaseConnection
	owner  string
	app    string
	appURL string
}

func newForeignRevisionsSchema(c *qt.C) *foreignRevisionsSchema {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.OracleAdmin)
	f := &foreignRevisionsSchema{admin: connectOracleLock(c, adminURL)}
	suffix := fmt.Sprintf("%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	f.owner = oracleAccount(c, f.admin, "ptah_fown_"+suffix, "GRANT CREATE SESSION, CREATE TABLE TO ptah_fown_"+suffix)
	f.app = oracleAccount(c, f.admin, "ptah_fapp_"+suffix, "GRANT CREATE SESSION, CREATE TABLE TO ptah_fapp_"+suffix)
	f.appURL = oracleAccountURL(c, adminURL, f.app)
	owner, err := migrator.NewFSMigrator(connectOracleLock(c, oracleAccountURL(c, adminURL, f.owner)), fstest.MapFS{})
	c.Assert(err, qt.IsNil)
	c.Assert(owner.Initialize(context.Background()), qt.IsNil)
	f.exec(c, `GRANT SELECT, INSERT, UPDATE, DELETE ON "`+f.owner+`".`+oracleRevisionTable+` TO `+f.app)
	return f
}

func (f *foreignRevisionsSchema) exec(c *qt.C, statement string) {
	c.Helper()
	_, err := f.admin.ExecContext(context.Background(), statement)
	c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
}

// migrator is the application account's run, recording in the owner's schema.
func (f *foreignRevisionsSchema) migrator(c *qt.C) *migrator.Migrator {
	c.Helper()
	run, err := migrator.NewFSMigrator(connectOracleLock(c, f.appURL), oracleWidgets())
	c.Assert(err, qt.IsNil)
	return run.WithMigrationsTable(f.owner, "")
}

// tables counts the tables named table that the owner and the application
// account own between them.
func (f *foreignRevisionsSchema) tables(c *qt.C, table string) int {
	c.Helper()
	var n int
	c.Assert(f.admin.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM all_tables WHERE owner IN (:1, :2) AND table_name = :3",
		f.owner, f.app, table).Scan(&n), qt.IsNil)
	return n
}

// oracleLogTable is the revision table's log, quoted as Ptah creates it.
const oracleLogTable = `"schema_migrations_log"`

// createLog has the owner run a migration once, which creates the revision
// table's log with Ptah's own DDL, and then empties what that run recorded
// and drops the table it created, as a DBA preparing the schema would.
func (f *foreignRevisionsSchema) createLog(c *qt.C) {
	c.Helper()
	ownerURL := oracleAccountURL(c, dbtarget.URL(c, dbtarget.OracleAdmin), f.owner)
	owner, err := migrator.NewFSMigrator(connectOracleLock(c, ownerURL), oracleWidgets())
	c.Assert(err, qt.IsNil)
	c.Assert(owner.MigrateUp(context.Background()), qt.IsNil)
	f.exec(c, `DROP TABLE "`+f.owner+`".widgets`)
	f.exec(c, `DELETE FROM "`+f.owner+`".`+oracleRevisionTable)
	f.exec(c, `DELETE FROM "`+f.owner+`".`+oracleLogTable)
	f.exec(c, `DELETE FROM "`+f.owner+`".ptah_locks`)
}

// value reads the one value query returns, as text.
func (f *foreignRevisionsSchema) value(c *qt.C, query string) string {
	c.Helper()
	var v string
	c.Assert(f.admin.QueryRowContext(context.Background(), query).Scan(&v), qt.IsNil, qt.Commentf("query: %s", query))
	return v
}

// ownerTables lists the tables the owner's schema holds, sorted.
func (f *foreignRevisionsSchema) ownerTables(c *qt.C) []string {
	c.Helper()
	rows, err := f.admin.QueryContext(context.Background(), "SELECT table_name FROM all_tables WHERE owner = :1", f.owner)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = rows.Close() })
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	slices.Sort(names)
	return names
}

func oracleAccountURL(c *qt.C, adminURL, account string) string {
	c.Helper()
	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(account, "ptah_password")
	return parsed.String()
}

// An account with DML grants alone records a full `migrate up` in tables a
// DBA created for it in another account's schema: the revision table, its log
// and the lock table. The run creates nothing there. It takes the lock with
// SELECT and INSERT on ptah_locks, and Initialize leaves alone the revision
// table the catalog shows, whose guarded CREATE Oracle would refuse with
// ORA-01031 to an account without CREATE ANY TABLE.
func TestOracleForeignRevisionsSchemaLockLive_HappyPath(t *testing.T) {
	t.Setenv("PTAH_ALLOW_FOREIGN_METADATA_TABLE", "1")
	c := qt.New(t)
	f := newForeignRevisionsSchema(c)
	f.exec(c, `CREATE TABLE "`+f.owner+`".ptah_locks (name VARCHAR2(1000) PRIMARY KEY)`)
	f.createLog(c)
	f.exec(c, `GRANT SELECT, INSERT ON "`+f.owner+`".ptah_locks TO `+f.app)
	f.exec(c, `GRANT SELECT, INSERT, UPDATE, DELETE ON "`+f.owner+`".`+oracleLogTable+` TO `+f.app)

	err := f.migrator(c).MigrateUp(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(f.value(c, `SELECT COUNT(*) FROM "`+f.app+`".widgets`), qt.Equals, "1")
	c.Assert(f.value(c, `SELECT state FROM "`+f.owner+`".`+oracleRevisionTable+` WHERE version = 1`), qt.Equals, "applied")
	c.Assert(f.value(c, `SELECT COUNT(*) FROM "`+f.owner+`".`+oracleLogTable+` WHERE version = 1`), qt.Not(qt.Equals), "0")
	c.Assert(f.value(c, `SELECT name FROM "`+f.owner+`".ptah_locks`), qt.Equals, "ptah_migrate")
	c.Assert(f.ownerTables(c), qt.DeepEquals, []string{"PTAH_LOCKS", "schema_migrations", "schema_migrations_log"})
}

// Without a lock table in the revisions schema, an account that may not create
// one there refuses before it runs anything, and says what the DBA can do:
// create the table and grant SELECT and INSERT on it, or grant the account
// CREATE ANY TABLE. A lock table in the account's own schema would not
// exclude another account recording in the same revision table, so the run
// does not fall back to one.
func TestOracleForeignRevisionsSchemaLockLive_FailurePath(t *testing.T) {
	t.Setenv("PTAH_ALLOW_FOREIGN_METADATA_TABLE", "1")
	c := qt.New(t)
	f := newForeignRevisionsSchema(c)
	run := f.migrator(c)

	err := run.MigrateUp(context.Background())

	c.Assert(err, qt.ErrorMatches, `(?s)failed to acquire migration lock for migrate up: `+
		`lock table "`+f.owner+`".ptah_locks does not exist, or this account holds no privilege on it, `+
		`and the account may not create it: create it with `+
		`CREATE TABLE "`+f.owner+`".ptah_locks \(name VARCHAR2\(1000\) PRIMARY KEY\) and `+
		`GRANT SELECT, INSERT ON "`+f.owner+`".ptah_locks TO the account, `+
		`or grant the account CREATE ANY TABLE: .*ORA-01031.*`)
	c.Assert(f.tables(c, "WIDGETS"), qt.Equals, 0)
	c.Assert(f.tables(c, "PTAH_LOCKS"), qt.Equals, 0)
	var recorded int
	c.Assert(f.admin.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM "`+f.owner+`".`+oracleRevisionTable+``).Scan(&recorded), qt.IsNil)
	c.Assert(recorded, qt.Equals, 0)
}
