//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dblock"
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

func oracleAccountURL(c *qt.C, adminURL, account string) string {
	c.Helper()
	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(account, "ptah_password")
	return parsed.String()
}

// A lock table the DBA created in the revisions schema, with SELECT and
// INSERT granted, is all an account needs to take the migration lock there:
// the run inserts the row of its lock name and locks it with FOR UPDATE
// NOWAIT, creating nothing in the owner's schema. A second account recording
// in the same revision table is refused while the first holds it.
func TestOracleForeignRevisionsSchemaLockLive_HappyPath(t *testing.T) {
	t.Setenv("PTAH_ALLOW_FOREIGN_METADATA_TABLE", "1")
	c := qt.New(t)
	f := newForeignRevisionsSchema(c)
	f.exec(c, `CREATE TABLE "`+f.owner+`".ptah_locks (name VARCHAR2(1000) PRIMARY KEY)`)
	f.exec(c, `GRANT SELECT, INSERT ON "`+f.owner+`".ptah_locks TO `+f.app)
	var secondErr error

	err := f.migrator(c).HoldMigrationLock(context.Background(), "test", func(context.Context) error {
		_, secondErr = dblock.AcquireInSchema(context.Background(), connectOracleLock(c, f.appURL),
			"ptah_migrate", f.owner, dblock.NoWait)
		return nil
	})

	c.Assert(err, qt.IsNil)
	c.Assert(secondErr, qt.ErrorMatches, `advisory lock "ptah_migrate" on oracle is held by another session`)
	c.Assert(f.tables(c, "PTAH_LOCKS"), qt.Equals, 1)
	var rows int
	c.Assert(f.admin.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM "`+f.owner+`".ptah_locks WHERE name = 'ptah_migrate'`).Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, 1)
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
