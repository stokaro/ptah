//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dblock"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// Two accounts that keep one revision table, in a schema --migrations-schema
// names, take one migration lock: the lock table lives beside the revision
// table, not in each login's own schema. Here the first account holds the
// lock while the second runs `migrate up` without waiting; the second refuses
// and records nothing. With the lock table in each login's schema, each
// account locks a row of a table of its own, and the second applies the
// migration while the first holds the lock.
//
// On Oracle every table in a schema belongs to that schema's account, so a
// revision table in a schema neither account owns is one Ptah would refuse as
// foreign. Sharing one is what PTAH_ALLOW_FOREIGN_METADATA_TABLE accepts, and
// the test sets it; without it the second account stops at that refusal
// instead, after taking a lock of its own.
func TestOracleMigrationsSchemaLockLive_FailurePath(t *testing.T) {
	t.Setenv("PTAH_ALLOW_FOREIGN_METADATA_TABLE", "1")
	c := qt.New(t)
	adminURL := dbtarget.URL(c, dbtarget.OracleAdmin)
	admin := connectOracleLock(c, adminURL)
	suffix := fmt.Sprintf("%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	owner := oracleAccount(c, admin, "ptah_revs_owner_"+suffix, "GRANT CREATE SESSION TO ptah_revs_owner_"+suffix)
	var runs []*migrator.Migrator
	for _, role := range []string{"first", "second"} {
		account := oracleAccount(c, admin, "ptah_revs_"+role+"_"+suffix,
			"GRANT CREATE SESSION, CREATE TABLE, CREATE ANY TABLE, CREATE ANY INDEX, ALTER ANY TABLE, "+
				"SELECT ANY TABLE, INSERT ANY TABLE, UPDATE ANY TABLE, DELETE ANY TABLE, LOCK ANY TABLE "+
				"TO ptah_revs_"+role+"_"+suffix)
		parsed, err := url.Parse(adminURL)
		c.Assert(err, qt.IsNil)
		parsed.User = url.UserPassword(account, "ptah_password")
		run, err := migrator.NewFSMigrator(connectOracleLock(c, parsed.String()), oracleWidgets())
		c.Assert(err, qt.IsNil)
		runs = append(runs, run.WithMigrationsTable(owner, "").WithMigrationLockTimeout(dblock.NoWait))
	}
	var secondErr error

	holdErr := runs[0].HoldMigrationLock(context.Background(), "test", func(context.Context) error {
		secondErr = runs[1].MigrateUp(context.Background())
		return nil
	})

	c.Assert(holdErr, qt.IsNil)
	c.Assert(secondErr, qt.ErrorAs, new(*migrator.MigrationLockTimeoutError))
	c.Assert(secondErr, qt.ErrorMatches,
		`failed to acquire migration lock for migrate up: migration lock "ptah_migrate" for oracle is held by another runner`)
	var recorded int
	c.Assert(admin.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM all_tables WHERE owner LIKE :1 AND table_name IN ('WIDGETS', 'schema_migrations')",
		"PTAH_REVS_%"+strings.ToUpper(suffix)).Scan(&recorded), qt.IsNil)
	c.Assert(recorded, qt.Equals, 0)
	var lockTables []string
	rows, err := admin.QueryContext(context.Background(),
		"SELECT owner FROM all_tables WHERE table_name = 'PTAH_LOCKS' AND owner LIKE :1",
		"PTAH_REVS_%"+strings.ToUpper(suffix))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = rows.Close() })
	for rows.Next() {
		var tableOwner string
		c.Assert(rows.Scan(&tableOwner), qt.IsNil)
		lockTables = append(lockTables, tableOwner)
	}
	c.Assert(rows.Err(), qt.IsNil)
	c.Assert(lockTables, qt.DeepEquals, []string{owner})
}

// oracleAccount creates the account name with quota on USERS and grants, and
// drops it with everything it owns when the test ends. It returns the name as
// the data dictionary spells it, in upper case.
func oracleAccount(c *qt.C, admin *dbschema.DatabaseConnection, name, grants string) string {
	c.Helper()
	c.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, dropErr := admin.ExecContext(ctx, "DROP USER "+name+" CASCADE")
		c.Check(dropErr, qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE USER " + name + " IDENTIFIED BY ptah_password",
		"ALTER USER " + name + " QUOTA UNLIMITED ON USERS",
		grants,
	} {
		_, err := admin.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	return strings.ToUpper(name)
}
