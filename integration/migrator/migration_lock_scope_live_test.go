//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dblock"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// lockScopeTarget is an application account and an owner account whose
// schema keeps a lock table the application may use, with the row of
// ptah_migrate there held by another session.
type lockScopeTarget struct {
	app   *migrator.Migrator
	owner string
	login string
}

func newLockScopeTarget(c *qt.C) lockScopeTarget {
	c.Helper()
	c.Setenv("PTAH_ALLOW_FOREIGN_METADATA_TABLE", "1")
	adminURL := dbtarget.URL(c, dbtarget.OracleAdmin)
	admin := connectOracleLock(c, adminURL)
	stamp := time.Now().UnixNano() % 1_000_000_000
	owner := oracleAccount(c, admin, fmt.Sprintf("ptah_ls_o_%d", stamp), fmt.Sprintf("GRANT CREATE SESSION, CREATE TABLE TO ptah_ls_o_%d", stamp))
	login := oracleAccount(c, admin, fmt.Sprintf("ptah_ls_a_%d", stamp), fmt.Sprintf("GRANT CREATE SESSION, CREATE TABLE TO ptah_ls_a_%d", stamp))
	for _, statement := range []string{
		`CREATE TABLE "` + owner + `".ptah_locks (name VARCHAR2(1000) PRIMARY KEY)`,
		`GRANT SELECT, INSERT ON "` + owner + `".ptah_locks TO ` + login,
	} {
		_, err := admin.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	held, err := dblock.AcquireInSchema(context.Background(), admin, "ptah_migrate", owner, dblock.NoWait)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(held.Release(context.Background()), qt.IsNil) })
	app, err := migrator.NewFSMigrator(connectOracleLock(c, oracleAccountURL(c, adminURL, login)), oracleWidgets())
	c.Assert(err, qt.IsNil)
	return lockScopeTarget{app: app.WithMigrationLockTimeout(500 * time.Millisecond), owner: owner, login: login}
}

// nested holds the lock of outer and, under it, asks for the lock of inner
// with the context the first hold gives, reporting whether inner's work ran.
func nested(outer, inner *migrator.Migrator) (bool, error) {
	ran := false
	err := outer.HoldMigrationLock(context.Background(), "outer", func(ctx context.Context) error {
		return inner.HoldMigrationLock(ctx, "inner", func(context.Context) error {
			ran = true
			return nil
		})
	})
	return ran, err
}

// Oracle's lock is a row of ptah_locks in the migrations schema, so a copy of
// a migrator configured for another schema asks for that schema's lock even
// under a held one of the same name. Here another session holds it, and the
// copy times out and runs nothing. Taken as held, the copy would run its work
// while the other schema's holder runs too.
func TestOracleLockScopeLive_FailurePath(t *testing.T) {
	c := qt.New(t)
	target := newLockScopeTarget(c)

	ran, err := nested(target.app, target.app.WithMigrationsTable(target.owner, ""))

	c.Assert(err, qt.ErrorAs, new(*migrator.MigrationLockTimeoutError))
	c.Assert(err, qt.ErrorMatches, `.*timed out acquiring migration lock "ptah_migrate" for oracle after 500ms.*`)
	c.Assert(ran, qt.IsFalse)
}

// A copy that names the session's current schema takes the same lock as one
// that names none, so under the held lock it runs at once. Asking again, it
// would wait for the row its own run holds and time out.
func TestOracleLockScopeLive_HappyPath(t *testing.T) {
	c := qt.New(t)
	target := newLockScopeTarget(c)

	ran, err := nested(target.app, target.app.WithMigrationsTable(target.login, ""))

	c.Assert(err, qt.IsNil)
	c.Assert(ran, qt.IsTrue)
}

// PostgreSQL's advisory lock keeps no table, so it is one lock whatever the
// migrations schema: a copy configured for another schema runs at once under
// the held lock. Asking again, it would wait for the lock its own run holds
// and time out.
func TestPostgresLockScopeLive_HappyPath(t *testing.T) {
	c := qt.New(t)
	app, err := migrator.NewFSMigrator(connectOracleLock(c, postgresTestURL(t)), oracleWidgets())
	c.Assert(err, qt.IsNil)
	app = app.WithMigrationLockTimeout(500 * time.Millisecond)

	ran, err := nested(app, app.WithMigrationsTable("ptah_other_schema", ""))

	c.Assert(err, qt.IsNil)
	c.Assert(ran, qt.IsTrue)
}
