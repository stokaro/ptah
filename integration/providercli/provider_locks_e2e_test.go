//go:build integration

package providercli_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/dblock"
	"ptah.run/migration/migrator"
)

func lockedProviderMigrator(conn *dbschema.DatabaseConnection) *migrator.Migrator {
	migration := migrator.CreateMigrationFromSQL(1, "records", `CREATE TABLE "records" ("id" INTEGER);`, `DROP TABLE "records";`)
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("main", "_revisions").WithMigrationLog(false)
}

func TestProviderMigratorUsesSessionLocks(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "locks")
	for _, mode := range []migrator.MigrationTxMode{migrator.MigrationTxModeFile, migrator.MigrationTxModeNone, migrator.MigrationTxModeAll} {
		t.Run(string(mode), func(t *testing.T) {
			c := qt.New(t)
			conn, _ := transactionConnection(c, host, "")
			runner := lockedProviderMigrator(conn).WithTransactionMode(mode)
			c.Assert(runner.MigrationLockSkipped(), qt.IsFalse)
			c.Assert(runner.MigrateUp(c.Context()), qt.IsNil)
			c.Assert(providerTablePresent(c, conn, "records"), qt.IsTrue)
			rows, err := runner.GetRevisions(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(rows, qt.HasLen, 1)
			c.Assert(rows[0].State, qt.Equals, "applied")
			c.Assert(runner.MigrateDown(c.Context()), qt.IsNil)
			c.Assert(providerTablePresent(c, conn, "records"), qt.IsFalse)
		})
	}
}

func TestProviderMigrationLockExcludesAnotherProcess(t *testing.T) {
	c := qt.New(t)
	owner, _ := yamlProviderCommand(c, "locks")
	contender, _ := yamlProviderCommand(c, "locks")
	conn, address := transactionConnection(c, owner, "")
	other := connectTransactionAddress(c, contender, address)
	service, err := conn.SessionLocker()
	c.Assert(err, qt.IsNil)
	held, err := service.TryLock(c.Context(), "ptah_migrate")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = held.Release(context.Background()) })
	for _, timeout := range []time.Duration{dblock.NoWait, 40 * time.Millisecond} {
		t.Run(timeout.String(), func(t *testing.T) {
			c := qt.New(t)
			err := lockedProviderMigrator(other).WithMigrationLockTimeout(timeout).MigrateUp(c.Context())
			c.Assert(migrator.IsMigrationLockTimeout(err), qt.IsTrue, qt.Commentf("error: %v", err))
			c.Assert(providerTablePresent(c, other, "records"), qt.IsFalse)
			c.Assert(providerTablePresent(c, other, "_revisions"), qt.IsFalse)
			c.Assert(held.CheckOwnership(c.Context()), qt.IsNil)
		})
	}
	ctx, cancel := context.WithTimeout(c.Context(), 100*time.Millisecond)
	defer cancel()
	_, err = dblock.Acquire(ctx, other, "ptah_migrate", 0)
	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
	c.Assert(held.Release(c.Context()), qt.IsNil)
	c.Assert(lockedProviderMigrator(other).MigrateUp(c.Context()), qt.IsNil)
	c.Assert(providerTablePresent(c, other, "records"), qt.IsTrue)
}

func TestProviderLockLossCancelsGuardedWork(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "locks")
	conn, _ := transactionConnection(c, host, "lose-lock-on-check")
	held, err := dblock.Acquire(c.Context(), conn, "migration", dblock.NoWait)
	c.Assert(err, qt.IsNil)
	c.Assert(held.Supported(), qt.IsTrue)
	c.Cleanup(func() { _ = held.Release(context.Background()) })
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Second)
	defer cancel()
	guarded, stop := held.Guard(ctx)
	defer stop()
	<-guarded.Done()
	c.Assert(dblock.IsLost(context.Cause(guarded)), qt.IsTrue)
	c.Assert(dblock.IsLost(held.Err()), qt.IsTrue)
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), `CREATE TABLE "late" ("id" INTEGER);`), qt.ErrorIs, dbschema.ErrSessionLockLost)
	c.Assert(dblock.IsLost(held.Release(c.Context())), qt.IsTrue)
	_, err = dblock.Acquire(c.Context(), conn, "replacement", dblock.NoWait)
	c.Assert(err, qt.ErrorIs, dbschema.ErrSessionLockLost)
}

func TestProviderLockServiceRefusalDoesNotChooseNoOp(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, _ := transactionConnection(c, host, "")
	_, err := dblock.Acquire(c.Context(), conn, "migration", dblock.NoWait)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(lockedProviderMigrator(conn).MigrateUp(c.Context()), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(providerTablePresent(c, conn, "records"), qt.IsFalse)
}

func TestProviderLockLostAcquisitionIsNotRetried(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "locks")
	conn, address := transactionConnection(c, host, "lost-lock-reply")
	_, err := dblock.Acquire(c.Context(), conn, "migration", time.Second)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), `CREATE TABLE "late" ("id" INTEGER);`), qt.ErrorIs, dbschema.ErrSessionLockLost)
	other := connectTransactionAddress(c, host, address)
	service, err := other.SessionLocker()
	c.Assert(err, qt.IsNil)
	// The fresh owner uses the same injected lost reply. Its unknown result
	// proves the previous attempt was cleaned up instead of remaining busy.
	_, err = service.TryLock(c.Context(), "migration")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
}

func TestProviderLockSessionUsesTheGuardedConnection(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "locks")
	conn, _ := transactionConnection(c, host, "")
	runErr, releaseErr := dblock.WithLockSession(c.Context(), conn, "migration", dblock.NoWait,
		func(scoped *dbschema.DatabaseConnection, lock *dblock.Lock) error {
			c.Assert(lock.Supported(), qt.IsTrue)
			service, err := scoped.SessionLocker()
			c.Assert(err, qt.IsNil)
			_, err = service.TryLock(c.Context(), "migration")
			c.Assert(err, qt.ErrorIs, dbschema.ErrSessionLockBusy)
			return scoped.Writer().ExecuteSQL(c.Context(), `CREATE TABLE "guarded" ("id" INTEGER);`)
		})
	c.Assert(runErr, qt.IsNil)
	c.Assert(releaseErr, qt.IsNil)
	c.Assert(providerTablePresent(c, conn, "guarded"), qt.IsTrue)
	called := false
	previewedLock := true
	runErr, releaseErr = dblock.WithPreviewLockSession(c.Context(), conn, "migration", dblock.NoWait,
		func(_ *dbschema.DatabaseConnection, lock *dblock.Lock) error {
			called = true
			previewedLock = lock.Supported()
			return nil
		})
	c.Assert(runErr, qt.IsNil)
	c.Assert(releaseErr, qt.IsNil)
	c.Assert(called, qt.IsTrue)
	c.Assert(previewedLock, qt.IsFalse)
}

func TestProviderLockLossBeforeCommitPreservesUnknownOutcome(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "locks")
	conn, address := transactionConnection(c, host, "lose-lock-before-commit")
	err := lockedProviderMigrator(conn).WithTransactionMode(migrator.MigrationTxModeFile).MigrateUp(c.Context())
	c.Assert(err, qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	fresh := connectTransactionAddress(c, host, address)
	c.Assert(providerTablePresent(c, fresh, "records"), qt.IsFalse)
}

func TestProviderLockLossAtReleaseDoesNotClaimRollback(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "locks")
	conn, address := transactionConnection(c, host, "lose-lock-on-check")
	err := lockedProviderMigrator(conn).MigrateUp(c.Context())
	c.Assert(err, qt.ErrorIs, dbschema.ErrSessionLockLost)
	fresh := connectTransactionAddress(c, host, address)
	c.Assert(providerTablePresent(c, fresh, "records"), qt.IsTrue)
	store, err := fresh.HistoryStore()
	c.Assert(err, qt.IsNil)
	snapshot := readProviderHistory(c, store)
	c.Assert(snapshot.Records, qt.HasLen, 1)
	c.Assert(snapshot.Records[0].State, qt.Equals, "applied")
}

func TestProviderLocksMustBeAcquiredBeforeTransactions(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "locks")
	conn, _ := transactionConnection(c, host, "")
	tx, err := conn.SchemaWriter().BeginTransaction(c.Context())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = tx.Rollback() })
	_, err = conn.WithExecutor(tx).SessionLocker()
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(tx.Rollback(), qt.IsNil)
	held, err := dblock.Acquire(c.Context(), conn, "migration", dblock.NoWait)
	c.Assert(err, qt.IsNil)
	c.Assert(held.Release(c.Context()), qt.IsNil)
}
