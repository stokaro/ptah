//go:build integration

package providercli_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/execoutcome"
	"ptah.run/migration/migrator"
)

func providerHistoryTable() dbschema.HistoryTable {
	return dbschema.HistoryTable{Schema: "main", Name: "_revisions", Format: dbschema.NativeHistory}
}
func providerHistoryRecord(version string) dbschema.HistoryRecord {
	return dbschema.HistoryRecord{Version: version, Description: "records", State: "applied", Direction: "up", Applied: 1, Total: 1, AppliedAt: time.Unix(1792571646, 123456789).UTC(), ExecutionTime: 1234567 * time.Nanosecond}
}
func readProviderHistory(c *qt.C, store dbschema.HistoryStore) dbschema.HistorySnapshot {
	c.Helper()
	snapshot, err := store.Read(c.Context(), providerHistoryTable())
	c.Assert(err, qt.IsNil)
	return snapshot
}
func TestProviderHistoryUsesTheActiveTransaction(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "transactions")
	for _, test := range []struct {
		name   string
		commit bool
		rows   int
		finish func(catalog.SchemaTransaction) error
	}{
		{"commit", true, 1, catalog.SchemaTransaction.Commit},
		{"rollback", false, 0, catalog.SchemaTransaction.Rollback},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, _ := transactionConnection(c, host, "")
			store, err := conn.HistoryStore()
			c.Assert(err, qt.IsNil)
			c.Assert(store.Prepare(c.Context(), providerHistoryTable()), qt.IsNil)
			tx, err := conn.SchemaWriter().BeginTransaction(c.Context())
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { _ = tx.Rollback() })
			scoped := conn.WithExecutor(tx)
			transactional, err := scoped.HistoryStore()
			c.Assert(err, qt.IsNil)
			c.Assert(scoped.Writer().ExecuteSQL(c.Context(), `CREATE TABLE "records" ("id" INTEGER NOT NULL PRIMARY KEY);`), qt.IsNil)
			row := providerHistoryRecord("1")
			c.Assert(transactional.CompareAndSwap(c.Context(), providerHistoryTable(), nil, &row), qt.IsNil)
			snapshot := readProviderHistory(c, transactional)
			c.Assert(snapshot.Records, qt.DeepEquals, []dbschema.HistoryRecord{row})
			c.Assert(test.finish(tx), qt.IsNil)
			c.Assert(readProviderHistory(c, store).Records, qt.HasLen, test.rows)
			c.Assert(providerTablePresent(c, conn, "records"), qt.Equals, test.commit)
			_, err = transactional.Read(c.Context(), providerHistoryTable())
			c.Assert(err, qt.ErrorMatches, "provider transaction has finished execution")
		})
	}
}
func TestProviderHistoryConflictPreventsSchemaCommit(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "transactions")
	conn, _ := transactionConnection(c, host, "")
	store, err := conn.HistoryStore()
	c.Assert(err, qt.IsNil)
	c.Assert(store.Prepare(c.Context(), providerHistoryTable()), qt.IsNil)
	row := providerHistoryRecord("1")
	c.Assert(store.CompareAndSwap(c.Context(), providerHistoryTable(), nil, &row), qt.IsNil)
	tx, err := conn.SchemaWriter().BeginTransaction(c.Context())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = tx.Rollback() })
	transactional, err := conn.WithExecutor(tx).HistoryStore()
	c.Assert(err, qt.IsNil)
	c.Assert(tx.ExecuteSQL(c.Context(), `CREATE TABLE "unrecorded" ("id" INTEGER);`), qt.IsNil)
	err = transactional.CompareAndSwap(c.Context(), providerHistoryTable(), nil, &row)
	c.Assert(err, qt.ErrorIs, provider.ErrExecution)
	c.Assert(err, qt.Not(qt.ErrorIs), ptaherr.ErrOutcomeUnknown)
	c.Assert(execoutcome.Unknown(err), qt.IsFalse)
	c.Assert(tx.Commit(), qt.ErrorIs, provider.ErrExecution)
	c.Assert(tx.Rollback(), qt.IsNil)
	c.Assert(providerTablePresent(c, conn, "unrecorded"), qt.IsFalse)
	c.Assert(readProviderHistory(c, store).Records, qt.DeepEquals, []dbschema.HistoryRecord{row})
}
func TestProviderHistoryDrivesCommonMigratorStatus(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "transactions")
	conn, _ := transactionConnection(c, host, "")
	first := migrator.CreateMigrationFromSQL(2, "second", "SELECT 2;", "")
	second := migrator.CreateMigrationFromSQL(10, "tenth", "SELECT 10;", "")
	migrations := migrator.NewRegisteredMigrationProvider(first, second)
	runner := migrator.NewMigrator(conn, migrations).WithMigrationsTable("main", "_revisions")
	present, err := runner.MetadataPresent(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(present, qt.IsFalse)
	c.Assert(runner.Initialize(c.Context()), qt.IsNil)
	store, err := conn.HistoryStore()
	c.Assert(err, qt.IsNil)
	for _, migration := range []*migrator.Migration{second, first} {
		row := providerHistoryRecord(migration.RevisionVersion())
		row.Description = migration.Description
		digest := sha256.Sum256([]byte(migration.UpSQL))
		row.Checksum = hex.EncodeToString(digest[:])
		c.Assert(store.CompareAndSwap(c.Context(), providerHistoryTable(), nil, &row), qt.IsNil)
	}
	snapshot, err := runner.GetMigrationStatusSnapshot(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(snapshot.Revisions, qt.HasLen, 2)
	c.Assert(snapshot.Revisions[0].Version, qt.Equals, int64(2))
	c.Assert(snapshot.Revisions[1].Version, qt.Equals, int64(10))
	c.Assert(snapshot.Revisions[0].AppliedAt, qt.Equals, providerHistoryRecord("2").AppliedAt)
	c.Assert(snapshot.Revisions[0].ExecutionTime, qt.Equals, 1234567*time.Nanosecond)
	c.Assert(snapshot.Status.CurrentVersion, qt.Equals, int64(10))
	c.Assert(snapshot.Status.HasPendingChanges, qt.IsFalse)
	prior := readProviderHistory(c, store).Records[0]
	dirty := prior
	dirty.State = "pending"
	dirty.Applied = 0
	dirty.Error = "interrupted"
	c.Assert(store.CompareAndSwap(c.Context(), providerHistoryTable(), &prior, &dirty), qt.IsNil)
	snapshot, err = runner.GetMigrationStatusSnapshot(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(snapshot.Status.DirtyRevision, qt.IsNotNil)
	c.Assert(snapshot.Status.DirtyRevision.Error, qt.Equals, "interrupted")
	c.Assert(snapshot.Status.HasPendingChanges, qt.IsTrue)
}
func TestProviderHistoryDryRunDoesNotCreateStorage(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "transactions")
	conn, _ := transactionConnection(c, host, "")
	conn.SchemaWriter().SetDryRun(true)
	runner := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider()).WithMigrationsTable("main", "_revisions")
	rows, err := runner.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 0)
	store, err := conn.HistoryStore()
	c.Assert(err, qt.IsNil)
	c.Assert(readProviderHistory(c, store).Exists, qt.IsFalse)
	c.Assert(store.Prepare(c.Context(), providerHistoryTable()), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	row := providerHistoryRecord("1")
	c.Assert(store.CompareAndSwap(c.Context(), providerHistoryTable(), nil, &row), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	conn.SchemaWriter().SetDryRun(false)
	c.Assert(runner.Initialize(c.Context()), qt.IsNil)
	c.Assert(readProviderHistory(c, store).Exists, qt.IsTrue)
}
func TestProviderHistoryRefusesAnUnboundExecutor(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "transactions")
	conn, _ := transactionConnection(c, host, "")
	store, err := conn.WithExecutor(historyUnboundExecutor{}).HistoryStore()
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(store, qt.IsNil)
}

type historyUnboundExecutor struct{}

func (historyUnboundExecutor) IsDryRun() bool                                   { return false }
func (historyUnboundExecutor) ExecuteSQL(context.Context, string, ...any) error { return nil }

func TestProviderHistoryMissingServiceDoesNotUseHostSQL(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "provider")
	conn, _ := transactionConnection(c, host, "")
	store, err := conn.HistoryStore()
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(store, qt.IsNil)
	runner := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider())
	rows, err := runner.GetRevisions(c.Context())
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(rows, qt.IsNil)
}

func TestProviderHistoryInvalidRequestDoesNotPoisonTransaction(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "transactions")
	conn, _ := transactionConnection(c, host, "")
	store, err := conn.HistoryStore()
	c.Assert(err, qt.IsNil)
	c.Assert(store.Prepare(c.Context(), providerHistoryTable()), qt.IsNil)
	tx, err := conn.SchemaWriter().BeginTransaction(c.Context())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = tx.Rollback() })
	transactional, err := conn.WithExecutor(tx).HistoryStore()
	c.Assert(err, qt.IsNil)
	c.Assert(transactional.Prepare(c.Context(), providerHistoryTable()), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(transactional.CompareAndSwap(c.Context(), providerHistoryTable(), nil, nil), qt.ErrorIs, provider.ErrProtocol)
	before, after := providerHistoryRecord("1"), providerHistoryRecord("2")
	c.Assert(transactional.CompareAndSwap(c.Context(), providerHistoryTable(), &before, &after), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(tx.ExecuteSQL(c.Context(), `CREATE TABLE "valid_after_refusal" ("id" INTEGER);`), qt.IsNil)
	c.Assert(transactional.CompareAndSwap(c.Context(), providerHistoryTable(), nil, &after), qt.IsNil)
	c.Assert(tx.Commit(), qt.IsNil)
	c.Assert(readProviderHistory(c, store).Records, qt.DeepEquals, []dbschema.HistoryRecord{after})
	c.Assert(providerTablePresent(c, conn, "valid_after_refusal"), qt.IsTrue)
}

func TestProviderHistoryLostReceiptKeepsTheHostOutcomeUnknown(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "transactions")
	conn, _ := transactionConnection(c, host, "lost-history")
	store, err := conn.HistoryStore()
	c.Assert(err, qt.IsNil)
	c.Assert(store.Prepare(c.Context(), providerHistoryTable()), qt.IsNil)
	row := providerHistoryRecord("1")
	err = store.CompareAndSwap(c.Context(), providerHistoryTable(), nil, &row)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	c.Assert(execoutcome.Unknown(err), qt.IsTrue)
	c.Assert(readProviderHistory(c, store).Records, qt.DeepEquals, []dbschema.HistoryRecord{row})
}

func TestProviderHistoryLostTransactionReceiptPreventsCommit(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "transactions")
	conn, _ := transactionConnection(c, host, "lost-history")
	store, err := conn.HistoryStore()
	c.Assert(err, qt.IsNil)
	c.Assert(store.Prepare(c.Context(), providerHistoryTable()), qt.IsNil)
	tx, err := conn.SchemaWriter().BeginTransaction(c.Context())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = tx.Rollback() })
	transactional, err := conn.WithExecutor(tx).HistoryStore()
	c.Assert(err, qt.IsNil)
	c.Assert(tx.ExecuteSQL(c.Context(), `CREATE TABLE "uncertain" ("id" INTEGER);`), qt.IsNil)
	row := providerHistoryRecord("1")
	err = transactional.CompareAndSwap(c.Context(), providerHistoryTable(), nil, &row)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	c.Assert(execoutcome.Unknown(err), qt.IsTrue)
	c.Assert(tx.Commit(), qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	c.Assert(tx.Rollback(), qt.IsNil)
	c.Assert(readProviderHistory(c, store).Records, qt.HasLen, 0)
	c.Assert(providerTablePresent(c, conn, "uncertain"), qt.IsFalse)
}

// These tests qualify revision writes before provider locking and migration-log
// storage are wired. Explicit caller options isolate that unfinished work;
// default workflows must retain both services.
func TestProviderHistoryRunsCommonMigratorUpAndDown(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "transactions")
	for _, mode := range []migrator.MigrationTxMode{migrator.MigrationTxModeFile, migrator.MigrationTxModeNone, migrator.MigrationTxModeAll} {
		t.Run(string(mode), func(t *testing.T) {
			c := qt.New(t)
			conn, _ := transactionConnection(c, host, "")
			migration := migrator.CreateMigrationFromSQL(1, "records", `CREATE TABLE "records" ("id" INTEGER);`, `DROP TABLE "records";`)
			runner := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
				WithMigrationsTable("main", "_revisions").
				WithTransactionMode(mode).WithMigrationLog(false).WithoutMigrationLock()
			c.Assert(runner.MigrateUp(c.Context()), qt.IsNil)
			c.Assert(providerTablePresent(c, conn, "records"), qt.IsTrue)
			rows, err := runner.GetRevisions(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(rows, qt.HasLen, 1)
			c.Assert(rows[0].State, qt.Equals, "applied")
			c.Assert(rows[0].Applied, qt.Equals, 1)
			c.Assert(rows[0].StatementOutcomeUnknown(), qt.IsFalse)
			c.Assert(runner.MigrateUp(c.Context()), qt.IsNil)
			c.Assert(runner.MigrateDown(c.Context()), qt.IsNil)
			c.Assert(providerTablePresent(c, conn, "records"), qt.IsFalse)
			store, err := conn.HistoryStore()
			c.Assert(err, qt.IsNil)
			snapshot, err := store.Read(c.Context(), dbschema.HistoryTable{Schema: "main", Name: "_revisions", Format: dbschema.NativeHistory})
			c.Assert(err, qt.IsNil)
			c.Assert(snapshot.Records, qt.HasLen, 0)
		})
	}
}

func TestProviderHistoryUnknownCommitDoesNotOverwriteAppliedRevision(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "transactions")
	conn, _ := transactionConnection(c, host, "lost-commit")
	migration := migrator.CreateMigrationFromSQL(1, "committed", `CREATE TABLE "committed" ("id" INTEGER);`, `DROP TABLE "committed";`)
	runner := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("main", "_revisions").WithTransactionMode(migrator.MigrationTxModeFile).
		WithMigrationLog(false).WithoutMigrationLock()
	c.Assert(runner.MigrateUp(c.Context()), qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	c.Assert(providerTablePresent(c, conn, "committed"), qt.IsTrue)
	rows, err := runner.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 1)
	c.Assert(rows[0].State, qt.Equals, "applied")
	c.Assert(rows[0].Error, qt.Equals, "")
	c.Assert(runner.MigrateUp(c.Context()), qt.IsNil)
}

func TestProviderHistoryLostPendingReceiptStopsBeforeSQL(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "transactions")
	conn, _ := transactionConnection(c, host, "lost-history")
	migration := migrator.CreateMigrationFromSQL(1, "blocked", `CREATE TABLE "blocked" ("id" INTEGER);`, `DROP TABLE "blocked";`)
	runner := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("main", "_revisions").WithTransactionMode(migrator.MigrationTxModeFile).
		WithMigrationLog(false).WithoutMigrationLock()
	c.Assert(runner.MigrateUp(c.Context()), qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	c.Assert(providerTablePresent(c, conn, "blocked"), qt.IsFalse)
	rows, err := runner.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 1)
	c.Assert(rows[0].StatementOutcomeUnknown(), qt.IsTrue)
	c.Assert(runner.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNotNil)
	c.Assert(providerTablePresent(c, conn, "blocked"), qt.IsFalse)
}

func TestProviderHistoryRecordsConfirmedAndUnconfirmedRollback(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "transactions")
	for _, test := range []struct {
		name    string
		fault   string
		body    string
		state   string
		unknown bool
	}{
		{"confirmed refusal", "reject", `CREATE TABLE "recovery" ("id" INTEGER);`, "failed", false},
		{"confirmed rollback after partial execution", "", `CREATE TABLE "recovery" ("id" INTEGER); CREATE TABLE "recovery" ("id" INTEGER);`, "failed", false},
		{"lost rollback receipt", "reject-rollback", `CREATE TABLE "recovery" ("id" INTEGER); DROP TABLE refused;`, "pending", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, _ := transactionConnection(c, host, test.fault)
			migration := migrator.CreateMigrationFromSQL(1, "recovery", test.body, `DROP TABLE "recovery";`)
			runner := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
				WithMigrationsTable("main", "_revisions").WithTransactionMode(migrator.MigrationTxModeFile).
				WithMigrationLog(false).WithoutMigrationLock()
			c.Assert(runner.MigrateUp(c.Context()), qt.IsNotNil)
			c.Assert(providerTablePresent(c, conn, "recovery"), qt.IsFalse)
			rows, err := runner.GetRevisions(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(rows, qt.HasLen, 1)
			c.Assert(rows[0].State, qt.Equals, test.state)
			c.Assert(rows[0].Applied, qt.Equals, 0)
			c.Assert(rows[0].StatementOutcomeUnknown(), qt.Equals, test.unknown)
		})
	}
}

func TestProviderHistoryRetriesAConfirmedNonTransactionalRefusal(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "transactions")
	conn, address := transactionConnection(c, host, "reject")
	migration := migrator.CreateMigrationFromSQL(1, "retry", `CREATE TABLE "retry" ("id" INTEGER);`, `DROP TABLE "retry";`)
	migrations := migrator.NewRegisteredMigrationProvider(migration)
	runner := migrator.NewMigrator(conn, migrations).WithMigrationsTable("main", "_revisions").
		WithTransactionMode(migrator.MigrationTxModeNone).WithMigrationLog(false).WithoutMigrationLock()
	c.Assert(runner.MigrateUp(c.Context()), qt.ErrorIs, provider.ErrExecution)
	rows, err := runner.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 1)
	c.Assert(rows[0].State, qt.Equals, "failed")
	c.Assert(rows[0].StatementOutcomeUnknown(), qt.IsFalse)
	cleanAddress, err := url.Parse(address)
	c.Assert(err, qt.IsNil)
	cleanAddress.RawQuery = ""
	fresh := connectTransactionAddress(c, host, cleanAddress.String())
	retry := migrator.NewMigrator(fresh, migrations).WithMigrationsTable("main", "_revisions").
		WithTransactionMode(migrator.MigrationTxModeNone).WithMigrationLog(false).WithoutMigrationLock()
	c.Assert(retry.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)
	c.Assert(providerTablePresent(c, fresh, "retry"), qt.IsTrue)
	rows, err = retry.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 1)
	c.Assert(rows[0].State, qt.Equals, "applied")
}

func TestProviderHistoryUnknownRollbackCommitPreservesDeletion(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "transactions")
	conn, address := transactionConnection(c, host, "")
	migration := migrator.CreateMigrationFromSQL(1, "removed", `CREATE TABLE "removed" ("id" INTEGER);`, `DROP TABLE "removed";`)
	migrations := migrator.NewRegisteredMigrationProvider(migration)
	runner := migrator.NewMigrator(conn, migrations).WithMigrationsTable("main", "_revisions").
		WithTransactionMode(migrator.MigrationTxModeFile).WithMigrationLog(false).WithoutMigrationLock()
	c.Assert(runner.MigrateUp(c.Context()), qt.IsNil)
	faultAddress, err := url.Parse(address)
	c.Assert(err, qt.IsNil)
	faultAddress.RawQuery = url.Values{"fault": {"lost-commit"}}.Encode()
	faulty := connectTransactionAddress(c, host, faultAddress.String())
	rollback := migrator.NewMigrator(faulty, migrations).WithMigrationsTable("main", "_revisions").
		WithTransactionMode(migrator.MigrationTxModeFile).WithMigrationLog(false).WithoutMigrationLock()
	c.Assert(rollback.MigrateDown(c.Context()), qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	c.Assert(providerTablePresent(c, conn, "removed"), qt.IsFalse)
	rows, err := runner.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 0)
}

func TestProviderHistoryFailedCommitReleasesTheLiveTransaction(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "transactions")
	conn, _ := transactionConnection(c, host, "lost-before-commit")
	migration := migrator.CreateMigrationFromSQL(1, "unfinished", `CREATE TABLE "unfinished" ("id" INTEGER);`, `DROP TABLE "unfinished";`)
	runner := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("main", "_revisions").WithTransactionMode(migrator.MigrationTxModeFile).
		WithMigrationLog(false).WithoutMigrationLock()
	c.Assert(runner.MigrateUp(c.Context()), qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	c.Assert(providerTablePresent(c, conn, "unfinished"), qt.IsFalse)
	rows, err := runner.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 1)
	c.Assert(rows[0].StatementOutcomeUnknown(), qt.IsTrue)
	// Another transaction on this session proves cleanup released the active
	// handle. The commit error still requires durable-state recovery.
	tx, err := conn.SchemaWriter().BeginTransaction(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(tx.Rollback(), qt.IsNil)
}
