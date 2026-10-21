//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

const repairFirstSQL = `CREATE TABLE "repair_first" ("id" INTEGER);`
const repairSecondSQL = `CREATE TABLE "repair_second" ("id" INTEGER);`

func providerRepairRunner(conn *dbschema.DatabaseConnection) (*migrator.Migrator, *migrator.Migration) {
	migration := migrator.CreateMigrationFromSQL(1, "repair", repairFirstSQL+"\n"+repairSecondSQL,
		`DROP TABLE "repair_second"; DROP TABLE "repair_first";`)
	runner := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("main", "_revisions").WithTransactionMode(migrator.MigrationTxModeNone).
		WithMigrationLog(false).WithoutMigrationLock()
	return runner, migration
}

func seedProviderRepair(c *qt.C, conn *dbschema.DatabaseConnection, row dbschema.HistoryRecord) dbschema.HistoryStore {
	c.Helper()
	store, err := conn.HistoryStore()
	c.Assert(err, qt.IsNil)
	c.Assert(store.Prepare(c.Context(), providerHistoryTable()), qt.IsNil)
	c.Assert(store.CompareAndSwap(c.Context(), providerHistoryTable(), nil, &row), qt.IsNil)
	return store
}

func providerRepairRecord(sql, direction string, applied int) dbschema.HistoryRecord {
	record := providerHistoryRecord("1")
	record.State, record.Direction = "failed", direction
	record.Applied, record.Total, record.Error = applied, 2, "confirmed refusal"
	sum := sha256.Sum256([]byte(sql))
	record.Checksum = hex.EncodeToString(sum[:])
	return record
}

func TestProviderHistoryRepairResumesOnlyUncommittedStatements(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, _ := transactionConnection(c, host, "")
	runner, migration := providerRepairRunner(conn)
	store := seedProviderRepair(c, conn, providerRepairRecord(migration.UpSQL, "up", 1))
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), repairFirstSQL), qt.IsNil)
	c.Assert(runner.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 2}), qt.IsNil)
	rows := readProviderHistory(c, store).Records
	c.Assert(rows, qt.HasLen, 1)
	c.Assert(rows[0].State, qt.Equals, "applied")
	c.Assert(rows[0].Applied, qt.Equals, 2)
	c.Assert(rows[0].Error, qt.Equals, "")
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), `INSERT INTO "repair_second" ("id") VALUES (1);`), qt.IsNil)
	c.Assert(runner.MigrateUp(c.Context()), qt.IsNil)
}

func TestProviderHistoryRepairFinishesRecordedRollback(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, _ := transactionConnection(c, host, "")
	runner, migration := providerRepairRunner(conn)
	store := seedProviderRepair(c, conn, providerRepairRecord(migration.DownSQL, "down", 1))
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), repairFirstSQL), qt.IsNil)
	c.Assert(runner.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 2}), qt.IsNil)
	c.Assert(readProviderHistory(c, store).Records, qt.HasLen, 0)
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), repairFirstSQL), qt.IsNil)
}

func TestProviderHistoryRepairRefusesUnknownResumeAndAllowsManualRecovery(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, _ := transactionConnection(c, host, "")
	runner, migration := providerRepairRunner(conn)
	row := providerRepairRecord(migration.UpSQL, "up", 0)
	row.Error = "statement execution outcome is unknown after process interruption"
	store := seedProviderRepair(c, conn, row)
	c.Assert(runner.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 1, Force: true}), qt.ErrorMatches, ".*unknown statement outcome.*")
	c.Assert(readProviderHistory(c, store).Records, qt.DeepEquals, []dbschema.HistoryRecord{row})
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), migration.UpSQL), qt.IsNil)
	c.Assert(runner.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1}), qt.IsNil)
	c.Assert(runner.MigrateUp(c.Context()), qt.IsNil)
}

func TestProviderHistoryRepairRequiresForceWhenNothingRan(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, _ := transactionConnection(c, host, "")
	runner, migration := providerRepairRunner(conn)
	store := seedProviderRepair(c, conn, providerRepairRecord(migration.UpSQL, "up", 0))
	var refusal *migrator.RepairNothingAppliedError
	c.Assert(runner.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1}), qt.ErrorAs, &refusal)
	c.Assert(refusal.Version, qt.Equals, int64(1))
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), migration.UpSQL), qt.IsNil)
	c.Assert(runner.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, Force: true}), qt.IsNil)
	c.Assert(readProviderHistory(c, store).Records[0].State, qt.Equals, "applied")
	c.Assert(runner.MigrateUp(c.Context()), qt.IsNil)
}

func TestProviderHistoryRepairLostReceiptPreservesDurableRecovery(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, address := transactionConnection(c, host, "")
	_, migration := providerRepairRunner(conn)
	seedProviderRepair(c, conn, providerRepairRecord(migration.UpSQL, "up", 0))
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), migration.UpSQL), qt.IsNil)
	c.Assert(conn.Close(), qt.IsNil)
	parsed, err := url.Parse(address)
	c.Assert(err, qt.IsNil)
	parsed.RawQuery = url.Values{"fault": {"lost-history"}}.Encode()
	faulted := connectTransactionAddress(c, host, parsed.String())
	runner, _ := providerRepairRunner(faulted)
	c.Assert(runner.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, Force: true}), qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	c.Assert(faulted.Close(), qt.IsNil)
	fresh := connectTransactionAddress(c, host, address)
	recovered, _ := providerRepairRunner(fresh)
	rows, err := recovered.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 1)
	c.Assert(rows[0].Dirty, qt.IsFalse)
	c.Assert(recovered.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1}), qt.ErrorMatches, ".*not dirty.*")
	c.Assert(recovered.MigrateUp(c.Context()), qt.IsNil)
}

func providerChecksumRunner(c *qt.C, conn *dbschema.DatabaseConnection, files map[string]string) *migrator.Migrator {
	c.Helper()
	fsys := make(fstest.MapFS, len(files)+1)
	for name, body := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	sum, err := migratesum.ComputeWithFormat(fsys, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	fsys[migratesum.AtlasFileName] = &fstest.MapFile{Data: sum.Bytes()}
	runner, err := migrator.NewFSMigrator(conn, fsys, migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	return runner.WithMigrationsTable("main", "_revisions").WithTransactionMode(migrator.MigrationTxModeNone).
		WithExecOrder(migrator.ExecOrderNonLinear).WithMigrationLog(false).WithoutMigrationLock()
}

func providerChecksumFiles() map[string]string {
	return map[string]string{
		"1_one.sql":   `CREATE TABLE "chain_one" ("id" INTEGER);`,
		"3_three.sql": `CREATE TABLE "chain_three" ("id" INTEGER);`,
		"4_four.sql":  `CREATE TABLE "chain_four" ("id" INTEGER);`,
	}
}

func TestProviderHistoryChecksumReconciliationIsAtomic(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, address := transactionConnection(c, host, "")
	files := providerChecksumFiles()
	initial := providerChecksumRunner(c, conn, files)
	c.Assert(initial.MigrateUp(c.Context()), qt.IsNil)
	store, err := conn.HistoryStore()
	c.Assert(err, qt.IsNil)
	before := readProviderHistory(c, store)
	c.Assert(before.Records, qt.HasLen, 3)
	c.Assert(conn.Close(), qt.IsNil)
	parsed, err := url.Parse(address)
	c.Assert(err, qt.IsNil)
	parsed.RawQuery = url.Values{"fault": {"reject-checksum-last"}}.Encode()
	faulted := connectTransactionAddress(c, host, parsed.String())
	files["2_two.sql"] = `CREATE TABLE "chain_two" ("id" INTEGER);`
	applying := providerChecksumRunner(c, faulted, files)
	c.Assert(applying.MigrateUp(c.Context()), qt.ErrorIs, provider.ErrExecution)
	store, err = faulted.HistoryStore()
	c.Assert(err, qt.IsNil)
	after := readProviderHistory(c, store)
	c.Assert(after.Records, qt.HasLen, 4)
	c.Assert(after.Records[2], qt.DeepEquals, before.Records[1])
	c.Assert(after.Records[3], qt.DeepEquals, before.Records[2])
	c.Assert(faulted.Writer().ExecuteSQL(c.Context(), `INSERT INTO "chain_two" ("id") VALUES (1);`), qt.IsNil)
	c.Assert(faulted.Close(), qt.IsNil)
	fresh := connectTransactionAddress(c, host, address)
	completing := providerChecksumRunner(c, fresh, files)
	c.Assert(completing.MigrateUp(c.Context()), qt.IsNil)
	reconcile, err := completing.VerifyAppliedChecksums(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(reconcile, qt.IsFalse)
	store, err = fresh.HistoryStore()
	c.Assert(err, qt.IsNil)
	final := readProviderHistory(c, store)
	c.Assert(final.Records[2].Checksum, qt.Not(qt.Equals), before.Records[1].Checksum)
	c.Assert(final.Records[3].Checksum, qt.Not(qt.Equals), before.Records[2].Checksum)
	c.Assert(completing.MigrateUp(c.Context()), qt.IsNil)
}

func TestProviderHistoryChecksumReconciliationRetainsUnknownCommit(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	for _, test := range []struct {
		fault     string
		committed bool
	}{
		{"lost-before-commit", false},
		{"lost-commit", true},
	} {
		t.Run(test.fault, func(t *testing.T) {
			c := qt.New(t)
			conn, address := transactionConnection(c, host, "")
			files := providerChecksumFiles()
			c.Assert(providerChecksumRunner(c, conn, files).MigrateUp(c.Context()), qt.IsNil)
			store, err := conn.HistoryStore()
			c.Assert(err, qt.IsNil)
			before := readProviderHistory(c, store)
			c.Assert(conn.Close(), qt.IsNil)
			parsed, err := url.Parse(address)
			c.Assert(err, qt.IsNil)
			parsed.RawQuery = url.Values{"fault": {test.fault}}.Encode()
			faulted := connectTransactionAddress(c, host, parsed.String())
			files["2_two.sql"] = `CREATE TABLE "chain_two" ("id" INTEGER);`
			c.Assert(providerChecksumRunner(c, faulted, files).MigrateUp(c.Context()), qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
			store, err = faulted.HistoryStore()
			c.Assert(err, qt.IsNil)
			after := readProviderHistory(c, store)
			c.Assert(after.Records, qt.HasLen, 4)
			c.Assert(after.Records[2].Checksum != before.Records[1].Checksum, qt.Equals, test.committed)
			c.Assert(after.Records[3].Checksum != before.Records[2].Checksum, qt.Equals, test.committed)
			c.Assert(faulted.Close(), qt.IsNil)
			fresh := connectTransactionAddress(c, host, address)
			runner := providerChecksumRunner(c, fresh, files)
			reconcile, err := runner.VerifyAppliedChecksums(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(reconcile, qt.Equals, !test.committed)
			c.Assert(runner.MigrateUp(c.Context()), qt.IsNil)
			reconcile, err = runner.VerifyAppliedChecksums(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(reconcile, qt.IsFalse)
		})
	}
}

func TestProviderHistoryForcedRepairCanInsertMissingRevision(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, _ := transactionConnection(c, host, "")
	runner, migration := providerRepairRunner(conn)
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), migration.UpSQL), qt.IsNil)
	c.Assert(runner.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1}), qt.ErrorMatches, ".*has no revision row.*")
	c.Assert(runner.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, Force: true}), qt.IsNil)
	c.Assert(runner.MigrateUp(c.Context()), qt.IsNil)
}

func TestProviderHistoryDryRunRepairPreservesRecord(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, _ := transactionConnection(c, host, "")
	runner, migration := providerRepairRunner(conn)
	row := providerRepairRecord(migration.UpSQL, "up", 0)
	store := seedProviderRepair(c, conn, row)
	conn.SchemaWriter().SetDryRun(true)
	c.Assert(runner.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, Force: true}), qt.IsNil)
	c.Assert(readProviderHistory(c, store).Records, qt.DeepEquals, []dbschema.HistoryRecord{row})
}
