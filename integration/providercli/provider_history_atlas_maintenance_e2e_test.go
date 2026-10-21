//go:build integration

package providercli_test

import (
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

func atlasRepairRunner(conn *dbschema.DatabaseConnection) *migrator.Migrator {
	runner, migration := providerRepairRunner(conn)
	migration.DownTxMode = migrationfile.FileTxModeNone
	return runner.WithMigrationsTable("main", atlasHistoryTable().Name).
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
}

func readAtlasHistory(c *qt.C, conn *dbschema.DatabaseConnection) dbschema.HistorySnapshot {
	c.Helper()
	store, err := conn.HistoryStore()
	c.Assert(err, qt.IsNil)
	snapshot, err := store.Read(c.Context(), atlasHistoryTable())
	c.Assert(err, qt.IsNil)
	return snapshot
}

func TestProviderAtlasHistoryRepairResumesSQLCommittedPrefix(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, sql, _ := atlasHistoryConnections(c, host)
	c.Assert(sql.Writer().ExecuteSQL(c.Context(), repairSecondSQL), qt.IsNil)
	c.Assert(atlasRepairRunner(sql).MigrateUp(c.Context()), qt.IsNotNil)
	before := readAtlasHistory(c, conn)
	c.Assert(before.Records, qt.HasLen, 1)
	c.Assert(before.Records[0].Applied, qt.Equals, 1)
	c.Assert(before.Records[0].PartialHashes, qt.HasLen, 1)
	c.Assert(sql.Writer().ExecuteSQL(c.Context(), `DROP TABLE "repair_second";`), qt.IsNil)

	changed := migrator.CreateMigrationFromSQL(1, "repair", strings.ReplaceAll(repairFirstSQL, "repair_first", "changed_prefix")+"\n"+repairSecondSQL, "")
	changedRunner := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(changed)).
		WithMigrationsTable("main", atlasHistoryTable().Name).WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithTransactionMode(migrator.MigrationTxModeNone).WithMigrationLog(false).WithoutMigrationLock()
	c.Assert(changedRunner.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 2, Force: true}), qt.ErrorMatches, ".*already committed statement prefix changed.*")
	c.Assert(readAtlasHistory(c, conn), qt.DeepEquals, before)
	c.Assert(providerTablePresent(c, sql, "changed_prefix"), qt.IsFalse)

	runner := atlasRepairRunner(conn)
	c.Assert(runner.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 2}), qt.IsNil)
	rows, err := atlasRepairRunner(sql).GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 1)
	c.Assert(rows[0].Dirty, qt.IsFalse)
	c.Assert(rows[0].Applied, qt.Equals, 2)
	c.Assert(rows[0].Error, qt.Equals, "")
	c.Assert(providerTablePresent(c, sql, "repair_first"), qt.IsTrue)
	c.Assert(providerTablePresent(c, sql, "repair_second"), qt.IsTrue)
	c.Assert(runner.MigrateUp(c.Context()), qt.IsNil)
}

func TestProviderAtlasHistoryRepairFinishesSQLRollback(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, sql, _ := atlasHistoryConnections(c, host)
	sqlRunner := atlasRepairRunner(sql)
	c.Assert(sqlRunner.MigrateUp(c.Context()), qt.IsNil)
	c.Assert(sql.Writer().ExecuteSQL(c.Context(), `DROP TABLE "repair_first";`), qt.IsNil)
	c.Assert(sqlRunner.MigrateDown(c.Context()), qt.ErrorMatches, "(?s).*no such table.*repair_first.*")
	before := readAtlasHistory(c, conn)
	c.Assert(before.Records, qt.HasLen, 1)
	c.Assert(before.Records[0].Applied, qt.Equals, 1)
	c.Assert(before.Records[0].PartialHashes, qt.HasLen, 1)
	runner := atlasRepairRunner(conn)
	rows, err := runner.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows[0].Direction, qt.Equals, migrator.MigrationDirectionDown)
	c.Assert(rows[0].Dirty, qt.IsTrue)
	c.Assert(runner.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1}), qt.IsNotNil)
	c.Assert(readAtlasHistory(c, conn), qt.DeepEquals, before)
	c.Assert(sql.Writer().ExecuteSQL(c.Context(), repairFirstSQL), qt.IsNil)
	c.Assert(runner.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 2}), qt.IsNil)
	c.Assert(readAtlasHistory(c, conn).Records, qt.HasLen, 0)
	c.Assert(providerTablePresent(c, sql, "repair_first"), qt.IsFalse)
	c.Assert(providerTablePresent(c, sql, "repair_second"), qt.IsFalse)
}

func TestProviderAtlasHistoryRepairRefusesUnknownResume(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, sql, _ := atlasHistoryConnections(c, host)
	runner := atlasRepairRunner(conn)
	c.Assert(runner.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, Force: true}), qt.IsNil)
	_, err := sql.ExecContext(c.Context(), `UPDATE main._atlas_revisions SET applied = 0, error = ?`, "statement execution outcome is unknown after process interruption")
	c.Assert(err, qt.IsNil)
	before := readAtlasHistory(c, conn)
	c.Assert(runner.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 1, Force: true}), qt.ErrorMatches, ".*unknown statement outcome.*")
	c.Assert(readAtlasHistory(c, conn), qt.DeepEquals, before)
	c.Assert(providerTablePresent(c, sql, "repair_first"), qt.IsFalse)
	c.Assert(sql.Writer().ExecuteSQL(c.Context(), repairFirstSQL+"\n"+repairSecondSQL), qt.IsNil)
	c.Assert(runner.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, Force: true}), qt.IsNil)
	rows, err := atlasRepairRunner(sql).GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows[0].Dirty, qt.IsFalse)
	c.Assert(runner.MigrateUp(c.Context()), qt.IsNil)
}

func atlasChecksumRunner(c *qt.C, conn *dbschema.DatabaseConnection, files map[string]string) *migrator.Migrator {
	c.Helper()
	return providerChecksumRunner(c, conn, files).WithMigrationsTable("main", atlasHistoryTable().Name).
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
}

func TestProviderAtlasHistoryChecksumBatchRecovery(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	for _, test := range []struct {
		fault     string
		committed bool
		cause     error
	}{
		{"reject-checksum-last", false, provider.ErrExecution},
		{"lost-before-commit", false, ptaherr.ErrOutcomeUnknown},
		{"lost-commit", true, ptaherr.ErrOutcomeUnknown},
	} {
		t.Run(test.fault, func(t *testing.T) {
			c := qt.New(t)
			conn, sql, address := atlasHistoryConnections(c, host)
			files := providerChecksumFiles()
			c.Assert(atlasChecksumRunner(c, conn, files).MigrateUp(c.Context()), qt.IsNil)
			before := readAtlasHistory(c, conn)
			c.Assert(before.Records, qt.HasLen, 3)
			c.Assert(conn.Close(), qt.IsNil)
			parsed, err := url.Parse(address)
			c.Assert(err, qt.IsNil)
			parsed.RawQuery = url.Values{"fault": {test.fault}}.Encode()
			faulty := connectTransactionAddress(c, host, parsed.String())
			files["2_two.sql"] = `CREATE TABLE "chain_two" ("id" INTEGER);`
			c.Assert(atlasChecksumRunner(c, faulty, files).MigrateUp(c.Context()), qt.ErrorIs, test.cause)
			after := readAtlasHistory(c, faulty)
			c.Assert(after.Records, qt.HasLen, 4)
			c.Assert(after.Records[2].Checksum != before.Records[1].Checksum, qt.Equals, test.committed)
			c.Assert(after.Records[3].Checksum != before.Records[2].Checksum, qt.Equals, test.committed)
			c.Assert(providerTablePresent(c, sql, "chain_two"), qt.IsTrue)
			c.Assert(faulty.Close(), qt.IsNil)
			fresh := connectTransactionAddress(c, host, address)
			recovered := atlasChecksumRunner(c, fresh, files)
			reconcile, err := recovered.VerifyAppliedChecksums(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(reconcile, qt.Equals, !test.committed)
			c.Assert(recovered.MigrateUp(c.Context()), qt.IsNil)
			reconcile, err = atlasChecksumRunner(c, sql, files).VerifyAppliedChecksums(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(reconcile, qt.IsFalse)
			files["1_one.sql"] = `CREATE TABLE "edited_source" ("id" INTEGER);`
			_, err = atlasChecksumRunner(c, fresh, files).VerifyAppliedChecksums(c.Context())
			c.Assert(err, qt.IsNotNil)
		})
	}
}

func TestProviderAtlasHistoryRepairAfterProcessDeath(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "interrupted.db")
	sql, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(path))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(sql.Close(), qt.IsNil) })
	address := &url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/")}
	crashAddress := *address
	crashAddress.RawQuery = url.Values{"fault": {"crash-after"}}.Encode()
	host, _ := yamlProviderCommand(c, "history-transactions")
	ctx, err := dbschema.WithNativeConnections(c.Context(), host.Connections())
	c.Assert(err, qt.IsNil)
	crashed, err := dbschema.ConnectToDatabase(ctx, crashAddress.String())
	c.Assert(err, qt.IsNil)
	// Closing a handle after its provider exits reports the lost connection.
	c.Cleanup(func() { c.Check(crashed.Close(), qt.IsNotNil) })
	c.Assert(atlasRepairRunner(crashed).MigrateUp(c.Context()), qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	c.Assert(providerTablePresent(c, sql, "repair_first"), qt.IsTrue)
	c.Assert(providerTablePresent(c, sql, "repair_second"), qt.IsFalse)
	rows, err := atlasRepairRunner(sql).GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 1)
	c.Assert(rows[0].Dirty, qt.IsTrue)
	c.Assert(rows[0].Applied, qt.Equals, 0)
	c.Assert(rows[0].Error, qt.Contains, "outcome is unknown")

	freshHost, _ := yamlProviderCommand(c, "history-transactions")
	fresh := connectTransactionAddress(c, freshHost, address.String())
	runner := atlasRepairRunner(fresh)
	before := readAtlasHistory(c, fresh)
	c.Assert(runner.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 1, Force: true}), qt.ErrorMatches, ".*unknown statement outcome.*")
	c.Assert(readAtlasHistory(c, fresh), qt.DeepEquals, before)
	c.Assert(providerTablePresent(c, sql, "repair_second"), qt.IsFalse)
	// The operator establishes the missing schema before repairing metadata.
	c.Assert(sql.Writer().ExecuteSQL(c.Context(), repairSecondSQL), qt.IsNil)
	c.Assert(runner.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, Force: true}), qt.IsNil)
	c.Assert(runner.MigrateUp(c.Context()), qt.IsNil)
	rows, err = atlasRepairRunner(sql).GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows[0].Dirty, qt.IsFalse)
	c.Assert(rows[0].Applied, qt.Equals, 2)
}
