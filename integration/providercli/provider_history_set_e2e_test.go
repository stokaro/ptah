//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

func providerSetRunner(conn *dbschema.DatabaseConnection) *migrator.Migrator {
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(2, "second", "CREATE TABLE set_second (id INTEGER);", ""),
		migrator.CreateMigrationFromSQL(10, "tenth", "CREATE TABLE set_tenth (id INTEGER); SELECT 10;", ""),
		migrator.CreateMigrationFromSQL(20, "twentieth", "CREATE TABLE set_twentieth (id INTEGER);", ""),
	)).WithMigrationsTable("main", "_revisions").WithMigrationLog(false).WithoutMigrationLock()
}

func TestProviderHistorySetPreservesCleanRowsAndDirtyDiagnostics(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, _ := transactionConnection(c, host, "")
	clean := providerHistoryRecord("2")
	clean.Checksum = "retain clean history"
	store := seedProviderRepair(c, conn, clean)
	dirty := providerRepairRecord("SELECT 10;", "down", 1)
	dirty.Version, dirty.Description, dirty.ErrorStatement = "10", "recorded description", "recorded statement"
	c.Assert(store.CompareAndSwap(c.Context(), providerHistoryTable(), nil, &dirty), qt.IsNil)
	high := providerHistoryRecord("30")
	c.Assert(store.CompareAndSwap(c.Context(), providerHistoryTable(), nil, &high), qt.IsNil)
	runner := providerSetRunner(conn)
	result, err := runner.SetRevision(c.Context(), 20)
	c.Assert(err, qt.IsNil)
	c.Assert(result.CurrentVersion, qt.Equals, int64(20))
	c.Assert(result.Removed, qt.DeepEquals, []migrator.AtlasRevisionChange{{Version: 30, RevisionVersion: "30", Description: high.Description}})
	c.Assert(result.Set, qt.DeepEquals, []migrator.AtlasRevisionChange{
		{Version: 10, RevisionVersion: "10", Description: "tenth"},
		{Version: 20, RevisionVersion: "20", Description: "twentieth"},
	})
	rows := readProviderHistory(c, store).Records
	c.Assert(rows, qt.HasLen, 3)
	byVersion := make(map[string]dbschema.HistoryRecord)
	for _, row := range rows {
		byVersion[row.Version] = row
	}
	c.Assert(byVersion["2"], qt.DeepEquals, clean)
	expected := dirty
	expected.State, expected.Direction, expected.Applied = "applied", "up", dirty.Total
	sum := sha256.Sum256([]byte(runner.MigrationProvider().Migrations()[1].UpSQL))
	expected.Checksum = hex.EncodeToString(sum[:])
	c.Assert(byVersion["10"], qt.DeepEquals, expected)
	c.Assert(byVersion["20"].State, qt.Equals, "applied")
	c.Assert(byVersion["20"].Applied, qt.Equals, 1)
	c.Assert(byVersion["20"].Total, qt.Equals, 1)
	c.Assert(providerTablePresent(c, conn, "set_twentieth"), qt.IsFalse)
	result, err = runner.SetRevision(c.Context(), 20)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Set, qt.HasLen, 0)
	c.Assert(result.Removed, qt.HasLen, 0)
	c.Assert(readProviderHistory(c, store).Records, qt.DeepEquals, rows)
}

func TestProviderHistorySetUsesNumericStorageIdentityAndZeroClearsAll(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, _ := transactionConnection(c, host, "")
	runner := providerMappedSetRunner(c, conn)
	result, err := runner.SetRevision(c.Context(), 10)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Set, qt.HasLen, 1)
	c.Assert(result.Set[0].RevisionVersion, qt.Equals, "10")
	result, err = runner.SetRevision(c.Context(), 10)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Set, qt.HasLen, 0)
	store, err := conn.HistoryStore()
	c.Assert(err, qt.IsNil)
	for _, version := range []string{"-1", "0"} {
		row := providerHistoryRecord(version)
		c.Assert(store.CompareAndSwap(c.Context(), providerHistoryTable(), nil, &row), qt.IsNil)
	}
	result, err = runner.SetRevision(c.Context(), 0)
	c.Assert(err, qt.IsNil)
	c.Assert(result.CurrentVersion, qt.Equals, int64(0))
	c.Assert(result.Removed, qt.HasLen, 3)
	c.Assert(readProviderHistory(c, store).Records, qt.HasLen, 0)
	c.Assert(providerTablePresent(c, conn, "set_body_must_not_run"), qt.IsFalse)
}

func TestProviderHistorySetRefusesInvalidTargetsAndWeakerIsolation(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, _ := transactionConnection(c, host, "")
	row := providerHistoryRecord("30")
	store := seedProviderRepair(c, conn, row)
	runner := providerSetRunner(conn)
	_, err := runner.SetRevision(c.Context(), -1)
	c.Assert(err, qt.ErrorMatches, "migration version must not be negative")
	_, err = runner.SetRevision(c.Context(), 99)
	c.Assert(err, qt.ErrorMatches, `migration with version "99" not found`)
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), "PRAGMA read_uncommitted=1;"), qt.IsNil)
	result, err := runner.SetRevision(c.Context(), 10)
	c.Assert(err, qt.IsNotNil)
	c.Assert(result, qt.DeepEquals, migrator.AtlasRevisionSetResult{})
	c.Assert(readProviderHistory(c, store).Records, qt.DeepEquals, []dbschema.HistoryRecord{row})
	conn.SchemaWriter().SetDryRun(true)
	result, err = runner.SetRevision(c.Context(), 10)
	c.Assert(err, qt.IsNil)
	c.Assert(result.CurrentVersion, qt.Equals, int64(10))
	c.Assert(readProviderHistory(c, store).Records, qt.DeepEquals, []dbschema.HistoryRecord{row})
}

func TestProviderHistorySetFaultsNeverReportSuccessOrReplay(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	for _, test := range []struct {
		fault    string
		versions []string
	}{
		{"lost-history-last", []string{"30"}},
		{"lost-before-commit", []string{"30"}},
		{"lost-commit", []string{"10", "2"}},
	} {
		t.Run(test.fault, func(t *testing.T) {
			c := qt.New(t)
			conn, address := transactionConnection(c, host, "")
			seedProviderRepair(c, conn, providerHistoryRecord("30"))
			faultAddress, err := url.Parse(address)
			c.Assert(err, qt.IsNil)
			faultAddress.RawQuery = url.Values{"fault": {test.fault}}.Encode()
			faulty := connectTransactionAddress(c, host, faultAddress.String())
			result, err := providerSetRunner(faulty).SetRevision(c.Context(), 10)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
			c.Assert(result, qt.DeepEquals, migrator.AtlasRevisionSetResult{})
			store, err := conn.HistoryStore()
			c.Assert(err, qt.IsNil)
			var versions []string
			for _, row := range readProviderHistory(c, store).Records {
				versions = append(versions, row.Version)
			}
			c.Assert(versions, qt.DeepEquals, test.versions)
			fresh := connectTransactionAddress(c, host, address)
			_, err = providerSetRunner(fresh).SetRevision(c.Context(), 10)
			c.Assert(err, qt.IsNil)
			c.Assert(readProviderHistory(c, store).Records, qt.HasLen, 2)
			c.Assert(providerTablePresent(c, fresh, "set_second"), qt.IsFalse)
			c.Assert(providerTablePresent(c, fresh, "set_tenth"), qt.IsFalse)
		})
	}
}
