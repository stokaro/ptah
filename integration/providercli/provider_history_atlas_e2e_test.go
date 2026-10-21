//go:build integration

package providercli_test

import (
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/providerhost"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

func atlasHistoryTable() dbschema.HistoryTable {
	return dbschema.HistoryTable{Schema: "main", Name: "_atlas_revisions", Format: dbschema.AtlasHistory}
}

func atlasHistoryConnections(c *qt.C, host *providerhost.Host) (native, sql *dbschema.DatabaseConnection, address string) {
	c.Helper()
	path := filepath.Join(c.TempDir(), "atlas.db")
	var err error
	sql, err = dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(path))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(sql.Close(), qt.IsNil) })
	address = (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/")}).String()
	return connectTransactionAddress(c, host, address), sql, address
}

func atlasHistoryRunner(conn *dbschema.DatabaseConnection) *migrator.Migrator {
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "first", "CREATE TABLE atlas_first (id INTEGER);", "DROP TABLE atlas_first;"),
		migrator.CreateMigrationFromSQL(2, "second", "CREATE TABLE atlas_second (id INTEGER);", "DROP TABLE atlas_second;"),
	)).WithMigrationsTable("main", atlasHistoryTable().Name).WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithMigrationLog(false).WithoutMigrationLock()
}

func TestProviderAtlasHistoryAdoptsSQLStorageAndPreservesMetadata(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, sql, _ := atlasHistoryConnections(c, host)
	sqlRunner := atlasHistoryRunner(sql)
	_, err := sqlRunner.SetRevision(c.Context(), 1)
	c.Assert(err, qt.IsNil)
	_, err = sql.ExecContext(c.Context(), `INSERT INTO main._atlas_revisions
(version, description, type, applied, total, executed_at, execution_time, hash, partial_hashes, operator_version)
VALUES ('.atlas_cloud_identifier', 'keep metadata', 0, 0, 0, '2024-01-01T00:00:00Z', 0, '', 'null', '')`)
	c.Assert(err, qt.IsNil)
	runner := atlasHistoryRunner(conn)
	rows, err := runner.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 1)
	c.Assert(rows[0].RevisionVersion(), qt.Equals, "1")
	c.Assert(rows[0].State, qt.Equals, "applied")
	store, err := conn.HistoryStore()
	c.Assert(err, qt.IsNil)
	snapshot, err := store.Read(c.Context(), atlasHistoryTable())
	c.Assert(err, qt.IsNil)
	c.Assert(snapshot.Records, qt.HasLen, 2)
	for _, row := range snapshot.Records {
		c.Assert(row.State, qt.Equals, "")
		c.Assert(row.Direction, qt.Equals, "")
	}
	result, err := runner.SetRevision(c.Context(), 2)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Set, qt.HasLen, 1)
	rows, err = sqlRunner.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 2)
	c.Assert(rows[1].AtlasType, qt.Equals, migrator.AtlasRevisionTypeApplied|migrator.AtlasRevisionTypeManuallySet)
	result, err = runner.SetRevision(c.Context(), 0)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Removed, qt.HasLen, 2)
	rows, err = sqlRunner.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 0)
	var metadata string
	c.Assert(sql.QueryRowContext(c.Context(), `SELECT description FROM main._atlas_revisions WHERE version = '.atlas_cloud_identifier'`).Scan(&metadata), qt.IsNil)
	c.Assert(metadata, qt.Equals, "keep metadata")
}

func TestProviderAtlasHistoryAppliesAndRollsBackUsingStoredFacts(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "transactions")
	conn, sql, _ := atlasHistoryConnections(c, host)
	runner := atlasHistoryRunner(conn)
	c.Assert(runner.MigrateUp(c.Context()), qt.IsNil)
	rows, err := atlasHistoryRunner(sql).GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 2)
	for _, row := range rows {
		c.Assert(row.State, qt.Equals, "applied")
		c.Assert(row.Applied, qt.Equals, 1)
		c.Assert(row.Total, qt.Equals, 1)
	}
	c.Assert(providerTablePresent(c, sql, "atlas_first"), qt.IsTrue)
	c.Assert(providerTablePresent(c, sql, "atlas_second"), qt.IsTrue)
	c.Assert(runner.MigrateDown(c.Context()), qt.IsNil)
	rows, err = atlasHistoryRunner(sql).GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 1)
	c.Assert(providerTablePresent(c, sql, "atlas_second"), qt.IsFalse)
	c.Assert(providerTablePresent(c, sql, "atlas_first"), qt.IsTrue)
}

func TestProviderAtlasHistoryPreservesEmptyExactIdentity(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, sql, _ := atlasHistoryConnections(c, host)
	runner, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"10_empty.sql": {Data: []byte("CREATE TABLE empty_body_must_not_run (id INTEGER);\n")},
	}, migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas), migrator.WithAtlasRevisionVersions(map[int64]string{10: ""}))
	c.Assert(err, qt.IsNil)
	runner = runner.WithMigrationsTable("main", atlasHistoryTable().Name).WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithMigrationLog(false).WithoutMigrationLock()
	result, err := runner.SetRevision(c.Context(), 10)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Set, qt.HasLen, 1)
	c.Assert(result.Set[0].RevisionVersion, qt.Equals, "")
	var version string
	c.Assert(sql.QueryRowContext(c.Context(), "SELECT version FROM main._atlas_revisions").Scan(&version), qt.IsNil)
	c.Assert(version, qt.Equals, "")
	result, err = runner.SetRevision(c.Context(), 10)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Set, qt.HasLen, 0)
	result, err = runner.SetRevision(c.Context(), 0)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Removed, qt.HasLen, 1)
	c.Assert(result.Removed[0].RevisionVersion, qt.Equals, "")
}

func TestProviderAtlasHistoryComparisonIncludesExternalSQLChanges(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, sql, _ := atlasHistoryConnections(c, host)
	_, err := atlasHistoryRunner(conn).SetRevision(c.Context(), 1)
	c.Assert(err, qt.IsNil)
	store, err := conn.HistoryStore()
	c.Assert(err, qt.IsNil)
	snapshot, err := store.Read(c.Context(), atlasHistoryTable())
	c.Assert(err, qt.IsNil)
	before := snapshot.Records[0]
	_, err = sql.ExecContext(c.Context(), "UPDATE main._atlas_revisions SET error = ? WHERE version = ?", "external diagnostic", "1")
	c.Assert(err, qt.IsNil)
	c.Assert(store.CompareAndSwap(c.Context(), atlasHistoryTable(), &before, nil), qt.ErrorIs, provider.ErrExecution)
	snapshot, err = store.Read(c.Context(), atlasHistoryTable())
	c.Assert(err, qt.IsNil)
	c.Assert(snapshot.Records[0].Error, qt.Equals, "external diagnostic")
	rows, err := atlasHistoryRunner(conn).GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows[0].State, qt.Equals, "failed")
	c.Assert(rows[0].Dirty, qt.IsTrue)
	c.Assert(store.CompareAndSwap(c.Context(), atlasHistoryTable(), &snapshot.Records[0], nil), qt.IsNil)
}

func TestProviderAtlasHistorySetPreservesDirtyStoredFacts(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, sql, _ := atlasHistoryConnections(c, host)
	runner := atlasHistoryRunner(conn)
	c.Assert(runner.Baseline(c.Context(), 1), qt.IsNil)
	c.Assert(providerTablePresent(c, sql, "atlas_first"), qt.IsFalse)
	_, err := sql.ExecContext(c.Context(), `UPDATE main._atlas_revisions SET
applied = 0, total = 2, error = 'recorded failure', error_stmt = 'recorded statement',
partial_hashes = '["h1:recorded"]', hash = 'recorded hash', operator_version = 'external operator'
WHERE version = '1'`)
	c.Assert(err, qt.IsNil)
	store, err := conn.HistoryStore()
	c.Assert(err, qt.IsNil)
	before, err := store.Read(c.Context(), atlasHistoryTable())
	c.Assert(err, qt.IsNil)
	c.Assert(before.Records, qt.HasLen, 1)
	result, err := runner.SetRevision(c.Context(), 1)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Set, qt.HasLen, 1)
	after, err := store.Read(c.Context(), atlasHistoryTable())
	c.Assert(err, qt.IsNil)
	expected := before.Records[0]
	expected.AtlasType = uint32(migrator.AtlasRevisionTypeApplied | migrator.AtlasRevisionTypeManuallySet)
	c.Assert(after.Records, qt.DeepEquals, []dbschema.HistoryRecord{expected})
	rows, err := atlasHistoryRunner(sql).GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 1)
	c.Assert(rows[0].Dirty, qt.IsTrue)
	c.Assert(rows[0].Error, qt.Equals, "recorded failure")
}

func TestProviderAtlasHistorySetPreservesUncertainCommit(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	for _, test := range []struct {
		fault string
		rows  int
	}{
		{"lost-history", 0},
		{"lost-before-commit", 0},
		{"lost-commit", 2},
	} {
		t.Run(test.fault, func(t *testing.T) {
			c := qt.New(t)
			conn, sql, address := atlasHistoryConnections(c, host)
			c.Assert(atlasHistoryRunner(conn).Initialize(c.Context()), qt.IsNil)
			faultAddress, err := url.Parse(address)
			c.Assert(err, qt.IsNil)
			faultAddress.RawQuery = url.Values{"fault": {test.fault}}.Encode()
			faulty := connectTransactionAddress(c, host, faultAddress.String())
			result, err := atlasHistoryRunner(faulty).SetRevision(c.Context(), 2)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
			c.Assert(result, qt.DeepEquals, migrator.AtlasRevisionSetResult{})
			rows, err := atlasHistoryRunner(sql).GetRevisions(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(rows, qt.HasLen, test.rows)
			fresh := connectTransactionAddress(c, host, address)
			_, err = atlasHistoryRunner(fresh).SetRevision(c.Context(), 2)
			c.Assert(err, qt.IsNil)
			rows, err = atlasHistoryRunner(sql).GetRevisions(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(rows, qt.HasLen, 2)
			c.Assert(providerTablePresent(c, sql, "atlas_first"), qt.IsFalse)
			c.Assert(providerTablePresent(c, sql, "atlas_second"), qt.IsFalse)
		})
	}
}
