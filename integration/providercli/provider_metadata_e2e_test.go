//go:build integration

package providercli_test

import (
	"context"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/providerhost"
	"ptah.run/migration/migrator"
)

func metadataProvider(c *qt.C) *providerhost.Host {
	c.Helper()
	host, _ := yamlProviderCommand(c, "metadata")
	return host
}

// The fixture inspector refuses DDL outside its narrow grammar, including
// ordinary log/tag table definitions. Verify schema effects and stored columns
// through an independent SQL connection rather than claiming full inspection.
func metadataConnections(c *qt.C, host *providerhost.Host, fault string) (native, sql *dbschema.DatabaseConnection, address string) {
	c.Helper()
	path := filepath.Join(c.TempDir(), "metadata.db")
	var err error
	sql, err = dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(path))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(sql.Close(), qt.IsNil) })
	address = (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/"), RawQuery: url.Values{"fault": {fault}}.Encode()}).String()
	return connectTransactionAddress(c, host, address), sql, address
}

func metadataMigrator(conn *dbschema.DatabaseConnection) *migrator.Migrator {
	migration := migrator.CreateMigrationFromSQL(1, "records", `CREATE TABLE "records" ("id" INTEGER);`, `DROP TABLE "records";`)
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).WithMigrationsTable("main", "_revisions").WithActor("release-bot")
}
func TestProviderMetadataMigratorRecordsApplyAndRollback(t *testing.T) {
	c := qt.New(t)
	host := metadataProvider(c)
	for _, mode := range []migrator.MigrationTxMode{migrator.MigrationTxModeFile, migrator.MigrationTxModeNone, migrator.MigrationTxModeAll} {
		t.Run(string(mode), func(t *testing.T) {
			c := qt.New(t)
			conn, sql, _ := metadataConnections(c, host, "")
			runner := metadataMigrator(conn).WithTransactionMode(mode)
			c.Assert(runner.MigrateUp(c.Context()), qt.IsNil)
			c.Assert(runner.RecordMigrationTag(c.Context(), "release", 1), qt.IsNil)
			c.Assert(runner.MigrateDown(c.Context()), qt.IsNil)
			rows, err := runner.GetRevisions(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(rows, qt.HasLen, 0)
			log, err := runner.MigrationLog(c.Context(), 0)
			c.Assert(err, qt.IsNil)
			c.Assert(log, qt.HasLen, 2)
			c.Assert(log[0].Outcome.State, qt.Equals, migrator.MigrationLogRolledBack)
			c.Assert(log[1].Outcome.State, qt.Equals, migrator.MigrationLogApplied)
			c.Assert(log[1].Start.Actor, qt.Equals, "release-bot")
			c.Assert(log[1].Start.ActorSource, qt.Equals, migrator.ActorProvided)
			sqlLog, err := metadataMigrator(sql).MigrationLog(c.Context(), 0)
			c.Assert(err, qt.IsNil)
			c.Assert(sqlLog, qt.DeepEquals, log)
			sqlVersion, err := metadataMigrator(sql).ResolveMigrationTag(c.Context(), "release")
			c.Assert(err, qt.IsNil)
			c.Assert(sqlVersion, qt.Equals, int64(1))
			version, err := runner.ResolveMigrationTag(c.Context(), "release")
			c.Assert(err, qt.IsNil)
			c.Assert(version, qt.Equals, int64(1))
			c.Assert(providerTablePresent(c, sql, "records"), qt.IsFalse)
		})
	}
}
func TestProviderMetadataUnknownLogDoesNotReplaySchema(t *testing.T) {
	c := qt.New(t)
	host := metadataProvider(c)
	conn, sql, address := metadataConnections(c, host, "lost-metadata-write")
	runner := metadataMigrator(conn)
	c.Assert(runner.MigrateUp(c.Context()), qt.IsNil)
	c.Assert(providerTablePresent(c, sql, "records"), qt.IsTrue)
	rows, err := runner.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 1)
	c.Assert(rows[0].State, qt.Equals, "applied")
	log, err := runner.MigrationLog(c.Context(), 0)
	c.Assert(err, qt.IsNil)
	c.Assert(log, qt.HasLen, 1)
	c.Assert(log[0].Outcome.State, qt.Equals, migrator.MigrationLogApplied)
	c.Assert(runner.RecordMigrationTag(c.Context(), "release", 1), qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	fresh := connectTransactionAddress(c, host, address)
	version, err := metadataMigrator(fresh).ResolveMigrationTag(c.Context(), "release")
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(1))
}
func TestProviderMetadataUnknownTagPreventsTransactionCommit(t *testing.T) {
	c := qt.New(t)
	host := metadataProvider(c)
	conn, _ := transactionConnection(c, host, "lost-metadata-write")
	store, err := conn.MigrationTagsStore()
	c.Assert(err, qt.IsNil)
	table := dbschema.MigrationMetadataTable{Schema: "main", Name: "release_tags"}
	c.Assert(store.Prepare(c.Context(), table), qt.IsNil)
	tx, err := conn.SchemaWriter().BeginTransaction(c.Context())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = tx.Rollback() })
	bound, err := conn.WithExecutor(tx).MigrationTagsStore()
	c.Assert(err, qt.IsNil)
	c.Assert(bound.Put(c.Context(), table, dbschema.MigrationTagRecord{Name: "release", Version: 9, RecordedAt: time.Now().UTC()}), qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	c.Assert(tx.Commit(), qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	c.Assert(tx.Rollback(), qt.IsNil)
	_, found, err := store.Lookup(c.Context(), table, "release")
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsFalse)
}
func TestProviderMetadataMissingServiceRefusesNativeFallback(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "locks")
	conn, _ := transactionConnection(c, host, "")
	_, err := conn.MigrationTagsStore()
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	_, err = conn.MigrationLogStore()
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	_, err = metadataMigrator(conn).MigrationTags(c.Context())
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
}
func TestProviderMetadataReaderSurvivesCallerCancellationForCleanup(t *testing.T) {
	c := qt.New(t)
	host := metadataProvider(c)
	conn, _ := transactionConnection(c, host, "")
	store, err := conn.MigrationTagsStore()
	c.Assert(err, qt.IsNil)
	reader, err := store.OpenRead(c.Context(), dbschema.MigrationMetadataTable{Schema: "main", Name: "absent"})
	c.Assert(err, qt.IsNil)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	page, err := reader.Next(ctx, 1)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(page.Records, qt.IsNil)
	c.Assert(reader.Close(c.Context()), qt.IsNil)
	c.Assert(reader.Close(ctx), qt.IsNil)
	_, err = reader.Next(c.Context(), 1)
	c.Assert(err, qt.IsNotNil)
}
func TestProviderMetadataReaderClosesAfterTransactionFinish(t *testing.T) {
	c := qt.New(t)
	host := metadataProvider(c)
	conn, _ := transactionConnection(c, host, "")
	tx, err := conn.SchemaWriter().BeginTransaction(c.Context())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = tx.Rollback() })
	store, err := conn.WithExecutor(tx).MigrationTagsStore()
	c.Assert(err, qt.IsNil)
	reader, err := store.OpenRead(c.Context(), dbschema.MigrationMetadataTable{Schema: "main", Name: "absent"})
	c.Assert(err, qt.IsNil)
	c.Assert(tx.Rollback(), qt.IsNil)
	c.Assert(reader.Close(c.Context()), qt.IsNil)
	c.Assert(reader.Close(c.Context()), qt.IsNil)
}
func TestProviderMetadataReaderClosesAfterSessionClose(t *testing.T) {
	c := qt.New(t)
	host := metadataProvider(c)
	conn, _ := transactionConnection(c, host, "")
	store, err := conn.MigrationTagsStore()
	c.Assert(err, qt.IsNil)
	reader, err := store.OpenRead(c.Context(), dbschema.MigrationMetadataTable{Schema: "main", Name: "absent"})
	c.Assert(err, qt.IsNil)
	c.Assert(conn.Close(), qt.IsNil)
	c.Assert(reader.Close(c.Context()), qt.IsNil)
}
func TestProviderMetadataReaderRejectsCrossPageCorruption(t *testing.T) {
	c := qt.New(t)
	host := metadataProvider(c)
	for _, fault := range []string{"repeat-metadata-record", "change-metadata-existence"} {
		t.Run(fault, func(t *testing.T) {
			c := qt.New(t)
			conn, _ := transactionConnection(c, host, fault)
			store, err := conn.MigrationTagsStore()
			c.Assert(err, qt.IsNil)
			table := dbschema.MigrationMetadataTable{Schema: "main", Name: "tags"}
			c.Assert(store.Prepare(c.Context(), table), qt.IsNil)
			for _, name := range []string{"first", "second"} {
				c.Assert(store.Put(c.Context(), table, dbschema.MigrationTagRecord{Name: name, Version: 1, RecordedAt: time.Now().UTC()}), qt.IsNil)
			}
			reader, err := store.OpenRead(c.Context(), table)
			c.Assert(err, qt.IsNil)
			first, err := reader.Next(c.Context(), 1)
			c.Assert(err, qt.IsNil)
			c.Assert(first.Complete, qt.IsFalse)
			second, err := reader.Next(c.Context(), 1)
			c.Assert(err, qt.IsNotNil)
			c.Assert(second.Records, qt.IsNil)
			c.Assert(reader.Close(c.Context()), qt.IsNil)
		})
	}
}
func TestProviderMetadataCloseFailureDiscardsReadResults(t *testing.T) {
	c := qt.New(t)
	host := metadataProvider(c)
	conn, _ := transactionConnection(c, host, "lost-metadata-close")
	tags, err := metadataMigrator(conn).MigrationTags(c.Context())
	c.Assert(err, qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	c.Assert(tags, qt.IsNil)
}
