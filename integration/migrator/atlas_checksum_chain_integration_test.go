//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	issue1241RevisionTable = "atlas_schema_revisions_issue_1241"
	issue1241OneFile       = "20240101000000_one.sql"
	issue1241TwoFile       = "20240101500000_two.sql"
	issue1241ThreeFile     = "20240102000000_three.sql"
	issue1241OneSQL        = "CREATE TABLE ptah_issue_1241_one (id BIGINT PRIMARY KEY);\n"
	issue1241TwoSQL        = "CREATE TABLE ptah_issue_1241_two (id BIGINT PRIMARY KEY);\n"
	issue1241ThreeSQL      = "CREATE TABLE ptah_issue_1241_three (id BIGINT PRIMARY KEY);\n"
)

func TestAtlasChecksumChainOutOfOrderPostgresIntegration(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), postgresTestURL(t))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	cleanupIssue1241(t, conn)
	t.Cleanup(func() { cleanupIssue1241(t, conn) })

	initialFS := issue1241AtlasFS(t, map[string]string{
		issue1241OneFile:   issue1241OneSQL,
		issue1241ThreeFile: issue1241ThreeSQL,
	})
	initial := issue1241Migrator(t, conn, initialFS)
	c.Assert(initial.MigrateUp(t.Context()), qt.IsNil)
	lateBefore := issue1241StoredHash(t, conn, "20240102000000")

	expandedFS := issue1241AtlasFS(t, map[string]string{
		issue1241OneFile:   issue1241OneSQL,
		issue1241TwoFile:   issue1241TwoSQL,
		issue1241ThreeFile: issue1241ThreeSQL,
	})
	linear := issue1241Migrator(t, conn, expandedFS)
	var outOfOrder *migrator.OutOfOrderError
	err = linear.MigrateUp(t.Context())
	c.Assert(err, qt.ErrorAs, &outOfOrder)
	c.Assert(tableExists(t, conn, "ptah_issue_1241_two"), qt.IsFalse)
	c.Assert(issue1241StoredHash(t, conn, "20240102000000"), qt.Equals, lateBefore)

	nonLinear := issue1241Migrator(t, conn, expandedFS).WithExecOrder(migrator.ExecOrderNonLinear)
	c.Assert(nonLinear.MigrateUp(t.Context()), qt.IsNil)
	c.Assert(tableExists(t, conn, "ptah_issue_1241_two"), qt.IsTrue)
	c.Assert(
		issue1241StoredHash(t, conn, "20240102000000"),
		qt.Equals,
		issue1241EntryHash(t, expandedFS, issue1241ThreeFile),
	)
	c.Assert(nonLinear.MigrateUp(t.Context()), qt.IsNil)

	editedFS := issue1241AtlasFS(t, map[string]string{
		issue1241OneFile:   "CREATE TABLE ptah_issue_1241_one (id BIGINT PRIMARY KEY, edited TEXT);\n",
		issue1241TwoFile:   issue1241TwoSQL,
		issue1241ThreeFile: issue1241ThreeSQL,
	})
	var mismatch *migrator.ChecksumMismatchError
	err = issue1241Migrator(t, conn, editedFS).
		WithExecOrder(migrator.ExecOrderNonLinear).
		MigrateUp(t.Context())
	c.Assert(err, qt.ErrorAs, &mismatch)
	c.Assert(mismatch.Version, qt.Equals, int64(20240101000000))
}

func issue1241AtlasFS(t *testing.T, files map[string]string) fstest.MapFS {
	c := qt.New(t)
	t.Helper()
	fsys := make(fstest.MapFS, len(files)+1)
	for name, body := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	sum, err := migratesum.ComputeWithFormat(fsys, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	fsys[migratesum.AtlasFileName] = &fstest.MapFile{Data: sum.Bytes()}
	return fsys
}

func issue1241Migrator(
	t *testing.T,
	conn *dbschema.DatabaseConnection,
	fsys fstest.MapFS,
) *migrator.Migrator {
	c := qt.New(t)
	t.Helper()
	m, err := migrator.NewFSMigrator(
		conn,
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	return m.WithMigrationsTable("", issue1241RevisionTable).
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
}

func issue1241StoredHash(t *testing.T, conn *dbschema.DatabaseConnection, version string) string {
	c := qt.New(t)
	t.Helper()
	var hash string
	err := conn.QueryRowContext(
		t.Context(),
		"SELECT hash FROM "+issue1241RevisionTable+" WHERE version = $1",
		version,
	).Scan(&hash)
	c.Assert(err, qt.IsNil)
	return hash
}

func issue1241EntryHash(t *testing.T, fsys fstest.MapFS, name string) string {
	c := qt.New(t)
	t.Helper()
	sum, err := migratesum.Parse(fsys[migratesum.AtlasFileName].Data)
	c.Assert(err, qt.IsNil)
	entries := make(map[string]string, len(sum.Entries))
	for _, entry := range sum.Entries {
		entries[entry.Name] = strings.TrimPrefix(entry.Hash, "h1:")
	}
	return entries[name]
}

func cleanupIssue1241(t *testing.T, conn *dbschema.DatabaseConnection) {
	t.Helper()
	for _, statement := range []string{
		"DROP TABLE IF EXISTS ptah_issue_1241_three",
		"DROP TABLE IF EXISTS ptah_issue_1241_two",
		"DROP TABLE IF EXISTS ptah_issue_1241_one",
		"DROP TABLE IF EXISTS " + issue1241RevisionTable,
	} {
		_, _ = conn.ExecContext(context.Background(), statement)
	}
}
