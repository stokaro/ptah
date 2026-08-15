package migrator_test

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestMigrationStatusDistinguishesExactEmptyIdentityFromNoHistory(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		"sqlite://"+filepath.Join(c.TempDir(), "empty-revision-identity.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"10_repeat.sql": {Data: []byte("CREATE TABLE empty_identity (id INTEGER PRIMARY KEY);\n")},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasRevisionVersions(map[int64]string{10: ""}),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)

	before, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(before.CurrentVersionKey, qt.Equals, "")
	c.Assert(before.CurrentVersionKeySet, qt.IsFalse)

	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
	after, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(after.CurrentVersionKey, qt.Equals, "")
	c.Assert(after.CurrentVersionKeySet, qt.IsTrue)
	c.Assert(after.AppliedMigrationKeys, qt.DeepEquals, []string{""})

	encoded, err := json.Marshal(after)
	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Contains, `"current_version_key":""`)
	c.Assert(string(encoded), qt.Not(qt.Contains), "current_version_key_set")
	var decodedStatus migrator.MigrationStatus
	c.Assert(json.Unmarshal(encoded, &decodedStatus), qt.IsNil)
	c.Assert(decodedStatus.CurrentVersionKey, qt.Equals, "")
	c.Assert(decodedStatus.CurrentVersionKeySet, qt.IsTrue)

	revisions, err := mig.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 1)
	encodedRevision, err := json.Marshal(revisions[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(encodedRevision), qt.Contains, `"atlas_version":""`)
	var decodedRevision migrator.MigrationRevision
	c.Assert(json.Unmarshal(encodedRevision, &decodedRevision), qt.IsNil)
	c.Assert(decodedRevision.RevisionVersion(), qt.Equals, "")
	reencodedRevision, err := json.Marshal(decodedRevision)
	c.Assert(err, qt.IsNil)
	c.Assert(string(reencodedRevision), qt.Contains, `"atlas_version":""`)
}

func TestAtlasRevisionIdentityCollationRefusesAliasesBeforeMigrationSQL(t *testing.T) {
	c := qt.New(t)
	conn, mig := newAtlasAliasCollationMigrator(
		c,
		"source-aliases.sqlite",
		fstest.MapFS{
			"10_upper.sql": {Data: []byte("CREATE TABLE identity_upper (id INTEGER PRIMARY KEY);\n")},
			"20_lower.sql": {Data: []byte("CREATE TABLE identity_lower (id INTEGER PRIMARY KEY);\n")},
		},
		map[int64]string{10: "A", 20: "a"},
	)

	c.Assert(mig.MigrateUp(c.Context()), qt.ErrorMatches,
		`failed to initialize migrations table: revision table cannot distinguish every exact Atlas identity under its configured version collation: 2 identities collapse to 1`)
	assertAtlasAliasBodiesAbsent(c, conn)
}

func TestAtlasRevisionIdentityCollationRefusesRecordedAliasBeforeMigrationSQL(t *testing.T) {
	c := qt.New(t)
	conn, mig := newAtlasAliasCollationMigrator(
		c,
		"recorded-alias.sqlite",
		fstest.MapFS{
			"10_upper.sql": {Data: []byte("CREATE TABLE identity_upper (id INTEGER PRIMARY KEY);\n")},
		},
		map[int64]string{10: "A"},
	)
	_, err := conn.ExecContext(c.Context(), `INSERT INTO atlas_schema_revisions
(version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version)
VALUES ('a', 'recorded alias', 2, 1, 1, CURRENT_TIMESTAMP, 0, NULL, NULL, '', NULL, 'Atlas')`)
	c.Assert(err, qt.IsNil)

	c.Assert(mig.MigrateUp(c.Context()), qt.ErrorMatches,
		`failed to initialize migrations table: revision table cannot distinguish every exact Atlas identity under its configured version collation: 2 identities collapse to 1`)
	assertAtlasAliasBodiesAbsent(c, conn)
}

func TestAtlasRevisionIdentityCollationScalesPastSQLiteCompoundLimit(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		"sqlite://"+filepath.Join(c.TempDir(), "many-revision-identities.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	fsys, versions := atlasIdentityScaleFixture(500)
	mig, err := migrator.NewFSMigrator(
		conn,
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasRevisionVersions(versions),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)

	c.Assert(mig.Initialize(c.Context()), qt.IsNil)
}

func TestAtlasRevisionIdentityCollationRefusesAliasesAcrossBlocksBeforeMigrationSQL(t *testing.T) {
	c := qt.New(t)
	fsys, versions := atlasIdentityCrossBlockAliasFixture(401)
	conn, mig := newAtlasAliasCollationMigrator(c, "cross-block-aliases.sqlite", fsys, versions)

	c.Assert(mig.MigrateUp(c.Context()), qt.ErrorMatches,
		`failed to initialize migrations table: revision table cannot distinguish every exact Atlas identity under its configured version collation: 201 identities collapse to 200`)
	assertAtlasAliasBodiesAbsent(c, conn)
}

func atlasIdentityScaleFixture(count int) (fstest.MapFS, map[int64]string) {
	fsys := make(fstest.MapFS, count)
	versions := make(map[int64]string, count)
	for index := 1; index <= count; index++ {
		version := int64(index)
		fsys[fmt.Sprintf("%d_migration.sql", version)] = &fstest.MapFile{Data: []byte("SELECT 1;\n")}
		versions[version] = fmt.Sprintf("token-%03d", version)
	}
	return fsys, versions
}

func atlasIdentityCrossBlockAliasFixture(count int) (fstest.MapFS, map[int64]string) {
	fsys := make(fstest.MapFS, count)
	versions := make(map[int64]string, count)
	for index := 1; index <= count; index++ {
		version := int64(index)
		identity := fmt.Sprintf("B-%03d", index)
		body := "SELECT 1;\n"
		if index == 1 {
			identity = "A"
			body = "CREATE TABLE identity_upper (id INTEGER PRIMARY KEY);\n"
		}
		if index == count {
			identity = "a"
			body = "CREATE TABLE identity_lower (id INTEGER PRIMARY KEY);\n"
		}
		fsys[fmt.Sprintf("%d_migration.sql", version)] = &fstest.MapFile{Data: []byte(body)}
		versions[version] = identity
	}
	return fsys, versions
}

func newAtlasAliasCollationMigrator(
	c *qt.C,
	database string,
	fsys fstest.MapFS,
	versions map[int64]string,
) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), database))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	_, err = conn.ExecContext(c.Context(), `CREATE TABLE atlas_schema_revisions (
	version VARCHAR(255) COLLATE NOCASE PRIMARY KEY,
	description TEXT NOT NULL,
	type BIGINT NOT NULL DEFAULT 2,
	applied BIGINT NOT NULL DEFAULT 0,
	total BIGINT NOT NULL DEFAULT 0,
	executed_at TIMESTAMP NOT NULL,
	execution_time BIGINT NOT NULL,
	error TEXT NULL,
	error_stmt TEXT NULL,
	hash VARCHAR(255) NOT NULL,
	partial_hashes JSON NULL,
	operator_version VARCHAR(255) NOT NULL
)`)
	c.Assert(err, qt.IsNil)
	mig, err := migrator.NewFSMigrator(
		conn,
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasRevisionVersions(versions),
	)
	c.Assert(err, qt.IsNil)
	return conn, mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
}

func assertAtlasAliasBodiesAbsent(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	var bodyTables int
	c.Assert(conn.QueryRowContext(c.Context(), `SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'table' AND name IN ('identity_upper', 'identity_lower')`).Scan(&bodyTables), qt.IsNil)
	c.Assert(bodyTables, qt.Equals, 0)
}

func TestRepairMigrationFindsMappedExactRevisionIdentity(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		"sqlite://"+filepath.Join(c.TempDir(), "mapped-repair.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"10_mapped.sql": {Data: []byte("CREATE TABLE mapped_repair (id INTEGER PRIMARY KEY);\n")},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasRevisionVersions(map[int64]string{10: "1.5"}),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(mig.Initialize(c.Context()), qt.IsNil)
	_, err = conn.ExecContext(c.Context(), `INSERT INTO atlas_schema_revisions
(version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version)
VALUES ('1.5', 'mapped', 2, 0, 1, '2026-08-13T00:00:00Z', 0, 'broken', '', '', NULL, 'Ptah')`)
	c.Assert(err, qt.IsNil)

	c.Assert(mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{
		Version: 10,
		Force:   true,
	}), qt.IsNil)

	var version string
	var applied, total int
	c.Assert(conn.QueryRowContext(
		c.Context(),
		"SELECT version, applied, total FROM atlas_schema_revisions",
	).Scan(&version, &applied, &total), qt.IsNil)
	c.Assert(version, qt.Equals, "1.5")
	c.Assert(applied, qt.Equals, total)
}

func TestMigrationStatusKeepsSquashedDotIdentityAsHistoryOnly(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		"sqlite://"+filepath.Join(c.TempDir(), "squashed-dot-history.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"20_baseline.sql": {Data: []byte("CREATE TABLE current_baseline (id INTEGER PRIMARY KEY);\n")},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasRevisionVersions(map[int64]string{
			10: ".foo",
			20: "2",
		}),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(mig.Initialize(c.Context()), qt.IsNil)
	_, err = conn.ExecContext(c.Context(), `INSERT INTO atlas_schema_revisions
(version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version)
VALUES ('.foo', 'squashed dot history', 2, 1, 1, '2026-08-13T00:00:00Z', 0, NULL, NULL, '', NULL, 'Atlas')`)
	c.Assert(err, qt.IsNil)

	revisions, err := mig.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 1)
	c.Assert(revisions[0].Version, qt.Equals, int64(10))
	c.Assert(revisions[0].RevisionVersion(), qt.Equals, ".foo")
	version, err := mig.GetCurrentVersion(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(10))
	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{10})
	c.Assert(status.PendingMigrations, qt.DeepEquals, []int64{20})
}

func TestMigrationStatusReportsCurrentFromRetiredExactIdentityOrder(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		"sqlite://"+filepath.Join(c.TempDir(), "retired-current.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"10_remaining.sql": {Data: []byte("CREATE TABLE remaining (id INTEGER PRIMARY KEY);\n")},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasRevisionVersions(map[int64]string{10: "1"}),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(mig.Initialize(c.Context()), qt.IsNil)
	_, err = conn.ExecContext(c.Context(), `INSERT INTO atlas_schema_revisions
(version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version)
VALUES
('1', 'remaining.sql', 2, 1, 1, '2026-08-13T00:00:00Z', 0, NULL, NULL, '', NULL, 'Ptah/source-identity'),
('2', 'removed.sql', 2, 1, 1, '2026-08-13T00:00:00Z', 0, NULL, NULL, '', NULL, 'Ptah/source-identity')`)
	c.Assert(err, qt.IsNil)

	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersionKey, qt.Equals, "2")
	c.Assert(status.CurrentVersionKeySet, qt.IsTrue)
}

func TestRetiredExactIdentityNamesCorruptRevisionMetadata(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		"sqlite://"+filepath.Join(c.TempDir(), "retired-corrupt.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"10_remaining.sql": {Data: []byte("CREATE TABLE remaining (id INTEGER PRIMARY KEY);\n")},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasRevisionVersions(map[int64]string{10: "1"}),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(mig.Initialize(c.Context()), qt.IsNil)
	_, err = conn.ExecContext(c.Context(), `INSERT INTO atlas_schema_revisions
(version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version)
VALUES ('retired', 'removed.sql', 2, -1, 1, '2026-08-13T00:00:00Z', 0, NULL, NULL, '', NULL, 'Ptah/source-identity')`)
	c.Assert(err, qt.IsNil)

	_, err = mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.ErrorMatches, `failed to get migration revisions: failed to scan migration revision: migration retired cannot read revision metadata: revision metadata records invalid progress applied=-1 total=1; inspect the database before choosing a repair point`)
}

func TestNativeRevisionStatusKeepsNumericOrderWithAtlasSourceMapping(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		"sqlite://"+filepath.Join(c.TempDir(), "native-order.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"2_two.sql":  {Data: []byte("CREATE TABLE version_two (id INTEGER PRIMARY KEY);\n")},
			"10_ten.sql": {Data: []byte("CREATE TABLE version_ten (id INTEGER PRIMARY KEY);\n")},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasRevisionVersions(map[int64]string{2: "2", 10: "10"}),
	)
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)

	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(10))
	c.Assert(status.CurrentVersionKey, qt.Equals, "10")
}
