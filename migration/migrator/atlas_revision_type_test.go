package migrator_test

import (
	"database/sql"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestAtlasRevisionType_String(t *testing.T) {

	tests := []struct {
		name         string
		revisionType migrator.AtlasRevisionType
		want         string
	}{
		{
			name:         "unknown zero value",
			revisionType: migrator.AtlasRevisionTypeUnknown,
			want:         "unknown (0000)",
		},
		{
			name:         "baseline",
			revisionType: migrator.AtlasRevisionTypeBaseline,
			want:         "baseline",
		},
		{
			name:         "applied",
			revisionType: migrator.AtlasRevisionTypeApplied,
			want:         "applied",
		},
		{
			name:         "baseline applied by a compatibility adapter",
			revisionType: migrator.AtlasRevisionTypeBaseline | migrator.AtlasRevisionTypeApplied,
			want:         "applied",
		},
		{
			name:         "manually set",
			revisionType: migrator.AtlasRevisionTypeManuallySet,
			want:         "manually set",
		},
		{
			name: "unknown baseline and manually set combination",
			revisionType: migrator.AtlasRevisionTypeBaseline |
				migrator.AtlasRevisionTypeManuallySet,
			want: "unknown (0101)",
		},
		{
			name: "applied and manually set",
			revisionType: migrator.AtlasRevisionTypeApplied |
				migrator.AtlasRevisionTypeManuallySet,
			want: "applied + manually set",
		},
		{
			name: "manually set converted baseline marker",
			revisionType: migrator.AtlasRevisionTypeBaseline |
				migrator.AtlasRevisionTypeApplied |
				migrator.AtlasRevisionTypeManuallySet,
			want: "manually set",
		},
		{
			name:         "unknown higher bit",
			revisionType: 8,
			want:         "unknown (1000)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tt.revisionType.String(), qt.Equals, tt.want)
		})
	}
}

func TestWithAtlasRevisionTypes_PreservesConvertedBaselineMarker(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		"sqlite://"+filepath.Join(c.TempDir(), "atlas-baseline-type.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"10_base.sql": {Data: []byte("CREATE TABLE baseline_marker (id INTEGER PRIMARY KEY);\n")},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasRevisionVersions(map[int64]string{10: "2"}),
		migrator.WithAtlasRevisionTypes(map[int64]migrator.AtlasRevisionType{
			10: migrator.AtlasRevisionTypeBaseline | migrator.AtlasRevisionTypeApplied,
		}),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)

	var revisionType int
	c.Assert(conn.QueryRowContext(
		c.Context(),
		"SELECT type FROM atlas_schema_revisions WHERE version = '2'",
	).Scan(&revisionType), qt.IsNil)
	c.Assert(revisionType, qt.Equals, int(migrator.AtlasRevisionTypeBaseline|migrator.AtlasRevisionTypeApplied))
}

func TestAtlasExecutedBaselineMarkerDoesNotCreateImplicitHistoryBoundary(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		"sqlite://"+filepath.Join(c.TempDir(), "atlas-executed-baseline-boundary.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"5_lower.sql": {Data: []byte("CREATE TABLE lower_pending (id INTEGER PRIMARY KEY);\n")},
			"10_base.sql": {Data: []byte("CREATE TABLE executed_baseline (id INTEGER PRIMARY KEY);\n")},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasRevisionVersions(map[int64]string{5: "1", 10: "2"}),
		migrator.WithAtlasRevisionTypes(map[int64]migrator.AtlasRevisionType{
			10: migrator.AtlasRevisionTypeBaseline | migrator.AtlasRevisionTypeApplied,
		}),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(mig.Initialize(c.Context()), qt.IsNil)
	_, err = conn.ExecContext(c.Context(), `INSERT INTO atlas_schema_revisions
(version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version)
VALUES ('2', 'base', 3, 0, 0, '2026-08-13T00:00:00Z', 0, '', '', '', NULL, 'Ptah')`)
	c.Assert(err, qt.IsNil)

	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.AppliedMigrationKeys, qt.DeepEquals, []string{"2"})
	c.Assert(status.PendingMigrationKeys, qt.DeepEquals, []string{"1"})
}

func TestMigrationRevision_DirtyFlagInPublicSnapshots(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		t.Context(),
		"sqlite://"+filepath.Join(t.TempDir(), "atlas-revisions.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_create_accounts.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	err = mig.MigrateUp(t.Context())
	c.Assert(err, qt.IsNil)

	_, err = conn.ExecContext(
		t.Context(),
		`UPDATE atlas_schema_revisions
SET applied = 0, total = 1, error = 'broken'
WHERE version = '1'`,
	)
	c.Assert(err, qt.IsNil)

	revisions, err := mig.GetRevisions(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 1)
	c.Assert(revisions[0].State, qt.Equals, "failed")
	c.Assert(revisions[0].Dirty, qt.IsTrue)

	snapshot, err := mig.GetMigrationStatusSnapshot(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(snapshot.Revisions, qt.HasLen, 1)
	c.Assert(snapshot.Revisions[0].State, qt.Equals, "failed")
	c.Assert(snapshot.Revisions[0].Dirty, qt.IsTrue)
	c.Assert(snapshot.Status.DirtyRevision, qt.IsNotNil)
	c.Assert(snapshot.Status.DirtyRevision.Dirty, qt.IsTrue)
}

func TestAtlasMigrationExecutionPreservesStartTimestamp(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		t.Context(),
		"sqlite://"+filepath.Join(t.TempDir(), "atlas-start-time.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_create_users.sql": &fstest.MapFile{Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n")},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	err = mig.Initialize(t.Context())
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(
		t.Context(),
		`CREATE TABLE atlas_revision_start_times (executed_at TEXT NOT NULL);
CREATE TRIGGER capture_atlas_revision_start_time
AFTER INSERT ON atlas_schema_revisions
BEGIN
    INSERT INTO atlas_revision_start_times (executed_at) VALUES (NEW.executed_at);
END;`,
	)
	c.Assert(err, qt.IsNil)

	err = mig.MigrateUp(t.Context())
	c.Assert(err, qt.IsNil)

	var startedAt, storedAt string
	err = conn.QueryRowContext(
		t.Context(),
		`SELECT captured.executed_at, CAST(revisions.executed_at AS TEXT)
FROM atlas_revision_start_times AS captured
CROSS JOIN atlas_schema_revisions AS revisions
WHERE revisions.version = '1'`,
	).Scan(&startedAt, &storedAt)
	c.Assert(err, qt.IsNil)
	c.Assert(storedAt, qt.Equals, startedAt)
}

func TestAtlasMigrationExecutionTimeline_TxModeAllSuccess(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		t.Context(),
		"sqlite://"+filepath.Join(t.TempDir(), "atlas-tx-all-success.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_slow_success.sql": &fstest.MapFile{
				Data: []byte("SELECT length(randomblob(67108864));\n"),
			},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithTransactionMode(migrator.MigrationTxModeAll)

	commandStartedAt := time.Now()
	err = mig.MigrateUp(t.Context())
	commandFinishedAt := time.Now()
	c.Assert(err, qt.IsNil)

	var executedAtText string
	var executionNanos int64
	err = conn.QueryRowContext(
		t.Context(),
		`SELECT CAST(executed_at AS TEXT), execution_time
FROM atlas_schema_revisions
WHERE version = '1'`,
	).Scan(&executedAtText, &executionNanos)
	c.Assert(err, qt.IsNil)

	executedAt, err := time.Parse(time.RFC3339Nano, executedAtText)
	c.Assert(err, qt.IsNil)
	executionTime := time.Duration(executionNanos)
	c.Assert(executedAt.Before(commandStartedAt), qt.IsFalse)
	c.Assert(executionTime >= 10*time.Millisecond, qt.IsTrue)
	c.Assert(executedAt.Add(executionTime).After(commandFinishedAt.Add(5*time.Millisecond)), qt.IsFalse)
}

func TestAtlasMigrationExecutionTimeline_TxModeAllFailure(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		t.Context(),
		"sqlite://"+filepath.Join(t.TempDir(), "atlas-tx-all-failure.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_slow_failure.sql": &fstest.MapFile{
				Data: []byte(`SELECT length(randomblob(67108864));
INSERT INTO missing_table (id) VALUES (1);
`),
			},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithTransactionMode(migrator.MigrationTxModeAll)

	commandStartedAt := time.Now()
	err = mig.MigrateUp(t.Context())
	commandFinishedAt := time.Now()
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "no such table: missing_table")

	var executedAtText string
	var executionNanos int64
	err = conn.QueryRowContext(
		t.Context(),
		`SELECT CAST(executed_at AS TEXT), execution_time
FROM atlas_schema_revisions
WHERE version = '1'`,
	).Scan(&executedAtText, &executionNanos)
	c.Assert(err, qt.IsNil)

	executedAt, err := time.Parse(time.RFC3339Nano, executedAtText)
	c.Assert(err, qt.IsNil)
	executionTime := time.Duration(executionNanos)
	c.Assert(executedAt.Before(commandStartedAt), qt.IsFalse)
	c.Assert(executionTime >= 10*time.Millisecond, qt.IsTrue)
	c.Assert(executedAt.Add(executionTime).After(commandFinishedAt.Add(5*time.Millisecond)), qt.IsFalse)
}

func TestAtlasRollbackFailure_PreservesNormalApplyMetadata(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		t.Context(),
		"sqlite://"+filepath.Join(t.TempDir(), "atlas-rollback.sqlite"),
	)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_create_users.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- migration.sql --
CREATE TABLE users (id INTEGER PRIMARY KEY);

-- down.sql --
DROP TABLE missing_users;
`)},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	err = mig.MigrateUp(t.Context())
	c.Assert(err, qt.IsNil)

	err = mig.MigrateDown(t.Context())
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "no such table: missing_users")

	var description string
	var revisionType int
	var partialHashes sql.NullString
	var partialHashesType string
	err = conn.QueryRowContext(
		t.Context(),
		`SELECT description, type, partial_hashes, typeof(partial_hashes)
FROM atlas_schema_revisions
WHERE version = '1'`,
	).Scan(&description, &revisionType, &partialHashes, &partialHashesType)
	c.Assert(err, qt.IsNil)
	c.Assert(description, qt.Equals, "create_users")
	c.Assert(revisionType, qt.Equals, int(migrator.AtlasRevisionTypeApplied))
	c.Assert(partialHashes, qt.DeepEquals, sql.NullString{String: "null", Valid: true})
	c.Assert(partialHashesType, qt.Equals, "blob")
}
