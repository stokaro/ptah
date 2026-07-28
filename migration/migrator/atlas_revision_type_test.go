package migrator_test

import (
	"database/sql"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestAtlasRevisionType_String(t *testing.T) {
	c := qt.New(t)

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
			name:         "unknown baseline and applied combination",
			revisionType: migrator.AtlasRevisionTypeBaseline | migrator.AtlasRevisionTypeApplied,
			want:         "unknown (0011)",
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
			name: "unknown baseline applied and manually set combination",
			revisionType: migrator.AtlasRevisionTypeBaseline |
				migrator.AtlasRevisionTypeApplied |
				migrator.AtlasRevisionTypeManuallySet,
			want: "unknown (0111)",
		},
		{
			name:         "unknown higher bit",
			revisionType: 8,
			want:         "unknown (1000)",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(tt.revisionType.String(), qt.Equals, tt.want)
		})
	}
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
	err = conn.QueryRowContext(
		t.Context(),
		`SELECT description, type, partial_hashes
FROM atlas_schema_revisions
WHERE version = '1'`,
	).Scan(&description, &revisionType, &partialHashes)
	c.Assert(err, qt.IsNil)
	c.Assert(description, qt.Equals, "Create Users")
	c.Assert(revisionType, qt.Equals, int(migrator.AtlasRevisionTypeApplied))
	c.Assert(partialHashes.Valid, qt.IsFalse)
}
