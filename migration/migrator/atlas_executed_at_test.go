package migrator_test

import (
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// TestAtlasRevisionExecutedAt_ReadsBothStoredTexts reads executed_at back from
// the two texts a SQLite Atlas revision table holds: the UTC text an RFC 3339
// writer stores, and the local time with its offset the pinned community
// binary and Ptah store. Each reads as the instant it names, in the
// zone it names, so a table either writer filled stays readable.
func TestAtlasRevisionExecutedAt_ReadsBothStoredTexts(t *testing.T) {
	tests := []struct {
		name       string
		stored     string
		wantRFC    string
		wantString string
	}{
		{
			name:       "UTC text",
			stored:     "2026-11-18T05:59:32.44384Z",
			wantRFC:    "2026-11-18T05:59:32.44384Z",
			wantString: "2026-11-18 05:59:32.44384 +0000 UTC",
		},
		{
			name:       "offset text",
			stored:     "2026-11-18 14:59:32.08399+09:00",
			wantRFC:    "2026-11-18T14:59:32.08399+09:00",
			wantString: "2026-11-18 14:59:32.08399 +0900 +0900",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := filepath.Join(c.TempDir(), "app.db")
			c.Assert(runAtlasUp(c, dbPath, atlasLinearityFiles("1_a.sql")), qt.IsNil)
			conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			_, err = conn.ExecContext(c.Context(), "UPDATE atlas_schema_revisions SET executed_at = ?", tt.stored)
			c.Assert(err, qt.IsNil)
			mig, err := migrator.NewFSMigrator(conn, atlasLinearityFiles("1_a.sql"), migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
			c.Assert(err, qt.IsNil)

			revisions, err := mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).GetRevisions(c.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(revisions, qt.HasLen, 1)
			c.Assert(revisions[0].AppliedAt.Format(time.RFC3339Nano), qt.Equals, tt.wantRFC)
			c.Assert(revisions[0].AppliedAt.String(), qt.Equals, tt.wantString)
		})
	}
}
