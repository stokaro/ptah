package migrator_test

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// atlasSQLFiles is a directory of the files named, each holding its body.
func atlasSQLFiles(files map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{}
	for name, body := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	return fsys
}

// TestAtlasPositions_ACheckpointNamedFirstRunsFirst applies a fresh database
// whose checkpoint 00_snapshot.sql sorts before 1_a.sql and 2_b.sql. A key
// above every number, given to 00 alone, puts the checkpoint after both files,
// and a fresh run bootstraps from it and skips them.
func TestAtlasPositions_ACheckpointNamedFirstRunsFirst(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "app.db")
	fsys := atlasSQLFiles(map[string]string{
		"00_snapshot.sql": "-- atlas:checkpoint\n\nCREATE TABLE snapshot (id int);\n",
		"1_a.sql":         "CREATE TABLE ta (id int);\n",
		"2_b.sql":         "CREATE TABLE tb (id int);\n",
	})

	err := runAtlasUp(c, dbPath, fsys)

	c.Assert(err, qt.IsNil)
	c.Assert(atlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{"00", "1", "2"})
}

// TestAtlasPositions_TheHighestNumberBesideAVersionString applies a file
// whose name spells the highest number an int64 holds beside v1_y.sql. A key
// one above every number wraps there and filters the file out as below the
// floor, so the run reports success without applying it.
func TestAtlasPositions_TheHighestNumberBesideAVersionString(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "app.db")

	err := runAtlasUp(c, dbPath, atlasLinearityFiles("9223372036854775807_x.sql", "v1_y.sql"))

	c.Assert(err, qt.IsNil)
	c.Assert(atlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{"9223372036854775807", "v1"})
}

// TestAtlasPositions_NumberAboveInt64OverANumericTable_FailurePath refuses a
// file whose name spells a number no int64 holds over the Ptah revision
// table. Its version is a string, and the table would keep the file under its
// position, which names another file once a file is added before it.
func TestAtlasPositions_NumberAboveInt64OverANumericTable_FailurePath(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "app.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	mig, err := migrator.NewFSMigrator(conn, atlasLinearityFiles("9223372036854775808_x.sql"),
		migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)

	err = mig.MigrateUp(c.Context())

	c.Assert(err, qt.ErrorMatches, `(?s).*Atlas migration 9223372036854775808_x\.sql has version `+
		`"9223372036854775808", which a revision table that records numbers cannot keep.*`)
}

// TestAtlasPositions_SetReadsExecutionOrder sets the revision in a directory
// holding 1R__rep.sql beside 1_a.sql. The two spell the number 1, so they run
// under their positions in file-name order, 1R__rep.sql first, and a set
// records the files up to the one named in that order, never both under one
// number.
func TestAtlasPositions_SetReadsExecutionOrder(t *testing.T) {
	tests := []struct {
		version string
		want    []string
	}{
		{version: "1R", want: []string{"1R"}},
		{version: "1", want: []string{"1", "1R"}},
	}
	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			c := qt.New(t)
			dbPath := filepath.Join(c.TempDir(), "app.db")
			mig := atlasUpOrderMigrator(c, dbPath, []string{"1R__rep.sql", "1_a.sql"}, migrator.ExecOrderLinear)
			version, err := mig.ResolveVersion(tt.version)
			c.Assert(err, qt.IsNil)

			_, err = mig.SetRevision(c.Context(), version)

			c.Assert(err, qt.IsNil)
			c.Assert(atlasRevisionVersions(c, dbPath), qt.DeepEquals, tt.want)
		})
	}
}

// TestMigratorRecoveryFlags names the flags a recovery command needs to read
// the same directory and revision table: --dir-format atlas only where the
// default directory format reads other files, here the Ptah pair beside
// v1_x.sql, and --revision-format atlas for an Atlas revision table.
func TestMigratorRecoveryFlags(t *testing.T) {
	ptahPair := map[string]string{
		"0000000001_a.up.sql":   "CREATE TABLE a (id int);\n",
		"0000000001_a.down.sql": "DROP TABLE a;\n",
		"v1_x.sql":              "CREATE TABLE v (id int);\n",
	}
	atlasOnly := map[string]string{"1_a.sql": "CREATE TABLE a (id int);\n"}
	tests := []struct {
		name   string
		files  map[string]string
		format migrationfile.DirFormat
		table  migrator.RevisionTableFormat
		want   string
	}{
		{name: "defaults", files: atlasOnly, format: migrationfile.DirFormatAuto, table: migrator.RevisionTableFormatPtah},
		{
			name: "atlas files the default reads", files: atlasOnly, format: migrationfile.DirFormatAtlas,
			table: migrator.RevisionTableFormatAtlas, want: " --revision-format atlas",
		},
		{
			name: "atlas files only the flag reads", files: ptahPair, format: migrationfile.DirFormatAtlas,
			table: migrator.RevisionTableFormatAtlas, want: " --dir-format atlas --revision-format atlas",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "app.db"))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			mig, err := migrator.NewFSMigrator(conn, atlasSQLFiles(tt.files), migrator.WithMigrationDirFormat(tt.format))
			c.Assert(err, qt.IsNil)

			got := mig.WithRevisionTableFormat(tt.table).RecoveryFlags()

			c.Assert(got, qt.Equals, tt.want)
		})
	}
}
