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

// TestAtlasRepeatableOverANumericTable_FailurePath refuses R__r.sql beside
// numbered files over the Ptah revision table, before anything runs. The
// Atlas format runs it once as the version R, under the key above every
// numbered file, and the table would record that key: the next status reads
// R as pending and the key as a migration with no file. Without the refusal
// the run applies both files and the table holds the rows 1 and 2.
func TestAtlasRepeatableOverANumericTable_FailurePath(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "app.db")
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	mig, err := migrator.NewFSMigrator(conn, atlasSQLFiles(map[string]string{
		"1_a.sql":  "CREATE TABLE ta (id int);\n",
		"R__r.sql": "CREATE TABLE tr (id int);\n",
	}), migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)

	err = mig.MigrateUp(c.Context())
	_, statusErr := mig.GetMigrationStatus(c.Context())

	c.Assert(err, qt.ErrorMatches, `(?s).*Atlas migration R__r\.sql has version "R", which a revision table that records `+
		`numbers cannot keep; use the Atlas revision table \(--revision-format atlas\), or Ptah's file names, under `+
		`which R__<name>\.sql is a repeatable migration`)
	c.Assert(statusErr, qt.ErrorMatches, `(?s).*Atlas migration R__r\.sql has version "R".*`)
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

// TestAtlasPositions_UpAndDownFilesShareAPosition runs 1_a.up.sql and
// 1_a.down.sql as one migration in a directory keyed by position, which
// 00_z.sql makes it: the migration runs under the position both files carry,
// up applies 00_z.sql and the two pairs under the rows 00, 1 and 2, and a
// rollback to 1 runs 2_b.down.sql alone. Given positions of their own, the
// halves of a pair leave a gap in the keys, and the migration runs under the
// key of its down file while a reader of the directory, such as ptah-compat
// migrate set, keys it by its up file.
func TestAtlasPositions_UpAndDownFilesShareAPosition(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "app.db")
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	fsys := atlasSQLFiles(map[string]string{
		"00_z.sql":     "CREATE TABLE z (id int);\n",
		"1_a.up.sql":   "CREATE TABLE a (id int);\n",
		"1_a.down.sql": "DROP TABLE a;\n",
		"2_b.up.sql":   "CREATE TABLE b (id int);\n",
		"2_b.down.sql": "DROP TABLE b;\n",
	})
	mig, err := migrator.NewFSMigrator(conn, fsys, migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
	c.Assert(atlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{"00", "1", "2"})
	keys := make(map[string]int64)
	for _, label := range []string{"00", "1", "2"} {
		key, err := mig.ResolveVersion(label)
		c.Assert(err, qt.IsNil)
		keys[label] = key
	}
	c.Assert(keys, qt.DeepEquals, map[string]int64{"00": 1, "1": 2, "2": 3})
	files, err := migrationfile.Discover(fsys, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	fileKeys := make(map[string]int64, len(files))
	for _, file := range files {
		fileKeys[file.Path] = file.OrderKey
	}
	c.Assert(fileKeys, qt.DeepEquals, map[string]int64{
		"00_z.sql": keys["00"], "1_a.down.sql": keys["1"], "1_a.up.sql": keys["1"],
		"2_b.down.sql": keys["2"], "2_b.up.sql": keys["2"],
	})

	err = mig.MigrateDownTo(c.Context(), keys["1"])

	c.Assert(err, qt.IsNil)
	c.Assert(atlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{"00", "1"})
	c.Assert(sqliteTableNames(c, conn), qt.DeepEquals, []string{"a", "z"})
}

// sqliteTableNames is the user tables of the database conn opens, by name,
// leaving out the revision table.
func sqliteTableNames(c *qt.C, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(),
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'atlas_%' ORDER BY name")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	names := make([]string, 0)
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}
