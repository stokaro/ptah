package migrator_test

import (
	"errors"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// atlasLinearityFiles is a directory of one CREATE TABLE file per name.
func atlasLinearityFiles(names ...string) fstest.MapFS {
	fsys := fstest.MapFS{}
	for index, name := range names {
		fsys[name] = &fstest.MapFile{Data: []byte("CREATE TABLE t" + string(rune('a'+index)) + " (id int);\n")}
	}
	return fsys
}

// runAtlasUpStringLinearity is runAtlasUp under
// [migrator.Migrator.WithAtlasStringLinearity], which ptah-compat selects.
func runAtlasUpStringLinearity(c *qt.C, dbPath string, fsys fstest.MapFS) error {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	mig, err := migrator.NewFSMigrator(conn, fsys, migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	return mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithAtlasStringLinearity().
		MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{})
}

// TestAtlasLinearity_NumericByDefault keeps the numeric rule a caller gets
// without [migrator.Migrator.WithAtlasStringLinearity], which the native
// commands run: 10_b.sql added after 1_a.sql and 2_b.sql applies, because 10
// is above 2.
func TestAtlasLinearity_NumericByDefault(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "app.db")
	c.Assert(runAtlasUp(c, dbPath, atlasLinearityFiles("1_a.sql", "2_b.sql")), qt.IsNil)

	err := runAtlasUp(c, dbPath, atlasLinearityFiles("1_a.sql", "2_b.sql", "10_b.sql"))

	c.Assert(err, qt.IsNil)
	c.Assert(atlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{"1", "10", "2"})
}

// TestAtlasLinearity_HappyPath applies, under the string rule, a file whose
// number is below the applied mark and whose version string sorts above it:
// the pinned community binary v1.3.0 applies 3__x.sql after
// 20260101000000_ts.sql.
func TestAtlasLinearity_HappyPath(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "app.db")
	c.Assert(runAtlasUpStringLinearity(c, dbPath, atlasLinearityFiles("1_a.sql", "20260101000000_ts.sql")), qt.IsNil)

	err := runAtlasUpStringLinearity(c, dbPath, atlasLinearityFiles("1_a.sql", "20260101000000_ts.sql", "3__x.sql"))

	c.Assert(err, qt.IsNil)
	c.Assert(atlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{"1", "20260101000000", "3"})
}

// TestAtlasLinearity_FailurePath refuses, under the string rule, a file that
// sorts before an applied file by name, or whose version string sorts below
// the highest applied one.
func TestAtlasLinearity_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		first   []string
		added   string
		applied []string
	}{
		{name: "10_b.sql sorts before an applied 1_a.sql", first: []string{"1_a.sql"}, added: "10_b.sql", applied: []string{"1"}},
		{name: "10 sorts below an applied 9", first: []string{"9_a.sql"}, added: "10_b.sql", applied: []string{"9"}},
		{name: "2 sorts below an applied 20260101000000", first: []string{"1_a.sql", "20260101000000_ts.sql"}, added: "2_b.sql", applied: []string{"1", "20260101000000"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := filepath.Join(c.TempDir(), "app.db")
			c.Assert(runAtlasUpStringLinearity(c, dbPath, atlasLinearityFiles(tt.first...)), qt.IsNil)

			err := runAtlasUpStringLinearity(c, dbPath, atlasLinearityFiles(append(tt.first, tt.added)...))

			_, ok := errors.AsType[*migrator.OutOfOrderError](err)
			c.Assert(ok, qt.IsTrue, qt.Commentf("%v", err))
			c.Assert(atlasRevisionVersions(c, dbPath), qt.DeepEquals, tt.applied)
		})
	}
}
