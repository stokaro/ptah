package migrator_test

import (
	"errors"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

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

// TestAtlasLinearity_HappyPath applies a file whose number is below the
// applied mark and whose version string sorts above it: the pinned community
// binary v1.3.0 applies 3__x.sql after 20260101000000_ts.sql.
func TestAtlasLinearity_HappyPath(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "app.db")
	c.Assert(runAtlasUp(c, dbPath, atlasLinearityFiles("1_a.sql", "20260101000000_ts.sql")), qt.IsNil)

	err := runAtlasUp(c, dbPath, atlasLinearityFiles("1_a.sql", "20260101000000_ts.sql", "3__x.sql"))

	c.Assert(err, qt.IsNil)
	c.Assert(atlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{"1", "20260101000000", "3"})
}

// TestAtlasLinearity_FailurePath refuses a file that sorts before an applied
// file by name, or whose version string sorts below the highest applied one.
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
			c.Assert(runAtlasUp(c, dbPath, atlasLinearityFiles(tt.first...)), qt.IsNil)

			err := runAtlasUp(c, dbPath, atlasLinearityFiles(append(tt.first, tt.added)...))

			_, ok := errors.AsType[*migrator.OutOfOrderError](err)
			c.Assert(ok, qt.IsTrue, qt.Commentf("%v", err))
			c.Assert(atlasRevisionVersions(c, dbPath), qt.DeepEquals, tt.applied)
		})
	}
}
