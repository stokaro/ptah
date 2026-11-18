package migrationfile_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrationfile"
)

// unreadAtlasFiles holds a migration each reading accepts and a file whose
// Atlas version, v1, is not a decimal number.
func unreadAtlasFiles() fstest.MapFS {
	return fstest.MapFS{
		"0000000001_a.up.sql":   {Data: []byte("CREATE TABLE a (id int);\n")},
		"0000000001_a.down.sql": {Data: []byte("DROP TABLE a;\n")},
		"v1_x.sql":              {Data: []byte("CREATE TABLE v (id int);\n")},
	}
}

// TestDiscover_AtlasFormatReadsAVersionThatIsNotANumber reads v1_x.sql as
// version v1 in an Atlas-format directory, as the pinned community binary
// v1.3.0 runs it. Left out, the file would never run and the directory would
// read as applied.
func TestDiscover_AtlasFormatReadsAVersionThatIsNotANumber(t *testing.T) {
	c := qt.New(t)

	files, err := migrationfile.Discover(unreadAtlasFiles(), migrationfile.DirFormatAtlas)

	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 3)
	c.Assert(files[2].Path, qt.Equals, "v1_x.sql")
	c.Assert(files[2].RevisionVersion(), qt.Equals, "v1")
	c.Assert(files[2].Version, qt.Equals, int64(0))
}

// TestDiscover_FailurePath_AnAtlasFileWithAnEmptyVersion refuses an
// Atlas-format directory holding _x.sql, which the pinned community binary
// v1.3.0 runs as version "" and Ptah cannot address.
func TestDiscover_FailurePath_AnAtlasFileWithAnEmptyVersion(t *testing.T) {
	c := qt.New(t)
	fsys := unreadAtlasFiles()
	fsys["_x.sql"] = &fstest.MapFile{Data: []byte("CREATE TABLE e (id int);\n")}

	files, err := migrationfile.Discover(fsys, migrationfile.DirFormatAtlas)

	c.Assert(err, qt.ErrorMatches, `Atlas migration files Ptah cannot run: _x\.sql \(version ""\)\. .*`)
	c.Assert(files, qt.IsNil)
}

// TestDiscover_PtahAndAutoFormatsLeaveTheFileOut keeps the Ptah and auto
// readings of the same directory as they are: each selects the Ptah pair and
// leaves v1_x.sql out, since neither format reads it as a migration.
func TestDiscover_PtahAndAutoFormatsLeaveTheFileOut(t *testing.T) {
	for _, format := range []migrationfile.DirFormat{migrationfile.DirFormatPtah, migrationfile.DirFormatAuto} {
		t.Run(string(format), func(t *testing.T) {
			c := qt.New(t)

			files, err := migrationfile.Discover(unreadAtlasFiles(), format)

			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, 2)
			c.Assert(files[0].Path, qt.Equals, "0000000001_a.down.sql")
			c.Assert(files[1].Path, qt.Equals, "0000000001_a.up.sql")
		})
	}
}
