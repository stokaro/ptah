package migrationfile_test

import (
	"fmt"
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
// runs it. Left out, the file would never run and the directory would
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
// runs as version "" and Ptah cannot address.
func TestDiscover_FailurePath_AnAtlasFileWithAnEmptyVersion(t *testing.T) {
	c := qt.New(t)
	fsys := unreadAtlasFiles()
	fsys["_x.sql"] = &fstest.MapFile{Data: []byte("CREATE TABLE e (id int);\n")}

	files, err := migrationfile.Discover(fsys, migrationfile.DirFormatAtlas)

	var unread *migrationfile.UnreadFilesError
	c.Assert(err, qt.ErrorMatches, `Atlas migration file Ptah cannot run: _x\.sql \(version ""\)\. .* rather than leave this file `+
		`unapplied; give it a version before the first underscore of its name, or move it out of the directory`)
	c.Assert(err, qt.ErrorAs, &unread)
	c.Assert(unread.Files, qt.DeepEquals, []string{"_x.sql"})
	c.Assert(unread.Remedy, qt.Equals, "give it a version before the first underscore of its name, or move it out of the directory")
	c.Assert(files, qt.IsNil)
}

// TestDiscover_FailurePath_AFileTheReadingLeavesOut refuses a directory whose
// reading leaves out a SQL file at its top level, and names the file. Left
// out, the file would never run, and the command would report the directory
// applied. The Ptah format, and auto mode that finds Ptah files, read neither
// v1_x.sql nor notes.sql; auto mode that finds Atlas files reads neither
// v2_b.sql, a version that is not a number, nor 3_c.SQL, whose extension
// atlas.sum would not cover. A Ptah name is case-sensitive, so
// 0000000002_b.UP.sql is not read either. The refusal reads as one file when
// it names one.
func TestDiscover_FailurePath_AFileTheReadingLeavesOut(t *testing.T) {
	ptahRefusal := `migration file the Ptah format does not read: %s\. Ptah reads a migration directory whole, ` +
		`so it refuses the directory rather than leave this file unapplied; name it NNNNNNNNNN_<name>\.up\.sql, ` +
		`NNNNNNNNNN_<name>\.down\.sql or R__<name>\.sql, with \.up, \.down and \.sql in lower case, or move it out ` +
		`of the directory`
	ptahRefusals := `migration files the Ptah format does not read: %s\. Ptah reads a migration directory whole, ` +
		`so it refuses the directory rather than leave these files unapplied; name each NNNNNNNNNN_<name>\.up\.sql, ` +
		`NNNNNNNNNN_<name>\.down\.sql or R__<name>\.sql, with \.up, \.down and \.sql in lower case, or move them out ` +
		`of the directory`
	atlasRefusal := `migration file format detection does not read: %s\. The directory reads as Atlas migrations, and ` +
		`Atlas runs every SQL file at the top level of the directory, so Ptah refuses the directory rather than leave ` +
		`this file unapplied; read the directory with --dir-format atlas, or move the file out of the directory`
	ptahPair := func(extra ...string) fstest.MapFS {
		fsys := fstest.MapFS{
			"0000000001_a.up.sql":   {Data: []byte("CREATE TABLE a (id int);\n")},
			"0000000001_a.down.sql": {Data: []byte("DROP TABLE a;\n")},
		}
		for _, name := range extra {
			fsys[name] = &fstest.MapFile{Data: []byte("CREATE TABLE x (id int);\n")}
		}
		return fsys
	}
	numbered := func(extra ...string) fstest.MapFS {
		fsys := fstest.MapFS{"1_a.sql": {Data: []byte("CREATE TABLE a (id int);\n")}}
		for _, name := range extra {
			fsys[name] = &fstest.MapFile{Data: []byte("CREATE TABLE x (id int);\n")}
		}
		return fsys
	}
	withNotes := unreadAtlasFiles()
	withNotes["notes.sql"] = &fstest.MapFile{Data: []byte("-- scratch pad\n")}
	tests := []struct {
		name   string
		fsys   fstest.MapFS
		format migrationfile.DirFormat
		want   string
		unread []string
	}{
		{"ptah beside a version that is not a number", unreadAtlasFiles(), migrationfile.DirFormatPtah, fmt.Sprintf(ptahRefusal, `v1_x\.sql`), []string{"v1_x.sql"}},
		{"auto beside a version that is not a number", unreadAtlasFiles(), migrationfile.DirFormatAuto, fmt.Sprintf(ptahRefusal, `v1_x\.sql`), []string{"v1_x.sql"}},
		{"auto beside notes", withNotes, migrationfile.DirFormatAuto, fmt.Sprintf(ptahRefusals, `notes\.sql, v1_x\.sql`), []string{"notes.sql", "v1_x.sql"}},
		{"auto beside a Ptah name with no direction", ptahPair("0000000002_legacy.sql"), migrationfile.DirFormatAuto,
			fmt.Sprintf(ptahRefusal, `0000000002_legacy\.sql`), []string{"0000000002_legacy.sql"}},
		{"ptah beside an uppercase direction", ptahPair("0000000002_b.UP.sql"), migrationfile.DirFormatPtah,
			fmt.Sprintf(ptahRefusal, `0000000002_b\.UP\.sql`), []string{"0000000002_b.UP.sql"}},
		{"ptah beside an uppercase extension", ptahPair("0000000002_b.up.SQL"), migrationfile.DirFormatPtah,
			fmt.Sprintf(ptahRefusal, `0000000002_b\.up\.SQL`), []string{"0000000002_b.up.SQL"}},
		{"ptah beside a version with no name", ptahPair("0000000002.up.sql"), migrationfile.DirFormatPtah,
			fmt.Sprintf(ptahRefusal, `0000000002\.up\.sql`), []string{"0000000002.up.sql"}},
		{"auto beside a numbered Atlas file", numbered("v2_b.sql"), migrationfile.DirFormatAuto, fmt.Sprintf(atlasRefusal, `v2_b\.sql`), []string{"v2_b.sql"}},
		{"auto beside an uppercase extension", numbered("3_c.SQL"), migrationfile.DirFormatAuto, fmt.Sprintf(atlasRefusal, `3_c\.SQL`), []string{"3_c.SQL"}},
		{"auto beside a legacy Ptah name", numbered("0000000001_ptah_width_legacy.sql"), migrationfile.DirFormatAuto,
			fmt.Sprintf(atlasRefusal, `0000000001_ptah_width_legacy\.sql`), []string{"0000000001_ptah_width_legacy.sql"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			files, err := migrationfile.Discover(test.fsys, test.format)

			var unread *migrationfile.UnreadFilesError
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorAs, &unread)
			c.Assert(unread.Files, qt.DeepEquals, test.unread)
			c.Assert(files, qt.IsNil)
		})
	}
}
