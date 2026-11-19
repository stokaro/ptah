package migrationfile_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrationfile"
)

// TestParseAtlasFileName_KeepsTheWholeVersionToken reads the version of a name
// whose version is not a number from the whole name, direction suffix and
// all, as the pinned community binary v1.3.0 records it: v1.up.sql is v1.up,
// and v2.down.sql runs as a migration of its own. A numbered name keeps its
// direction, as an importer writes 1_a.up.sql and 1_a.down.sql.
func TestParseAtlasFileName_KeepsTheWholeVersionToken(t *testing.T) {
	tests := []struct {
		name        string
		version     string
		direction   string
		description string
	}{
		{name: "v1.up.sql", version: "v1.up", direction: "up", description: ""},
		{name: "v2.down.sql", version: "v2.down", direction: "up", description: ""},
		{name: "v1_x.down.sql", version: "v1", direction: "up", description: "x.down"},
		{name: "1_a.down.sql", version: "1", direction: "down", description: "a"},
		{name: "1_a.up.sql", version: "1", direction: "up", description: "a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := migrationfile.ParseAtlasFileName(tt.name)
			c.Assert(err, qt.IsNil)
			c.Assert(got.RevisionVersion(), qt.Equals, tt.version)
			c.Assert(got.Direction, qt.Equals, tt.direction)
			c.Assert(got.VerbatimDescription, qt.Equals, tt.description)
		})
	}
}

// TestDiscover_FailurePath_AnAtlasVersionStartingWithADot refuses
// .draft_x.sql. The pinned community binary v1.3.0 runs it and records
// .draft, measured on SQLite; Ptah reads every revision row whose version
// starts with a dot as the table's own, so it would run the file and then
// report it pending.
func TestDiscover_FailurePath_AnAtlasVersionStartingWithADot(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_a.sql":      {Data: []byte("CREATE TABLE a (id int);\n")},
		".draft_x.sql": {Data: []byte("CREATE TABLE d (id int);\n")},
	}

	files, err := migrationfile.Discover(fsys, migrationfile.DirFormatAtlas)

	c.Assert(err, qt.ErrorMatches, `Atlas records the version of \.draft_x\.sql as "\.draft", which starts with a `+
		`dot as the revision table's own rows do, so Ptah could not tell its row from them; rename the file`)
	c.Assert(files, qt.IsNil)
}

// TestDiscover_OrderKeys gives each file the key it runs under: its number
// where every name spells one, R__view.sql above them, and its position in
// file-name order where a version is not a number or a repeatable spells a
// numbered file's number. Files of one version share it.
func TestDiscover_OrderKeys(t *testing.T) {
	tests := []struct {
		name  string
		files []string
		want  map[string]int64
	}{
		{
			name:  "numbered",
			files: []string{"9_a.sql", "10_b.sql", "R__view.sql"},
			want:  map[string]int64{"10_b.sql": 10, "9_a.sql": 9, "R__view.sql": 11},
		},
		{
			name:  "a version string",
			files: []string{"9_a.sql", "10_b.sql", "00_z.sql", "v1_x.sql"},
			want:  map[string]int64{"00_z.sql": 1, "10_b.sql": 2, "9_a.sql": 3, "v1_x.sql": 4},
		},
		{
			name:  "a repeatable spelling a number",
			files: []string{"1_a.sql", "1R__rep.sql"},
			want:  map[string]int64{"1R__rep.sql": 1, "1_a.sql": 2},
		},
		{
			name:  "one version in two files",
			files: []string{"1_a.up.sql", "1_a.down.sql", "v1_x.sql"},
			want:  map[string]int64{"1_a.down.sql": 1, "1_a.up.sql": 1, "v1_x.sql": 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fsys := fstest.MapFS{}
			for _, name := range tt.files {
				fsys[name] = &fstest.MapFile{Data: []byte("SELECT 1;\n")}
			}

			files, err := migrationfile.Discover(fsys, migrationfile.DirFormatAtlas)

			c.Assert(err, qt.IsNil)
			got := make(map[string]int64, len(files))
			for _, file := range files {
				got[file.Path] = file.OrderKey
			}
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

// TestDiscover_FailurePath_AnAtlasGovernedFileTheSelectionLeavesOut refuses a
// directory Atlas governs whose selection leaves a top-level file out. Beside
// atlas.sum, auto mode reads the Ptah pair as Ptah files and v1_x.sql as no
// migration, and the pinned community binary v1.3.0 runs all three; read with
// Atlas repeatables, R__view.sql is a repeatable and _x.sql is version "",
// which no command can name.
func TestDiscover_FailurePath_AnAtlasGovernedFileTheSelectionLeavesOut(t *testing.T) {
	c := qt.New(t)
	pair := fstest.MapFS{
		"0000000001_a.up.sql":   {Data: []byte("CREATE TABLE a (id int);\n")},
		"0000000001_a.down.sql": {Data: []byte("DROP TABLE a;\n")},
		"v1_x.sql":              {Data: []byte("CREATE TABLE v (id int);\n")},
		"atlas.sum": {Data: []byte("h1:x=\n0000000001_a.down.sql h1:x=\n0000000001_a.up.sql h1:x=\n" +
			"v1_x.sql h1:x=\n")},
	}
	repeatable := fstest.MapFS{
		"R__view.sql": {Data: []byte("CREATE VIEW v AS SELECT 1;\n")},
		"_x.sql":      {Data: []byte("CREATE TABLE x (id int);\n")},
	}

	files, err := migrationfile.Discover(pair, migrationfile.DirFormatAuto)
	_, repeatables, repeatableErr := migrationfile.DiscoverAtlasRepeatables(repeatable)

	c.Assert(err, qt.ErrorMatches, `migration file the selected format does not read: v1_x\.sql\. atlas\.sum covers `+
		`it .*; read the directory with --dir-format atlas, or remove the file from the directory`)
	c.Assert(files, qt.IsNil)
	c.Assert(repeatableErr, qt.ErrorMatches, `Atlas migration file Ptah cannot run: _x\.sql \(version ""\)\..*`)
	c.Assert(repeatables, qt.IsNil)
}
