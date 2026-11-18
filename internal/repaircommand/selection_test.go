package repaircommand_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/repaircommand"
	"ptah.run/migration/migrationfile"
)

// sqlFiles is a directory of the files named, each holding a statement.
func sqlFiles(names ...string) fstest.MapFS {
	fsys := fstest.MapFS{}
	for _, name := range names {
		fsys[name] = &fstest.MapFile{Data: []byte("SELECT 1;\n")}
	}
	return fsys
}

// TestSelectionFor names the flags a recovery command needs to read the same
// directory and revision table: --dir-format atlas only where the default
// directory format reads other files, here the Ptah pair beside v1_x.sql or
// the R__ file the default reads as no repeatable, and --revision-format atlas
// for an Atlas revision table.
func TestSelectionFor(t *testing.T) {
	ptahPair := sqlFiles("0000000001_a.up.sql", "0000000001_a.down.sql", "v1_x.sql")
	atlasOnly := sqlFiles("1_a.sql")
	tests := []struct {
		name    string
		reading repaircommand.Reading
		want    repaircommand.Selection
	}{
		{name: "defaults", reading: repaircommand.Reading{Dir: atlasOnly, DirFormat: migrationfile.DirFormatAuto}},
		{name: "no directory", reading: repaircommand.Reading{DirFormat: migrationfile.DirFormatAtlas}},
		{
			name:    "an empty atlas directory",
			reading: repaircommand.Reading{Dir: fstest.MapFS{}, DirFormat: migrationfile.DirFormatAtlas},
		},
		{
			name:    "atlas files the default reads",
			reading: repaircommand.Reading{Dir: atlasOnly, DirFormat: migrationfile.DirFormatAtlas, AtlasTable: true},
			want:    repaircommand.AtlasTable,
		},
		{
			name:    "atlas files only the flag reads",
			reading: repaircommand.Reading{Dir: ptahPair, DirFormat: migrationfile.DirFormatAtlas, AtlasTable: true},
			want:    repaircommand.AtlasDirectory + repaircommand.AtlasTable,
		},
		{
			name: "atlas repeatables",
			reading: repaircommand.Reading{
				Dir: sqlFiles("1_a.sql", "R__view.sql"), DirFormat: migrationfile.DirFormatAtlas, AtlasRepeatables: true,
			},
			want: repaircommand.AtlasDirectory,
		},
		{
			name:    "an atlas table over ptah files",
			reading: repaircommand.Reading{Dir: ptahPair, DirFormat: migrationfile.DirFormatPtah, AtlasTable: true},
			want:    repaircommand.AtlasTable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got := repaircommand.SelectionFor(tt.reading)

			c.Assert(got, qt.Equals, tt.want)
		})
	}
}
