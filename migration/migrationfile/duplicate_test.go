package migrationfile_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrationfile"
)

// TestDiscover_TwoFilesForOneDirectionOfAPtahVersion_FailurePath pins the
// refusal of a Ptah-format directory where more than one file carries one
// direction of one version. Without it the migrator keeps whichever file
// loads last and never runs the others. The refusal names every such file.
func TestDiscover_TwoFilesForOneDirectionOfAPtahVersion_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		fsys      fstest.MapFS
		direction string
		files     []string
		wantErr   string
	}{
		{
			name: "two up files",
			fsys: fstest.MapFS{
				"0000000002_a.up.sql":   {Data: []byte("CREATE TABLE a (id INTEGER);\n")},
				"0000000002_a.down.sql": {Data: []byte("DROP TABLE a;\n")},
				"0000000002_b.up.sql":   {Data: []byte("CREATE TABLE b (id INTEGER);\n")},
			},
			direction: "up",
			files:     []string{"0000000002_a.up.sql", "0000000002_b.up.sql"},
			wantErr:   `duplicate up migration for version 2: 0000000002_a\.up\.sql and 0000000002_b\.up\.sql`,
		},
		{
			name: "two down files",
			fsys: fstest.MapFS{
				"0000000002_a.up.sql":   {Data: []byte("CREATE TABLE a (id INTEGER);\n")},
				"0000000002_a.down.sql": {Data: []byte("DROP TABLE a;\n")},
				"0000000002_b.down.sql": {Data: []byte("DROP TABLE a;\n")},
			},
			direction: "down",
			files:     []string{"0000000002_a.down.sql", "0000000002_b.down.sql"},
			wantErr:   `duplicate down migration for version 2: 0000000002_a\.down\.sql and 0000000002_b\.down\.sql`,
		},
		{
			name: "a checkpoint beside an ordinary file",
			fsys: fstest.MapFS{
				"0000000002_a.up.sql":            {Data: []byte("CREATE TABLE a (id INTEGER);\n")},
				"0000000002_a.down.sql":          {Data: []byte("DROP TABLE a;\n")},
				"0000000002_a.checkpoint.up.sql": {Data: []byte("CREATE TABLE a (id INTEGER);\n")},
			},
			direction: "up",
			files:     []string{"0000000002_a.checkpoint.up.sql", "0000000002_a.up.sql"},
			wantErr:   `duplicate up migration for version 2: 0000000002_a\.checkpoint\.up\.sql and 0000000002_a\.up\.sql`,
		},
		{
			name: "three up files",
			fsys: fstest.MapFS{
				"0000000002_a.up.sql":   {Data: []byte("CREATE TABLE a (id INTEGER);\n")},
				"0000000002_a.down.sql": {Data: []byte("DROP TABLE a;\n")},
				"0000000002_b.up.sql":   {Data: []byte("CREATE TABLE b (id INTEGER);\n")},
				"0000000002_c.up.sql":   {Data: []byte("CREATE TABLE c (id INTEGER);\n")},
			},
			direction: "up",
			files:     []string{"0000000002_a.up.sql", "0000000002_b.up.sql", "0000000002_c.up.sql"},
			wantErr: `duplicate up migration for version 2: 0000000002_a\.up\.sql, 0000000002_b\.up\.sql and ` +
				`0000000002_c\.up\.sql`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			files, err := migrationfile.Discover(test.fsys, migrationfile.DirFormatPtah)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			var duplicate *migrationfile.DuplicateFileError
			c.Assert(err, qt.ErrorAs, &duplicate)
			c.Assert(duplicate.Direction, qt.Equals, test.direction)
			c.Assert(duplicate.Files, qt.DeepEquals, test.files)
			c.Assert(files, qt.IsNil)
		})
	}
}

// TestDiscover_TwoFilesForOneDirectionOfAPtahVersion_HappyPath pins where the
// refusal stops: an up and a down file of one version, and two versions, are
// one migration each, and in the Atlas format the files of one version are
// left to the loader, which knows a revision's identity.
func TestDiscover_TwoFilesForOneDirectionOfAPtahVersion_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		fsys   fstest.MapFS
		format migrationfile.DirFormat
		want   []string
	}{
		{
			name: "an up and a down file of each version",
			fsys: fstest.MapFS{
				"0000000001_a.up.sql":   {Data: []byte("CREATE TABLE a (id INTEGER);\n")},
				"0000000001_a.down.sql": {Data: []byte("DROP TABLE a;\n")},
				"0000000002_b.up.sql":   {Data: []byte("CREATE TABLE b (id INTEGER);\n")},
				"0000000002_b.down.sql": {Data: []byte("DROP TABLE b;\n")},
			},
			format: migrationfile.DirFormatPtah,
			want:   []string{"0000000001_a.down.sql", "0000000001_a.up.sql", "0000000002_b.down.sql", "0000000002_b.up.sql"},
		},
		{
			name: "two Atlas files of one version",
			fsys: fstest.MapFS{
				"1_a.sql": {Data: []byte("CREATE TABLE a (id INTEGER);\n")},
				"1_b.sql": {Data: []byte("CREATE TABLE b (id INTEGER);\n")},
			},
			format: migrationfile.DirFormatAtlas,
			want:   []string{"1_a.sql", "1_b.sql"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			files, err := migrationfile.Discover(test.fsys, test.format)

			c.Assert(err, qt.IsNil)
			paths := make([]string, 0, len(files))
			for _, file := range files {
				paths = append(paths, file.Path)
			}
			c.Assert(paths, qt.DeepEquals, test.want)
		})
	}
}
