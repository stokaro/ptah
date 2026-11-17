package repeatableset_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/repeatableset"
	"ptah.run/migration/migrationfile"
)

func ptahDirectory(extra map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{
		"0000000001_users.up.sql":   {Data: []byte("CREATE TABLE users (id int);\n")},
		"0000000001_users.down.sql": {Data: []byte("DROP TABLE users;\n")},
	}
	for name, body := range extra {
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	return fsys
}

func TestPaths_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		fsys   fstest.MapFS
		format migrationfile.DirFormat
		want   map[string]struct{}
	}{
		{
			name:   "the repeatable migrations of a Ptah directory, nested ones included",
			fsys:   ptahDirectory(map[string]string{"R__a.sql": "SELECT 1;\n", "views/R__b.sql": "SELECT 2;\n"}),
			format: migrationfile.DirFormatAuto,
			want:   map[string]struct{}{"R__a.sql": {}, "views/R__b.sql": {}},
		},
		{
			name:   "none in a directory read in the Atlas format",
			fsys:   fstest.MapFS{"1_users.sql": {Data: []byte("SELECT 1;\n")}, "R__a.sql": {Data: []byte("SELECT 1;\n")}},
			format: migrationfile.DirFormatAtlas,
			want:   make(map[string]struct{}),
		},
		{
			name:   "none, without the discovery's refusal, where no file claims the prefix",
			fsys:   fstest.MapFS{"notes.sql": {Data: []byte("SELECT 1;\n")}},
			format: migrationfile.DirFormatPtah,
			want:   nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := repeatableset.Paths(tc.fsys, tc.format)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, tc.want)
		})
	}
}

func TestPaths_FailurePath(t *testing.T) {
	t.Run("a misspelled repeatable name is refused", func(t *testing.T) {
		c := qt.New(t)
		got, err := repeatableset.Paths(ptahDirectory(map[string]string{"R__view.up.sql": "SELECT 1;\n"}), migrationfile.DirFormatAuto)
		c.Assert(err, qt.ErrorMatches, `repeatable migration file "R__view.up.sql": .*`)
		c.Assert(got, qt.IsNil)
	})
}
