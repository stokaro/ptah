package migrationfile_test

import (
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrationfile"
)

func repeatableFS(names ...string) fstest.MapFS {
	fsys := fstest.MapFS{}
	for _, name := range names {
		fsys[name] = &fstest.MapFile{Data: []byte("SELECT 1;\n")}
	}
	return fsys
}

func TestParseRepeatableFileName_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		want     string
	}{
		{name: "letters and underscores", filename: "R__user_names.sql", want: "user_names"},
		{name: "digits and hyphens", filename: "R__010-views.sql", want: "010-views"},
		{name: "upper case kept", filename: "R__Views.sql", want: "Views"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := migrationfile.ParseRepeatableFileName(tc.filename)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, &migrationfile.RepeatableFile{Identity: tc.want})
			c.Assert(migrationfile.RepeatableFileName(tc.want), qt.Equals, tc.filename)
		})
	}
}

func TestParseRepeatableFileName_FailurePath(t *testing.T) {
	t.Run("names that do not claim to be repeatable", func(t *testing.T) {
		for _, filename := range []string{"0000000001_init.up.sql", "R_view.sql", "R__view.txt", "r__view.sql"} {
			t.Run(filename, func(t *testing.T) {
				c := qt.New(t)
				got, err := migrationfile.ParseRepeatableFileName(filename)
				c.Assert(err, qt.ErrorIs, migrationfile.ErrNotRepeatableFileName)
				c.Assert(got, qt.IsNil)
			})
		}
	})

	tests := []struct {
		name     string
		filename string
		wantErr  string
	}{
		{name: "extension in upper case", filename: "R__view.SQL", wantErr: `repeatable migration file "R__view.SQL": spell the extension .sql`},
		{name: "direction suffix", filename: "R__view.up.sql", wantErr: `repeatable migration file "R__view.up.sql": the name after R__ takes .*`},
		{name: "empty identity", filename: "R__.sql", wantErr: `repeatable migration file "R__.sql": the name after R__ takes .*`},
		{name: "space", filename: "R__user names.sql", wantErr: `repeatable migration file "R__user names.sql": the name after R__ takes .*`},
		{name: "too long", filename: "R__" + strings.Repeat("a", 256) + ".sql", wantErr: `repeatable migration file .*: the identity is longer than 255 characters`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := migrationfile.ParseRepeatableFileName(tc.filename)
			c.Assert(err, qt.ErrorMatches, tc.wantErr)
			c.Assert(err, qt.Not(qt.ErrorIs), migrationfile.ErrNotRepeatableFileName)
			c.Assert(got, qt.IsNil)
		})
	}
}

func TestDiscoverRepeatables_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		fsys   fstest.MapFS
		format migrationfile.DirFormat
		want   []migrationfile.RepeatableFile
	}{
		{
			name:   "ptah format, ordered by identity across directories",
			fsys:   repeatableFS("0000000001_init.up.sql", "R__views.sql", "nested/R__a_first.sql"),
			format: migrationfile.DirFormatPtah,
			want: []migrationfile.RepeatableFile{
				{Path: "nested/R__a_first.sql", Identity: "a_first"},
				{Path: "R__views.sql", Identity: "views"},
			},
		},
		{
			name:   "byte order puts upper case and digits first",
			fsys:   repeatableFS("R__b.sql", "R__B2.sql", "R__10.sql", "R__9.sql"),
			format: migrationfile.DirFormatPtah,
			want: []migrationfile.RepeatableFile{
				{Path: "R__10.sql", Identity: "10"},
				{Path: "R__9.sql", Identity: "9"},
				{Path: "R__B2.sql", Identity: "B2"},
				{Path: "R__b.sql", Identity: "b"},
			},
		},
		{
			name:   "auto format beside ptah versioned files",
			fsys:   repeatableFS("0000000001_init.up.sql", "R__views.sql"),
			format: migrationfile.DirFormatAuto,
			want:   []migrationfile.RepeatableFile{{Path: "R__views.sql", Identity: "views"}},
		},
		{
			name:   "auto format with no ptah versioned file",
			fsys:   repeatableFS("R__views.sql"),
			format: migrationfile.DirFormatAuto,
			want:   nil,
		},
		{
			name:   "atlas format reads R__ as a version",
			fsys:   repeatableFS("1_init.sql", "R__views.sql"),
			format: migrationfile.DirFormatAtlas,
			want:   nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := migrationfile.DiscoverRepeatables(tc.fsys, tc.format)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, tc.want)
		})
	}
}

func TestDiscoverRepeatables_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		fsys    fstest.MapFS
		wantErr string
	}{
		{
			name:    "identities that differ only in case",
			fsys:    repeatableFS("R__Views.sql", "nested/R__views.sql"),
			wantErr: `repeatable migrations .* and .* carry identities that differ only in letter case`,
		},
		{
			name:    "misspelled repeatable name",
			fsys:    repeatableFS("0000000001_init.up.sql", "R__views.up.sql"),
			wantErr: `repeatable migration file "R__views.up.sql": .*`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := migrationfile.DiscoverRepeatables(tc.fsys, migrationfile.DirFormatPtah)
			c.Assert(err, qt.ErrorMatches, tc.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

func TestDiscover_LeavesRepeatablesOut(t *testing.T) {
	t.Run("beside versioned files", func(t *testing.T) {
		c := qt.New(t)
		files, err := migrationfile.Discover(repeatableFS("0000000001_init.up.sql", "R__views.sql"), migrationfile.DirFormatPtah)
		c.Assert(err, qt.IsNil)
		c.Assert(files, qt.HasLen, 1)
		c.Assert(files[0].Path, qt.Equals, "0000000001_init.up.sql")
	})

	t.Run("a ptah directory of repeatable migrations alone", func(t *testing.T) {
		c := qt.New(t)
		files, err := migrationfile.Discover(repeatableFS("R__views.sql"), migrationfile.DirFormatPtah)
		c.Assert(err, qt.IsNil)
		c.Assert(files, qt.HasLen, 0)
	})
}
