package migrationfile_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrationfile"
)

// TestParseFileTxMode_HeadingHappyPath holds the header that runs straight
// into its statement to lines that lead with the key, and the header a blank
// line ends to the key anywhere in a line, as the community binary reads it.
func TestParseFileTxMode_HeadingHappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want migrationfile.FileTxMode
	}{
		{
			name: "a directive line above the statement",
			sql:  "-- atlas:txmode none\nCREATE TABLE t (id int);\n",
			want: migrationfile.FileTxModeNone,
		},
		{
			name: "a directive line under a heading, above the statement",
			sql:  "-- Create \"t\" table\n--  atlas:txmode none\nCREATE TABLE t (id int);\n",
			want: migrationfile.FileTxModeNone,
		},
		{
			name: "the key inside a line of a header a blank line ends",
			sql:  "-- x atlas:txmode none\n\nCREATE TABLE t (id int);\n",
			want: migrationfile.FileTxModeNone,
		},
		{
			name: "a heading that names the key above the statement",
			sql:  "-- Create \"atlas:txmode none\" table\nCREATE TABLE \"atlas:txmode none\" (id int);\n",
			want: migrationfile.FileTxModeUnspecified,
		},
		{
			name: "a heading that names an unknown mode above the statement",
			sql:  "-- Create \"atlas:txmode bogus\" table\nCREATE TABLE \"atlas:txmode bogus\" (id int);\n",
			want: migrationfile.FileTxModeUnspecified,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := migrationfile.ParseFileTxMode("x.sql", test.sql)

			c.Assert(got.Err, qt.IsNil)
			c.Assert(got.Mode, qt.Equals, test.want)
		})
	}
}

// TestParseFileTxMode_HeadingFailurePath is the control: the same heading in a
// header a blank line ends is read as a directive, and the community binary
// refuses it there too.
func TestParseFileTxMode_HeadingFailurePath(t *testing.T) {
	c := qt.New(t)

	got := migrationfile.ParseFileTxMode("x.sql", "-- Create \"atlas:txmode bogus\" table\n\nCREATE TABLE t (id int);\n")

	c.Assert(got.Err, qt.ErrorMatches, `unknown txmode "bogus\\" table" found in file directive "x.sql"`)
	c.Assert(got.Mode, qt.Equals, migrationfile.FileTxModeUnspecified)
}
