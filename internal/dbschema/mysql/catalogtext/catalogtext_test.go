package catalogtext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/mysql/catalogtext"
)

// TestSQL feeds the bytes MySQL 26.10 reports in CHECK_CLAUSE, read back with
// HEX(CHECK_CLAUSE), and expects the SQL SHOW CREATE TABLE prints for the
// same constraint.
func TestSQL(t *testing.T) {
	tests := []struct {
		name     string
		reported string
		want     string
	}{
		{
			name:     "ASCII without a string",
			reported: "(`qty` > 0)",
			want:     "(`qty` > 0)",
		},
		{
			name:     "quote",
			reported: "(`c` <> _utf8mb4\\'it\\\\\\'s\\')",
			want:     "(`c` <> _utf8mb4'it\\'s')",
		},
		{
			name:     "backslash",
			reported: "(`c` <> _utf8mb4\\'ab\\\\\\\\cd\\')",
			want:     "(`c` <> _utf8mb4'ab\\\\cd')",
		},
		{
			name:     "backslash before another byte",
			reported: "(`c` <> _utf8mb4\\'a\\tb\\')",
			want:     "(`c` <> _utf8mb4'a\\tb')",
		},
		{
			name:     "two-byte character",
			reported: "(`c` <> _utf8mb4\\'hÃ©llo\\')",
			want:     "(`c` <> _utf8mb4'héllo')",
		},
		{
			name:     "three-byte characters with bytes Windows-1252 maps elsewhere",
			reported: "(`c` <> _utf8mb4\\'â\u0098\u0083â\u0082¬\\')",
			want:     "(`c` <> _utf8mb4'☃€')",
		},
		{
			name:     "non-ASCII character beside a quote",
			reported: "(`c` <> _utf8mb4\\'Ã©\\\\\\'\\')",
			want:     "(`c` <> _utf8mb4'é\\'')",
		},
		{
			name:     "Latin-1 string",
			reported: "(`c` <> _latin1\\'é\\')",
			want:     "(`c` <> _latin1'é')",
		},
		{
			name:     "character above U+00FF",
			reported: "(`c` <> _utf8mb4\\'☃\\')",
			want:     "(`c` <> _utf8mb4'☃')",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(catalogtext.SQL(test.reported), qt.Equals, test.want)
		})
	}
}
