package sqlmode_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlmode"
)

// The modes are MySQL 26.10's and MariaDB 12.3's defaults, each with and
// without NO_BACKSLASH_ESCAPES.
func TestHas(t *testing.T) {
	tests := []struct {
		name, sqlMode string
		want          bool
	}{
		{name: "MySQL's default", sqlMode: "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"},
		{name: "MariaDB's default", sqlMode: "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"},
		{name: "in the middle", sqlMode: "ONLY_FULL_GROUP_BY,NO_BACKSLASH_ESCAPES,STRICT_TRANS_TABLES", want: true},
		{name: "first", sqlMode: "NO_BACKSLASH_ESCAPES,STRICT_TRANS_TABLES", want: true},
		{name: "alone", sqlMode: "NO_BACKSLASH_ESCAPES", want: true},
		{name: "in lower case with spaces", sqlMode: "strict_trans_tables, no_backslash_escapes ", want: true},
		{name: "a longer name", sqlMode: "NO_BACKSLASH_ESCAPES_X"},
		{name: "a shorter name", sqlMode: "BACKSLASH_ESCAPES"},
		{name: "empty", sqlMode: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(sqlmode.Has(test.sqlMode, sqlmode.NoBackslashEscapes), qt.Equals, test.want)
		})
	}
}
