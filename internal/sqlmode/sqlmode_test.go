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

// A session is strict when it names either strict mode, in any case.
func TestStrict(t *testing.T) {
	tests := []struct {
		name, sqlMode string
		want          bool
	}{
		{name: "MySQL's default", sqlMode: "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION", want: true},
		{name: "every table", sqlMode: "STRICT_ALL_TABLES", want: true},
		{name: "in lower case", sqlMode: "ansi_quotes, strict_all_tables", want: true},
		{name: "loose", sqlMode: "NO_ENGINE_SUBSTITUTION"},
		{name: "loose with the zero date modes", sqlMode: "NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO"},
		{name: "empty", sqlMode: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(sqlmode.Strict(test.sqlMode), qt.Equals, test.want)
		})
	}
}

// WithStrict makes a loose session strict and keeps every mode whose effect
// strictness leaves alone, ANSI_QUOTES and NO_BACKSLASH_ESCAPES among them. A
// strict session is left as it is, its zero date modes included.
func TestWithStrict(t *testing.T) {
	tests := []struct {
		name, sqlMode, want string
	}{
		{name: "MySQL's default", sqlMode: "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION", want: "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"},
		{name: "strict on every table", sqlMode: "STRICT_ALL_TABLES,NO_ZERO_DATE", want: "STRICT_ALL_TABLES,NO_ZERO_DATE"},
		{name: "loose", sqlMode: "NO_ENGINE_SUBSTITUTION", want: "NO_ENGINE_SUBSTITUTION,STRICT_ALL_TABLES"},
		{name: "loose with ANSI_QUOTES and NO_BACKSLASH_ESCAPES", sqlMode: "ANSI_QUOTES,NO_BACKSLASH_ESCAPES", want: "ANSI_QUOTES,NO_BACKSLASH_ESCAPES,STRICT_ALL_TABLES"},
		{name: "loose with the zero date modes", sqlMode: "REAL_AS_FLOAT,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,PIPES_AS_CONCAT", want: "REAL_AS_FLOAT,PIPES_AS_CONCAT,STRICT_ALL_TABLES"},
		{name: "loose in lower case with spaces", sqlMode: "ansi_quotes, no_zero_date ", want: "ansi_quotes,STRICT_ALL_TABLES"},
		{name: "empty", sqlMode: "", want: "STRICT_ALL_TABLES"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(sqlmode.WithStrict(test.sqlMode), qt.Equals, test.want)
		})
	}
}
