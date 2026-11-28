package sqlservername_test

import (
	"strings"
	"testing"
	"unicode/utf16"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlservername"
)

func TestPrimaryKey(t *testing.T) {
	tests := []struct {
		name  string
		table string
		want  string
	}{
		{name: "a short name", table: "orders", want: "orders_pkey"},
		{name: "the longest name the suffix fits", table: strings.Repeat("t", 123), want: strings.Repeat("t", 123) + "_pkey"},
		{name: "a name that would not fit", table: strings.Repeat("t", 125), want: strings.Repeat("t", 114) + "_7c217e06_pkey"},
		{
			// A rune outside the Basic Multilingual Plane is two UTF-16 code
			// units, so 62 of them leave no room for the suffix.
			name:  "a name counted in UTF-16 code units",
			table: strings.Repeat("\U0001F600", 62),
			want:  strings.Repeat("\U0001F600", 57) + "_7270b9b9_pkey",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := sqlservername.PrimaryKey(test.table)

			c.Assert(got, qt.Equals, test.want)
			c.Assert(len(utf16.Encode([]rune(got))) <= 128, qt.IsTrue)
		})
	}
}

// TestPrimaryKey_LongNamesThatStartAlike pins the hash: two long names that
// share the part a cut keeps take two names.
func TestPrimaryKey_LongNamesThatStartAlike(t *testing.T) {
	c := qt.New(t)
	stem := strings.Repeat("t", 130)

	c.Assert(sqlservername.PrimaryKey(stem+"a"), qt.Not(qt.Equals), sqlservername.PrimaryKey(stem+"b"))
}
