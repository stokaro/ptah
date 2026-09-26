package mysqlname_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/mysqlname"
)

func TestCheck(t *testing.T) {
	tests := []struct {
		name  string
		table string
		n     uint32
		want  string
	}{
		{name: "the first CHECK", table: "c", n: 1, want: "c_chk_1"},
		{name: "a later CHECK", table: "al", n: 8, want: "al_chk_8"},
		{name: "the table keeps its case", table: "MixedCase", n: 1, want: "MixedCase_chk_1"},
		{name: "the number past the largest wraps to zero", table: "h6", n: 0, want: "h6_chk_0"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqlname.Check(test.table, test.n), qt.Equals, test.want)
		})
	}
}

// TestNextCheckNumber covers the number ALTER TABLE gives an unnamed CHECK.
// Each row is a table measured on MySQL 8.4.11 and 26.7.0: held are the
// CHECKs it held, and want the number of the CHECK an unnamed ADD CHECK added.
func TestNextCheckNumber(t *testing.T) {
	tests := []struct {
		name  string
		table string
		held  []string
		want  uint32
	}{
		{name: "no CHECK", table: "f5", want: 1},
		{name: "one more than the largest, past a gap", table: "al", held: []string{"al_chk_1", "al_chk_5", "al_chk_7"}, want: 8},
		{name: "a leading zero", table: "al3", held: []string{"al3_chk_07"}, want: 8},
		{name: "text after the digits", table: "al6", held: []string{"al6_chk_3x"}, want: 4},
		{name: "a space before the digits", table: "b6", held: []string{"b6_chk_ 3"}, want: 4},
		{name: "a tab before the digits", table: "h4", held: []string{"h4_chk_\t5"}, want: 6},
		{name: "a plus sign", table: "b2", held: []string{"b2_chk_+3"}, want: 4},
		{name: "a minus sign wraps", table: "b1", held: []string{"b1_chk_-3"}, want: 4294967294},
		{name: "a space and a minus sign", table: "h7", held: []string{"h7_chk_ -2", "h7_chk_5"}, want: 4294967295},
		{name: "zero", table: "b0", held: []string{"b0_chk_0"}, want: 1},
		{name: "a number past 32 bits keeps its low bits", table: "h1", held: []string{"h1_chk_4294967296"}, want: 1},
		{name: "a number past 32 bits, not a multiple", table: "h2", held: []string{"h2_chk_4294967300"}, want: 5},
		{name: "the largest 32-bit number", table: "h6", held: []string{"h6_chk_4294967295"}, want: 0},
		{name: "the largest 64-bit number", table: "h3", held: []string{"h3_chk_9223372036854775807"}, want: 0},
		{name: "a number past 64 bits", table: "b7", held: []string{"b7_chk_99999999999999999999"}, want: 0},
		{name: "the table in another case", table: "al4", held: []string{"AL4_chk_3"}, want: 1},
		{name: "the label in another case", table: "al5", held: []string{"al5_CHK_3"}, want: 1},
		{name: "a declared table in mixed case", table: "MixedAlt", held: []string{"mixedalt_chk_3"}, want: 1},
		{name: "a letter before the digits", table: "b3", held: []string{"b3_chk_x3"}, want: 1},
		{name: "a sign and no digits", table: "h5", held: []string{"h5_chk_-"}, want: 1},
		{name: "another table's name", table: "al8", held: []string{"other_chk_9"}, want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqlname.NextCheckNumber(test.table, test.held), qt.Equals, test.want)
		})
	}
}

// TestMariaDBCheck names an unnamed table-level CHECK as MariaDB 11.8.9 did.
// taken holds the names the table's CHECKs held or the statement wrote.
func TestMariaDBCheck(t *testing.T) {
	tests := []struct {
		name  string
		taken []string
		want  string
	}{
		{name: "the first", want: "CONSTRAINT_1"},
		{name: "past a name the statement writes", taken: []string{"CONSTRAINT_1"}, want: "CONSTRAINT_2"},
		{name: "past a name in another case", taken: []string{"constraint_1"}, want: "CONSTRAINT_2"},
		{name: "the first gap", taken: []string{"CONSTRAINT_1", "CONSTRAINT_2", "CONSTRAINT_4"}, want: "CONSTRAINT_3"},
		{name: "a name that only looks numbered", taken: []string{"CONSTRAINT_5"}, want: "CONSTRAINT_1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := mysqlname.MariaDBCheck(func(name string) bool {
				return slices.ContainsFunc(test.taken, func(held string) bool { return strings.EqualFold(held, name) })
			})

			c.Assert(got, qt.Equals, test.want)
		})
	}
}
