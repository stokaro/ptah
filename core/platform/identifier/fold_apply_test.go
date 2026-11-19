package identifier_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
)

// TestFoldApply pins the name each fold stores for a bare name. The upper
// rows are what Oracle Free 23.26 reported in USER_TABLES for each name
// created bare in an AL32UTF8 database; the lower rows are what PostgreSQL 18
// reported in pg_class in a UTF-8 database.
func TestFoldApply(t *testing.T) {
	tests := []struct {
		name  string
		fold  identifier.Fold
		value string
		want  string
	}{
		{name: "upper ASCII", fold: identifier.FoldUpper, value: "orders", want: "ORDERS"},
		{name: "upper accented letters", fold: identifier.FoldUpper, value: "été", want: "ÉTÉ"},
		{name: "upper sharp s keeps one rune", fold: identifier.FoldUpper, value: "straße", want: "STRAßE"},
		{name: "upper dotless i", fold: identifier.FoldUpper, value: "ıdx", want: "IDX"},
		{name: "upper ligature without a one-rune uppercase", fold: identifier.FoldUpper, value: "ﬁx", want: "ﬁX"},
		{name: "lower ASCII", fold: identifier.FoldLower, value: "ORDERS", want: "orders"},
		{name: "lower keeps a non-ASCII letter", fold: identifier.FoldLower, value: "ÉTÉ", want: "ÉtÉ"},
		{name: "lower sharp s", fold: identifier.FoldLower, value: "STRAßE", want: "straße"},
		{name: "no fold", fold: "", value: "Orders", want: "Orders"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.fold.Apply(test.value), qt.Equals, test.want)
		})
	}
}
