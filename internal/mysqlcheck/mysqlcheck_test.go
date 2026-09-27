package mysqlcheck_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/mysqlcheck"
)

// TestOtherColumn reads the columns a CHECK written on a column names. Every
// row is a column CHECK measured on MySQL 8.4.11: a row with want is refused
// with `ERROR 3813 (HY000): Column check constraint ... references other
// column.`, and a row without it is accepted.
func TestOtherColumn(t *testing.T) {
	tests := []struct {
		name       string
		column     string
		expression string
		columns    []string
		want       string
		wantFound  bool
	}{
		{name: "another column", column: "b", expression: "b > a", columns: []string{"a", "b"}, want: "a", wantFound: true},
		{name: "its own column", column: "b", expression: "b > 0", columns: []string{"a", "b"}},
		{name: "its own column in another case", column: "b", expression: "B > 0", columns: []string{"a", "b"}},
		{
			name: "another column qualified by its table", column: "b", expression: "p3.a > 0", columns: []string{"a", "b"},
			want: "a", wantFound: true,
		},
		{name: "its own column qualified", column: "b", expression: "b > 0 AND r1.p2.b < 9", columns: []string{"a", "b"}},
		{
			name: "another column quoted in another case", column: "b", expression: "b > `a`", columns: []string{"A", "b"},
			want: "A", wantFound: true,
		},
		{name: "a qualifier that is another column's name", column: "b", expression: "s.b > 0", columns: []string{"s", "b"}},
		{name: "a string spelling another column", column: "b", expression: "b <> 'a'", columns: []string{"a", "b"}},
		{name: "a function", column: "b", expression: "abs(b) > 0", columns: []string{"a", "b", "abs"}},
		{
			name: "an INTERVAL unit spelling a column", column: "b",
			expression: "b > DATE_SUB('2020-01-01', INTERVAL 1 YEAR)", columns: []string{"a", "year", "b"},
		},
		{
			name: "an INTERVAL value naming another column", column: "b",
			expression: "b > DATE_SUB('2020-01-01', INTERVAL (a + 1) DAY)", columns: []string{"a", "day", "b"},
			want: "a", wantFound: true,
		},
		{name: "the type of a CAST", column: "b", expression: "CAST(b AS char) <> ''", columns: []string{"char", "b"}},
		{name: "a typed literal", column: "b", expression: "b > DATE '2020-01-01'", columns: []string{"date", "b"}},
		{name: "a collation", column: "b", expression: "b COLLATE utf8mb4_bin <> ''", columns: []string{"utf8mb4_bin", "b"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, found := mysqlcheck.OtherColumn(test.column, test.expression, test.columns)

			c.Assert(found, qt.Equals, test.wantFound)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}
