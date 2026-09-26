package pgname_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pgname"
)

// TestCheck names an unnamed CHECK as PostgreSQL does. Every row is a name
// PostgreSQL 18.6 gave `CREATE TABLE <table> (<columns>, CHECK (<expression>))`,
// read back from pg_constraint; taken holds the names the same statement or
// schema had already used when the row's CHECK was created.
func TestCheck(t *testing.T) {
	longTable := "a_table_name_that_is_quite_long_indeed_for_testing_truncation"
	longColumn := "a_column_name_that_is_also_rather_long_for_truncation"
	literalColumns := []string{"s", "lo", "date", "day", "text", "length", "numeric", "i", "d"}
	tests := []struct {
		name       string
		table      string
		columns    []string
		expression string
		taken      []string
		want       string
	}{
		{
			name:  "one column",
			table: "a", columns: []string{"plan"}, expression: "plan IN ('x','y')",
			want: "a_plan_check",
		},
		{
			name:  "two columns",
			table: "c", columns: []string{"lo", "hi"}, expression: "lo < hi",
			want: "c_check",
		},
		{
			name:  "two columns on a column-level CHECK",
			table: "e", columns: []string{"a", "b"}, expression: "a IS NULL OR b IS NOT NULL",
			want: "e_check",
		},
		{
			name:  "no column",
			table: "f", columns: []string{"a"}, expression: "true",
			want: "f_check",
		},
		{
			name:  "no column, the name taken",
			table: "f", columns: []string{"a"}, expression: "1 > 0", taken: []string{"f_check"},
			want: "f_check1",
		},
		{
			name:  "one column twice",
			table: "g", columns: []string{"a"}, expression: "a > 0 AND a < 10",
			want: "g_a_check",
		},
		{
			name:  "a string literal spelling a column",
			table: "n", columns: []string{"lo", "hi", "s"}, expression: "s <> 'lo'",
			want: "n_s_check",
		},
		{
			name:  "an escape string spelling a column",
			table: "lit", columns: literalColumns, expression: `E'lo\'s' <> s`,
			want: "lit_s_check",
		},
		{
			name:  "a dollar-quoted string spelling a column",
			table: "lit", columns: literalColumns, expression: "$$lo$$ <> s", taken: []string{"lit_s_check"},
			want: "lit_s_check1",
		},
		{
			name:  "a typed literal whose type is a column's name",
			table: "lit", columns: literalColumns, expression: "d > date '2000-01-01'",
			want: "lit_d_check",
		},
		{
			name:  "an interval field that is a column's name",
			table: "lit", columns: literalColumns, expression: "i > interval '1' day",
			want: "lit_i_check",
		},
		{
			name:  "a cast to a type that is a column's name",
			table: "lit", columns: literalColumns, expression: "s::text <> ''", taken: []string{"lit_s_check", "lit_s_check1"},
			want: "lit_s_check2",
		},
		{
			name:  "a function that is a column's name",
			table: "lit", columns: literalColumns, expression: "length(s) > 0",
			taken: []string{"lit_s_check", "lit_s_check1", "lit_s_check2"},
			want:  "lit_s_check3",
		},
		{
			name:  "CAST to a type that is a column's name",
			table: "lit", columns: literalColumns, expression: "CAST(lo AS numeric) > 0",
			want: "lit_lo_check",
		},
		{
			name:  "a cast to a type of two words and a modifier",
			table: "lit", columns: literalColumns, expression: "s::character varying(10) <> ''",
			taken: []string{"lit_s_check", "lit_s_check1", "lit_s_check2", "lit_s_check3"},
			want:  "lit_s_check4",
		},
		{
			name:  "a cast to an array type",
			table: "lit", columns: literalColumns, expression: "s::text[] <> '{}'",
			taken: []string{"lit_s_check", "lit_s_check1", "lit_s_check2", "lit_s_check3", "lit_s_check4"},
			want:  "lit_s_check5",
		},
		{
			name:  "a cast to a qualified type",
			table: "lit", columns: literalColumns, expression: "lo::pg_catalog.int8 > 0", taken: []string{"lit_lo_check"},
			want: "lit_lo_check1",
		},
		{
			name:  "a column qualified by schema and table",
			table: "lit", columns: literalColumns, expression: "m.lit.lo > 0", taken: []string{"lit_lo_check", "lit_lo_check1"},
			want: "lit_lo_check2",
		},
		{
			name:  "a schema qualifier that is a column's name",
			table: "lit2", columns: []string{"m", "lo"}, expression: "m.lit2.lo > 0",
			want: "lit2_lo_check",
		},
		{
			name:  "a table qualifier that is a column's name",
			table: "status", columns: []string{"status", "n"}, expression: "status.n > 0",
			want: "status_n_check",
		},
		{
			name:  "a whole-row reference",
			table: "lit", columns: literalColumns, expression: "lit.* IS NOT NULL",
			want: "lit_check",
		},
		{
			name:  "the table's name alone",
			table: "wr", columns: []string{"a"}, expression: "wr IS NOT NULL",
			want: "wr_check",
		},
		{
			name:  "a whole-row reference beside a column",
			table: "wr2", columns: []string{"a"}, expression: "wr2 IS NOT NULL AND a > 0",
			want: "wr2_check",
		},
		{
			name:  "a column the table's name",
			table: "status", columns: []string{"status"}, expression: "status <> ''",
			want: "status_status_check",
		},
		{
			name:  "EXTRACT's field that is a column's name",
			table: "kw", columns: []string{"year", "zone", "ts", "s"}, expression: "extract(year from ts) > 2000",
			want: "kw_ts_check",
		},
		{
			name:  "AT TIME ZONE beside a column named zone",
			table: "kw", columns: []string{"year", "zone", "ts", "s"}, expression: "ts AT TIME ZONE 'UTC' > '2000-01-01'",
			taken: []string{"kw_ts_check"}, want: "kw_ts_check1",
		},
		{
			name:  "a collation that is a column's name",
			table: "co", columns: []string{"s", "C"}, expression: `s COLLATE "C" > ''`,
			want: "co_s_check",
		},
		{
			name:  "a cast to a type of two words, the second a column's name",
			table: "ty", columns: []string{"x", "precision", "zone", "time", "ts"}, expression: "x::double precision > 0",
			want: "ty_x_check",
		},
		{
			name:  "a cast to a type whose words are columns' names",
			table: "ty", columns: []string{"x", "precision", "zone", "time", "ts"},
			expression: "ts::timestamp with time zone > '2000-01-01'",
			want:       "ty_ts_check",
		},
		{
			name:  "a function of two columns",
			table: "fn", columns: []string{"a", "b"}, expression: "coalesce(a, b) > 0",
			want: "fn_check",
		},
		{
			name:  "IS DISTINCT FROM",
			table: "fn", columns: []string{"a", "b"}, expression: "a IS DISTINCT FROM b", taken: []string{"fn_check"},
			want: "fn_check1",
		},
		{
			name:  "a quoted mixed-case column",
			table: "Mixed", columns: []string{"Col"}, expression: `"Col" > 0`,
			want: "Mixed_Col_check",
		},
		{
			name:  "an unquoted name folds to lower case",
			table: "ws", columns: []string{"a", "A"}, expression: "A > 0",
			want: "ws_a_check",
		},
		{
			name:  "a quoted name keeps its case",
			table: "ws", columns: []string{"a", "A"}, expression: `"A" > 0`,
			want: "ws_A_check",
		},
		{
			name:  "the folded and the quoted name are two columns",
			table: "ws", columns: []string{"a", "A"}, expression: `a > 0 AND "A" > 0`,
			want: "ws_check",
		},
		{
			name:  "a long table and column cut to 63 bytes",
			table: longTable, columns: []string{longColumn}, expression: longColumn + " > 0",
			want: "a_table_name_that_is_quite_l_a_column_name_that_is_also_r_check",
		},
		{
			name:  "a long table and column, numbered",
			table: longTable, columns: []string{longColumn}, expression: longColumn + " < 9",
			taken: []string{"a_table_name_that_is_quite_l_a_column_name_that_is_also_r_check"},
			want:  "a_table_name_that_is_quite_l_a_column_name_that_is_also__check1",
		},
		{
			name:  "a long table and no column",
			table: longTable, columns: []string{longColumn}, expression: "true",
			want: "a_table_name_that_is_quite_long_indeed_for_testing_trunca_check",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := pgname.Check(test.table, test.expression, test.columns, func(name string) bool {
				return slices.Contains(test.taken, name)
			})

			c.Assert(got, qt.Equals, test.want)
		})
	}
}
