package pgname_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pgname"
)

// TestExclude names an unnamed EXCLUDE constraint as PostgreSQL does. Every row
// is a name PostgreSQL 18.6 gave `CREATE TABLE <table> (..., EXCLUDE USING
// <method> (<elements>))`, read back from pg_constraint; taken holds the names
// the schema already held.
func TestExclude(t *testing.T) {
	longTable := "a_table_name_that_is_quite_long_indeed_for_testing_truncation"
	longColumn := "a_column_name_that_is_also_rather_long_for_truncation"
	tests := []struct {
		name     string
		table    string
		elements string
		taken    []string
		want     string
	}{
		{name: "one column", table: "a", elements: "r WITH =", want: "a_r_excl"},
		{name: "two columns", table: "b", elements: "r WITH =, s WITH <>", want: "b_r_s_excl"},
		{name: "three columns", table: "mm", elements: "r WITH =, s WITH =, t WITH =", want: "mm_r_s_t_excl"},
		{name: "a second constraint over the column", table: "c", elements: "r WITH =", taken: []string{"c_r_excl"}, want: "c_r_excl1"},
		{name: "a function", table: "d", elements: "lower(t) WITH =", want: "d_lower_excl"},
		{name: "a function in parentheses", table: "e", elements: "(lower(t)) WITH =", want: "e_lower_excl"},
		{name: "an operator expression", table: "f", elements: "(r + 1) WITH =", want: "f_expr_excl"},
		{name: "a function of two columns", table: "g", elements: "int4range(lo, hi) WITH &&", want: "g_int4range_excl"},
		{name: "one column twice", table: "h", elements: "r WITH =, r WITH <>", want: "h_r_r1_excl"},
		{name: "two expressions", table: "o", elements: "(r * 2) WITH =, (r * 3) WITH =", want: "o_expr_expr1_excl"},
		{name: "one function twice", table: "n10", elements: "abs(r) WITH =, abs(r) WITH =", want: "n10_abs_abs1_excl"},
		{name: "a quoted mixed-case column", table: "i", elements: `"R" WITH =`, want: "i_R_excl"},
		{name: "an opclass", table: "n", elements: "r int4_ops WITH =", want: "n_r_excl"},
		{name: "a descending column", table: "n", elements: "r DESC WITH =", taken: []string{"n_r_excl"}, want: "n_r_excl1"},
		{name: "a collation", table: "p", elements: `t COLLATE "C" WITH =`, want: "p_t_excl"},
		{name: "a collation in parentheses", table: "n8", elements: `(t COLLATE "C") WITH =`, want: "n8_t_excl"},
		{name: "an index of the name", table: "kk", elements: "r WITH =", taken: []string{"kk_r_excl"}, want: "kk_r_excl1"},
		{name: "a column qualified by its table", table: "m6", elements: "(m6.r) WITH =", want: "m6_r_excl"},
		{name: "a cast of a column", table: "m1", elements: "(r::text) WITH =", want: "m1_r_excl"},
		{name: "a cast of a constant", table: "m2", elements: "(1::text) WITH =", want: "m2_text_excl"},
		{name: "a cast to an SQL-standard type", table: "n1", elements: "(1::integer) WITH =", want: "n1_int4_excl"},
		{name: "a cast to a type of two words", table: "n2", elements: "(1::character varying) WITH =", want: "n2_varchar_excl"},
		{name: "a cast to double precision", table: "n3", elements: "(1::double precision) WITH =", want: "n3_float8_excl"},
		{name: "a cast to a type with a modifier", table: "p2", elements: "('2000-01-01'::timestamp(3) with time zone) WITH =", want: "p2_timestamptz_excl"},
		{name: "a cast of a cast", table: "n9", elements: "(1::boolean::int) WITH =", taken: []string{"n9_int8_excl"}, want: "n9_int4_excl"},
		{name: "a cast of a function", table: "m7", elements: "(lower(t)::varchar) WITH =", want: "m7_lower_excl"},
		{name: "CAST of a column", table: "n4", elements: "(CAST(r AS text)) WITH =", want: "n4_r_excl"},
		{name: "COALESCE", table: "m3", elements: "(coalesce(r, s)) WITH =", want: "m3_coalesce_excl"},
		{name: "NULLIF", table: "m8", elements: "(nullif(r, 0)) WITH =", want: "m8_nullif_excl"},
		{name: "GREATEST", table: "m9", elements: "(greatest(r, s)) WITH =", want: "m9_greatest_excl"},
		{name: "CASE with a constant ELSE", table: "m4", elements: "(CASE WHEN r > 0 THEN 1 ELSE 0 END) WITH =", want: "m4_case_excl"},
		{name: "CASE with a column ELSE", table: "n5", elements: "(CASE WHEN r > 0 THEN r ELSE s END) WITH =", want: "n5_s_excl"},
		{
			name: "CASE whose ELSE is a CASE", table: "p1",
			elements: "(CASE WHEN r > 0 THEN 1 ELSE CASE WHEN r < 0 THEN 0 ELSE r END END) WITH =",
			want:     "p1_r_excl",
		},
		{name: "ARRAY", table: "n6", elements: "(ARRAY[r]) WITH =", want: "n6_array_excl"},
		{name: "a subscript", table: "p3", elements: "(r[1]) WITH =", want: "p3_r_excl"},
		{
			name: "a long table and column cut to 63 bytes", table: longTable, elements: longColumn + " WITH =",
			want: "a_table_name_that_is_quite_lo_a_column_name_that_is_also_r_excl",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := pgname.Exclude(test.table, test.elements, func(name string) bool {
				return slices.Contains(test.taken, name)
			})

			c.Assert(got, qt.Equals, test.want)
		})
	}
}
