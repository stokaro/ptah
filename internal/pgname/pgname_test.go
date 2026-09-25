package pgname_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pgname"
)

// Each expected name is the one PostgreSQL 18.6 gave the inline UNIQUE of
// `CREATE TABLE <table> (id int, <column> int UNIQUE)`, read back from
// pg_constraint. The table and column are the names the server kept, so the
// multibyte row starts from identifiers it had already cut to 63 bytes.
func TestColumnKey(t *testing.T) {
	tests := []struct {
		name   string
		table  string
		column string
		want   string
	}{
		{
			name:  "short names",
			table: "t", column: "c",
			want: "t_c_key",
		},
		{
			name:  "both long, the table longer",
			table: "t3649_a_rather_long_table_name_for_truncation", column: "a_rather_long_column_name_too",
			want: "t3649_a_rather_long_table_nam_a_rather_long_column_name_too_key",
		},
		{
			name:  "only the column long",
			table: "short_t", column: "a_column_name_that_is_long_enough_to_need_truncation_on_its_own",
			want: "short_t_a_column_name_that_is_long_enough_to_need_truncatio_key",
		},
		{
			name:  "only the table long",
			table: "a_table_name_that_is_long_enough_to_need_truncation_on_its_own", column: "c",
			want: "a_table_name_that_is_long_enough_to_need_truncation_on_it_c_key",
		},
		{
			name:  "both long, cut to the same length",
			table: "a_table_name_that_is_exactly_thirty_bytes_long_x", column: "a_column_name_that_is_exactly_thirty_bytes",
			want: "a_table_name_that_is_exactly__a_column_name_that_is_exactly_key",
		},
		{
			name:  "multibyte names cut back to a character boundary",
			table: "äöüäöüäöüäöüäöüäöüäöüäöüäöüäöüä", column: "ßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßß",
			want: "äöüäöüäöüäöüäö_ßßßßßßßßßßßßßß_key",
		},
		{
			name:  "names that need quoting",
			table: "Mixed Case", column: "Some Col",
			want: "Mixed Case_Some Col_key",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := pgname.ColumnKey(test.table, test.column)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// Object leaves out the parts it is not given, as makeObjectName does. The
// last three rows are names PostgreSQL 18.6 chose: the primary key and the
// NOT NULL constraint of `CREATE TABLE <that name> (id int PRIMARY KEY)`, and
// an inline REFERENCES. The label `fkey` leaves an odd number of bytes for
// the two names, so they cannot come out the same length, and the second is
// the one that gives up the last byte.
func TestObject(t *testing.T) {
	tests := []struct {
		name  string
		name1 string
		name2 string
		label string
		want  string
	}{
		{name: "no second name", name1: "orders", label: "pkey", want: "orders_pkey"},
		{name: "no label", name1: "orders", name2: "total", want: "orders_total"},
		{
			name:  "no second name, the first cut to fit the label",
			name1: "a_table_name_that_is_long_enough_to_need_truncation_on_its_own_", label: "pkey",
			want: "a_table_name_that_is_long_enough_to_need_truncation_on_its_pkey",
		},
		{
			name:  "a label longer than the key label",
			name1: "a_table_name_that_is_long_enough_to_need_truncation_on_its_own_", name2: "id", label: "not_null",
			want: "a_table_name_that_is_long_enough_to_need_truncation_id_not_null",
		},
		{
			name:  "an odd number of bytes left, the second name cut last",
			name1: "forty_bytes_table_name_forty_bytes_long_x", name2: "forty_bytes_column_name_forty_bytes_long", label: "fkey",
			want: "forty_bytes_table_name_forty__forty_bytes_column_name_fort_fkey",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := pgname.Object(test.name1, test.name2, test.label)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// Constraint joins the columns and numbers a taken name, as
// ChooseConstraintName does. The rows are names PostgreSQL 18.6 chose.
func TestConstraint(t *testing.T) {
	tests := []struct {
		name    string
		table   string
		columns []string
		label   string
		taken   []string
		want    string
	}{
		{name: "a foreign key over one column", table: "child", columns: []string{"parent_id"}, label: "fkey", want: "child_parent_id_fkey"},
		{name: "a foreign key over two columns", table: "child", columns: []string{"a", "b"}, label: "fkey", want: "child_a_b_fkey"},
		{name: "a second key over the same column", table: "twice", columns: []string{"p"}, label: "fkey", taken: []string{"twice_p_fkey"}, want: "twice_p_fkey1"},
		{name: "a UNIQUE beside an index of its name", table: "q", columns: []string{"a"}, label: "key", taken: []string{"q_a_key"}, want: "q_a_key1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := pgname.Constraint(test.table, test.columns, test.label, func(name string) bool {
				return slices.Contains(test.taken, name)
			})

			c.Assert(got, qt.Equals, test.want)
		})
	}
}
