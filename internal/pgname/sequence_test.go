package pgname_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pgname"
)

// Each expected name is the one PostgreSQL 18.6 gave the sequence of a serial
// or identity column, read back from pg_class.
func TestSequence(t *testing.T) {
	tests := []struct {
		name   string
		table  string
		column string
		want   string
	}{
		{
			name:  "short names keep their case",
			table: "MixedCase", column: "Id",
			want: "MixedCase_Id_seq",
		},
		{
			name:  "both long, the table longer",
			table: "a_table_name_that_is_quite_long_for_testing_purposes_only_x", column: "a_column_name_that_is_also_rather_long_for_test_xyz",
			want: "a_table_name_that_is_quite_lo_a_column_name_that_is_also_ra_seq",
		},
		{
			name:  "only the table long",
			table: "a_table_name_that_is_quite_long_for_testing_purposes_only_x", column: "b",
			want: "a_table_name_that_is_quite_long_for_testing_purposes_only_b_seq",
		},
		{
			name:  "multibyte names that fit",
			table: "ünï", column: "ñameßßßßßßßßßßßßßßßßßßßßßßß",
			want: "ünï_ñameßßßßßßßßßßßßßßßßßßßßßßß_seq",
		},
		{
			name:  "the cut falls inside a multibyte character",
			table: "tä", column: "ßßßßßßßßßßßßßßßßßßßßßßßßßßßßßa",
			want: "tä_ßßßßßßßßßßßßßßßßßßßßßßßßßßß_seq",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgname.Sequence(test.table, test.column), qt.Equals, test.want)
		})
	}
}
