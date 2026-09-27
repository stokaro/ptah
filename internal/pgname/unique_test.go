package pgname_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pgname"
)

// TestUnique names an unnamed UNIQUE after every column of its index, the
// INCLUDE columns among them (stokaro/ptah#3863). Each expected name was read
// back from pg_constraint on PostgreSQL 18.6 after `CREATE TABLE <table> (...,
// UNIQUE (<columns>) INCLUDE (<include>))`.
func TestUnique(t *testing.T) {
	tests := []struct {
		name    string
		table   string
		columns []string
		include []string
		taken   []string
		want    string
	}{
		{
			name:  "key columns alone",
			table: "p", columns: []string{"a", "b"},
			want: "p_a_b_key",
		},
		{
			name:  "an INCLUDE column",
			table: "i1", columns: []string{"x"}, include: []string{"y"},
			want: "i1_x_y_key",
		},
		{
			name:  "an INCLUDE column that repeats a key column",
			table: "i4", columns: []string{"x"}, include: []string{"x"},
			want: "i4_x_x1_key",
		},
		{
			name:  "two key and two INCLUDE columns, one repeated",
			table: "i3", columns: []string{"x", "y"}, include: []string{"z", "x"},
			want: "i3_x_y_z_x1_key",
		},
		{
			name:  "a quoted INCLUDE column keeps its case",
			table: "i10", columns: []string{"x"}, include: []string{"Y"},
			want: "i10_x_Y_key",
		},
		{
			name:  "the name is taken",
			table: "i1", columns: []string{"x"}, include: []string{"y"}, taken: []string{"i1_x_y_key"},
			want: "i1_x_y_key1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := pgname.Unique(test.table, test.columns, test.include, func(name string) bool {
				return slices.Contains(test.taken, name)
			})

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestPrimaryKey names an unnamed primary key `<table>_pkey`, numbered past a
// name a constraint already holds. Measured on PostgreSQL 18.6: `id int PRIMARY
// KEY, x int, CONSTRAINT k6_pkey CHECK (x > 0)` names the key k6_pkey1.
func TestPrimaryKey(t *testing.T) {
	tests := []struct {
		name  string
		table string
		taken []string
		want  string
	}{
		{name: "free", table: "k5", want: "k5_pkey"},
		{name: "a CHECK holds the name", table: "k6", taken: []string{"k6_pkey"}, want: "k6_pkey1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := pgname.PrimaryKey(test.table, func(name string) bool { return slices.Contains(test.taken, name) })

			c.Assert(got, qt.Equals, test.want)
		})
	}
}
