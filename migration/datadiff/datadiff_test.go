package datadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/datadiff"
)

func TestCompute(t *testing.T) {

	tests := []struct {
		name    string
		table   string
		keys    []string
		desired []datadiff.Row
		live    []datadiff.Row
		want    *datadiff.DataDiff
	}{
		{
			name:  "all inserts when live is empty",
			table: "regions",
			keys:  []string{"code"},
			desired: []datadiff.Row{
				{"code": "US", "name": "United States"},
				{"code": "CZ", "name": "Czechia"},
			},
			live: nil,
			want: &datadiff.DataDiff{
				Table: "regions",
				Keys:  []string{"code"},
				Inserts: []datadiff.Row{
					{"code": "CZ", "name": "Czechia"},
					{"code": "US", "name": "United States"},
				},
			},
		},
		{
			name:    "all deletes when desired is empty",
			table:   "regions",
			keys:    []string{"code"},
			desired: nil,
			live: []datadiff.Row{
				{"code": "US", "name": "United States"},
				{"code": "CZ", "name": "Czechia"},
			},
			want: &datadiff.DataDiff{
				Table: "regions",
				Keys:  []string{"code"},
				Deletes: []datadiff.Row{
					{"code": "CZ", "name": "Czechia"},
					{"code": "US", "name": "United States"},
				},
			},
		},
		{
			name:  "mixed insert update delete with deterministic ordering",
			table: "regions",
			keys:  []string{"code"},
			// Deliberately unsorted input to prove the output is sorted by key.
			desired: []datadiff.Row{
				{"code": "FR", "name": "France"},
				{"code": "US", "name": "USA"},
				{"code": "AT", "name": "Austria"},
				{"code": "CZ", "name": "Czechia"},
			},
			live: []datadiff.Row{
				{"code": "ZZ", "name": "Zeta"},
				{"code": "US", "name": "United States"},
				{"code": "FR", "name": "France"},
				{"code": "DE", "name": "Germany"},
			},
			want: &datadiff.DataDiff{
				Table: "regions",
				Keys:  []string{"code"},
				Inserts: []datadiff.Row{
					{"code": "AT", "name": "Austria"},
					{"code": "CZ", "name": "Czechia"},
				},
				Updates: []datadiff.RowUpdate{
					{
						Key:     map[string]any{"code": "US"},
						Desired: datadiff.Row{"code": "US", "name": "USA"},
						Live:    datadiff.Row{"code": "US", "name": "United States"},
					},
				},
				Deletes: []datadiff.Row{
					{"code": "DE", "name": "Germany"},
					{"code": "ZZ", "name": "Zeta"},
				},
			},
		},
		{
			name:  "no change yields empty diff",
			table: "regions",
			keys:  []string{"code"},
			desired: []datadiff.Row{
				{"code": "US", "name": "United States"},
			},
			live: []datadiff.Row{
				{"code": "US", "name": "United States"},
			},
			want: &datadiff.DataDiff{Table: "regions", Keys: []string{"code"}},
		},
		{
			name: "composite keys",
			// composite (tenant, code); one insert, one delete, one unchanged.
			table: "prices",
			keys:  []string{"tenant", "code"},
			desired: []datadiff.Row{
				{"tenant": 1, "code": "A", "val": "x"},
				{"tenant": 2, "code": "A", "val": "y"},
			},
			live: []datadiff.Row{
				{"tenant": 1, "code": "A", "val": "x"},
				{"tenant": 1, "code": "B", "val": "z"},
			},
			want: &datadiff.DataDiff{
				Table: "prices",
				Keys:  []string{"tenant", "code"},
				Inserts: []datadiff.Row{
					{"tenant": 2, "code": "A", "val": "y"},
				},
				Deletes: []datadiff.Row{
					{"tenant": 1, "code": "B", "val": "z"},
				},
			},
		},
		{
			name:  "live-only column does not trigger update",
			table: "regions",
			keys:  []string{"code"},
			desired: []datadiff.Row{
				{"code": "US", "name": "United States"},
			},
			// live carries an extra managed-unaware column; it must be ignored.
			live: []datadiff.Row{
				{"code": "US", "name": "United States", "population": int64(331)},
			},
			want: &datadiff.DataDiff{Table: "regions", Keys: []string{"code"}},
		},
		{
			name:  "live NULL versus desired empty string is an update",
			table: "regions",
			keys:  []string{"code"},
			// A live SQL NULL (nil) must stay distinct from a desired empty
			// string, so the desired "" is reported as a change to apply.
			desired: []datadiff.Row{
				{"code": "US", "note": ""},
			},
			live: []datadiff.Row{
				{"code": "US", "note": nil},
			},
			want: &datadiff.DataDiff{
				Table: "regions",
				Keys:  []string{"code"},
				Updates: []datadiff.RowUpdate{
					{
						Key:     map[string]any{"code": "US"},
						Desired: datadiff.Row{"code": "US", "note": ""},
						Live:    datadiff.Row{"code": "US", "note": nil},
					},
				},
			},
		},
		{
			name:  "normalized values compare equal across numeric types",
			table: "regions",
			keys:  []string{"id"},
			// desired id is int, live id is int64; both key match and column
			// comparison must treat them as equal, yielding no diff.
			desired: []datadiff.Row{
				{"id": 1, "name": "x"},
			},
			live: []datadiff.Row{
				{"id": int64(1), "name": "x"},
			},
			want: &datadiff.DataDiff{Table: "regions", Keys: []string{"id"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := datadiff.Compute("", tt.table, tt.keys, tt.desired, tt.live)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

func TestComputeErrors(t *testing.T) {

	tests := []struct {
		name    string
		keys    []string
		desired []datadiff.Row
		live    []datadiff.Row
	}{
		{
			name:    "empty keys",
			keys:    nil,
			desired: []datadiff.Row{{"code": "US"}},
			live:    nil,
		},
		{
			name:    "desired row missing key column",
			keys:    []string{"code"},
			desired: []datadiff.Row{{"name": "United States"}},
			live:    nil,
		},
		{
			name:    "live row missing key column",
			keys:    []string{"code"},
			desired: []datadiff.Row{{"code": "US"}},
			live:    []datadiff.Row{{"name": "Germany"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := datadiff.Compute("", "regions", tt.keys, tt.desired, tt.live)
			c.Assert(err, qt.IsNotNil)
			c.Assert(got, qt.IsNil)
		})
	}
}
