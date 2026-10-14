package postgres

// White-box testing required: applyPostgresIndexOptions decodes the
// pg_index.indoption bitmasks the index query fetches beside the key texts,
// and the bitmasks have no other source. Which ordering a dialect records is
// not observable through the exported reader API without a live server.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
)

// TestApplyPostgresIndexOptions_NullsOrder pins which NULLS ordering each
// dialect records. PostgreSQL records the one that contradicts its direction's
// default. CockroachDB records none: it has no NULLS clause in an index key,
// and v26.3.2 reports 2 for `(a)` and 1 for `(b DESC)`, its own fixed order.
func TestApplyPostgresIndexOptions_NullsOrder(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		options string
		want    []catalog.IndexPart
	}{
		{
			name:    "PostgreSQL defaults",
			dialect: platform.Postgres,
			options: "[0, 3]",
			want:    []catalog.IndexPart{{Name: "a"}, {Name: "b", Desc: true}},
		},
		{
			name:    "PostgreSQL orderings against the direction",
			dialect: platform.Postgres,
			options: "[2, 1]",
			want: []catalog.IndexPart{
				{Name: "a", NullsOrder: catalog.NullsOrderFirst},
				{Name: "b", Desc: true, NullsOrder: catalog.NullsOrderLast},
			},
		},
		{
			name:    "CockroachDB's fixed order",
			dialect: platform.CockroachDB,
			options: "[2, 1]",
			want:    []catalog.IndexPart{{Name: "a"}, {Name: "b", Desc: true}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := applyPostgresIndexOptions(test.dialect, []catalog.IndexPart{{Name: "a"}, {Name: "b"}}, test.options)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
