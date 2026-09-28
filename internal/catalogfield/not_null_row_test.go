package catalogfield_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/catalogfield"
)

// TestIsNotNullRow tells a column's NOT NULL, as PostgreSQL lists it, from a
// CHECK its author named with the same suffix (stokaro/ptah#3935).
func TestIsNotNullRow(t *testing.T) {
	tests := []struct {
		name       string
		constraint catalog.Constraint
		want       bool
	}{
		{
			name:       "PostgreSQL 17 lists a NOT NULL as a CHECK",
			constraint: catalog.Constraint{Type: "CHECK", Name: "2200_16390_2_not_null", CheckClause: new("p IS NOT NULL")},
			want:       true,
		},
		{
			name:       "PostgreSQL 18 keeps a NOT NULL with no condition",
			constraint: catalog.Constraint{Type: "CHECK", Name: "c_p_not_null"},
			want:       true,
		},
		{
			name:       "a CHECK its author named with the suffix",
			constraint: catalog.Constraint{Type: "CHECK", Name: "p_not_null", CheckClause: new("p > 0")},
			want:       false,
		},
		{
			name:       "a CHECK on two columns being set",
			constraint: catalog.Constraint{Type: "CHECK", Name: "pq_not_null", CheckClause: new("p IS NOT NULL AND q IS NOT NULL")},
			want:       false,
		},
		{
			name:       "an IS NOT NULL CHECK under another name",
			constraint: catalog.Constraint{Type: "CHECK", Name: "ck", CheckClause: new("p IS NOT NULL")},
			want:       false,
		},
		{
			name:       "a UNIQUE with the suffix",
			constraint: catalog.Constraint{Type: "UNIQUE", Name: "p_not_null"},
			want:       false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(catalogfield.IsNotNullRow(test.constraint), qt.Equals, test.want)
		})
	}
}
