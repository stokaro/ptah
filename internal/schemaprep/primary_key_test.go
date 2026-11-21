package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

// TestNeedsPrimaryKeyConstraint_WithoutOverlaps sends a WITHOUT OVERLAPS key to
// the table constraint spelling whatever its width. The clause qualifies a
// column inside the key's list, and the column spelling `col PRIMARY KEY` has
// no list to write it in, so a key there would reach the server as a plain key
// (stokaro/ptah#6330). The plain key of one column is the control.
func TestNeedsPrimaryKeyConstraint_WithoutOverlaps(t *testing.T) {
	tests := []struct {
		name  string
		table schemamodel.Table
		want  bool
	}{
		{
			name:  "a plain key of one column",
			table: schemamodel.Table{Name: "p", PrimaryKey: []string{"valid_at"}},
			want:  false,
		},
		{
			name:  "a temporal key of one column",
			table: schemamodel.Table{Name: "p", PrimaryKey: []string{"valid_at"}, PrimaryKeyWithoutOverlaps: true},
			want:  true,
		},
		{
			name: "a temporal key of two columns",
			table: schemamodel.Table{
				Name: "p", PrimaryKey: []string{"id", "valid_at"}, PrimaryKeyWithoutOverlaps: true,
			},
			want: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(schemaprep.NeedsPrimaryKeyConstraint(test.table, nil), qt.Equals, test.want)
		})
	}
}
