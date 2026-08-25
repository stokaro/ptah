package clickhouse

// White-box testing required: primaryKeyBeyondSortingKey is package-local and
// the field it fills is not reachable through an exported API on its own.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestPrimaryKeyBeyondSortingKey_CarriesAPrefixKey pins the tuning a MergeTree
// table is given.
//
// A primary key that is a prefix of the ORDER BY is the point of the pair: the
// sparse index holds one mark per granule of the prefix and the rest of the
// sorting key only orders rows inside it. Measured on ClickHouse 26.7.5.10,
// `PRIMARY KEY (id) ORDER BY (id, s)` read and replayed came back with
// primary_key `id, s` -- a wider index over the same rows, and nothing said so
// (stokaro/ptah#2198).
func TestPrimaryKeyBeyondSortingKey_CarriesAPrefixKey(t *testing.T) {
	tests := []struct {
		name       string
		primaryKey string
		sortingKey string
		want       string
	}{
		{
			name:       "a prefix of the sorting key is carried",
			primaryKey: "(id)",
			sortingKey: "id, s",
			want:       "(id)",
		},
		{
			// The control, and the common case: naming it again would put the
			// same expression in the statement twice.
			name:       "a primary key equal to the sorting key is not carried",
			primaryKey: "id, s",
			sortingKey: "id, s",
			want:       "",
		},
		{
			// The catalog parenthesizes one column and not the other for the
			// same table, so the pair has to be compared after that is stripped
			// or every single-column table gains an override that changes
			// nothing.
			name:       "the parentheses the catalog adds to one side do not make a difference",
			primaryKey: "(id)",
			sortingKey: "id",
			want:       "",
		},
		{
			name:       "a table with no keys at all carries none",
			primaryKey: "",
			sortingKey: "",
			want:       "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(primaryKeyBeyondSortingKey(test.primaryKey, test.sortingKey), qt.Equals, test.want)
		})
	}
}
