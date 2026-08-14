package postgres

// White-box testing required: parsePostgresIndexParts decodes the
// pg_index.indkey attribute-number vector that readIndexesForSchema fetches
// alongside the index key texts. Mapping attnum 0 to "this key is an
// expression" is the whole of the fix for #1242, and it is not observable
// through the exported reader API without a live PostgreSQL server, because
// the attnum vector has no other source.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
)

func TestParsePostgresIndexParts_HappyPath(t *testing.T) {

	tests := []struct {
		name     string
		columns  []string
		attnums  string
		expected []types.DBIndexPart
	}{
		{
			// Measured on PostgreSQL 17.10: CREATE INDEX i ON t (lower(name))
			// reports indkey {0} and key text "lower(name)".
			name:     "single expression key",
			columns:  []string{"lower(name)"},
			attnums:  "[0]",
			expected: []types.DBIndexPart{{Expr: "lower(name)"}},
		},
		{
			name:     "single column key",
			columns:  []string{"plain"},
			attnums:  "[2]",
			expected: []types.DBIndexPart{{Name: "plain"}},
		},
		{
			name:    "column and expression in one index",
			columns: []string{"tenant_id", "lower(name)"},
			attnums: "[2,0]",
			expected: []types.DBIndexPart{
				{Name: "tenant_id"},
				{Expr: "lower(name)"},
			},
		},
		{
			// A column whose name is spelled like a call must stay a column:
			// its attnum is positive, so the attnum vector is what separates
			// it from the expression case above.
			name:     "column named like a function call",
			columns:  []string{"lower(name)"},
			attnums:  "[3]",
			expected: []types.DBIndexPart{{Name: "lower(name)"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			parts, err := parsePostgresIndexParts(test.columns, test.attnums)
			c.Assert(err, qt.IsNil)
			c.Assert(parts, qt.DeepEquals, test.expected)
		})
	}
}

func TestParsePostgresIndexParts_FallsBackToColumnsOnly(t *testing.T) {

	// Returning nil leaves DBIndex.Parts empty, which the rest of the pipeline
	// reads as "this reader supplied only the legacy Columns form" rather than
	// as "this index has no keys".
	tests := []struct {
		name    string
		columns []string
		attnums string
	}{
		{name: "empty attnum text", columns: []string{"a"}, attnums: ""},
		{name: "empty attnum array", columns: []string{"a"}, attnums: "[]"},
		{name: "fewer attnums than keys", columns: []string{"a", "b"}, attnums: "[2]"},
		{name: "more attnums than keys", columns: []string{"a"}, attnums: "[2,3]"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			parts, err := parsePostgresIndexParts(test.columns, test.attnums)
			c.Assert(err, qt.IsNil)
			c.Assert(parts, qt.IsNil)
		})
	}
}

func TestParsePostgresIndexParts_FailurePath(t *testing.T) {

	t.Run("attnum text is not json", func(t *testing.T) {
		c := qt.New(t)
		parts, err := parsePostgresIndexParts([]string{"a"}, "{not json}")
		c.Assert(err, qt.IsNotNil)
		c.Assert(parts, qt.IsNil)
	})
}
