package postgrescomparison_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/postgrescomparison"
)

// TestIndexes_OptionsEqual reads a declared index's access method, storage
// parameters, and each key's operator class and NULLS ordering, against what
// PostgreSQL reports. The reader reports pg_am.amname in lower case and
// records a key's operator class and NULLS ordering only where they are not
// the default.
func TestIndexes_OptionsEqual(t *testing.T) {
	tests := []struct {
		name     string
		declared schemamodel.Index
		reported catalog.Index
		want     bool
	}{
		{
			name:     "no method is btree",
			declared: schemamodel.Index{Fields: []string{"a"}},
			reported: catalog.Index{Method: "btree", Columns: []string{"a"}},
			want:     true,
		},
		{
			name:     "a method in upper case",
			declared: schemamodel.Index{Type: "GIN", Fields: []string{"tsv"}},
			reported: catalog.Index{Method: "gin", Columns: []string{"tsv"}},
			want:     true,
		},
		{
			name:     "btree against hash",
			declared: schemamodel.Index{Type: "hash", Fields: []string{"a"}},
			reported: catalog.Index{Method: "btree", Columns: []string{"a"}},
			want:     false,
		},
		{
			name:     "an index-level operator class applies to each key",
			declared: schemamodel.Index{Operator: "TEXT_PATTERN_OPS", Fields: []string{"a"}},
			reported: catalog.Index{Parts: []catalog.IndexPart{{Name: "a", Operator: "text_pattern_ops"}}},
			want:     true,
		},
		{
			name:     "an operator class the server does not report",
			declared: schemamodel.Index{Parts: []schemamodel.IndexPart{{Name: "a", Operator: "text_pattern_ops"}}},
			reported: catalog.Index{Parts: []catalog.IndexPart{{Name: "a"}}},
			want:     false,
		},
		{
			name: "an explicit spelling of the default ordering",
			declared: schemamodel.Index{Parts: []schemamodel.IndexPart{
				{Name: "a", NullsOrder: "last"}, {Name: "b", Desc: true, NullsOrder: "FIRST"},
			}},
			reported: catalog.Index{Parts: []catalog.IndexPart{{Name: "a"}, {Name: "b", Desc: true}}},
			want:     true,
		},
		{
			name:     "NULLS FIRST on an ascending key",
			declared: schemamodel.Index{Parts: []schemamodel.IndexPart{{Name: "a", NullsOrder: "FIRST"}}},
			reported: catalog.Index{Parts: []catalog.IndexPart{{Name: "a"}}},
			want:     false,
		},
		{
			name:     "storage parameters in another case and spacing",
			declared: schemamodel.Index{Type: "brin", Fields: []string{"a"}, StorageParams: map[string]string{"pages_per_range": " 32 "}},
			reported: catalog.Index{Method: "brin", Columns: []string{"a"}, StorageParams: map[string]string{"pages_per_range": "32"}},
			want:     true,
		},
		{
			name:     "a storage parameter the server does not hold",
			declared: schemamodel.Index{Fields: []string{"a"}, StorageParams: map[string]string{"fillfactor": "70"}},
			reported: catalog.Index{Columns: []string{"a"}},
			want:     false,
		},
		{
			name:     "another number of keys is the shared comparison's",
			declared: schemamodel.Index{Fields: []string{"a", "b"}},
			reported: catalog.Index{Columns: []string{"a"}},
			want:     true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(postgrescomparison.Indexes{}.OptionsEqual(test.declared, test.reported), qt.Equals, test.want)
		})
	}
}

// TestIndexes_RenameCarries pins that a PostgreSQL index carries no facet
// through a rename, so the comparison rebuilds an index with a facet.
func TestIndexes_RenameCarries(t *testing.T) {
	c := qt.New(t)
	c.Assert(postgrescomparison.Indexes{}.RenameCarries(schemaext.Kind("example.org/facet")), qt.IsFalse)
}

// TestGeneratedExpressions_Spelling reads a generated column's expression the
// way PostgreSQL prints it back: without the casts its parser inserted, and
// with the names unquoted, the spacing removed and the case folded.
func TestGeneratedExpressions_Spelling(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		want       string
	}{
		{name: "a declaration", expression: "price * quantity", want: "price*quantity"},
		{name: "what the catalog prints", expression: `("Price" * (quantity)::numeric)`, want: "(price*quantity)"},
		{name: "a cast with arguments", expression: "amount::numeric(10, 2) + 1", want: "amount+1"},
		{name: "a cast to a multi-word type", expression: "code::character varying", want: "code"},
		{name: "a cast inside a string is text", expression: "'a::b' || name", want: "'a::b'||name"},
		{name: "a double-quoted token is a name", expression: `"Total" + 1`, want: "total+1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(postgrescomparison.GeneratedExpressions{}.Spelling(test.expression), qt.Equals, test.want)
		})
	}
}

// TestRules pins the rules PostgreSQL registers: its index options and its
// generated expressions, and nothing it leaves to the shared comparison.
func TestRules(t *testing.T) {
	c := qt.New(t)
	rules := postgrescomparison.Rules()
	c.Assert(rules.Indexes, qt.Equals, postgrescomparison.Indexes{})
	c.Assert(rules.GeneratedExpressions, qt.Equals, postgrescomparison.GeneratedExpressions{})
	c.Assert(rules.IndexDefinitions, qt.IsNil)
	c.Assert(rules.IndexPredicates, qt.IsNil)
	c.Assert(rules.ForeignKeyIndexes, qt.IsNil)
	c.Assert(rules.Columns, qt.IsNil)
	c.Assert(rules.Views, qt.IsNil)
	c.Assert(rules.Routines, qt.IsNil)
}
