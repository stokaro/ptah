package clickhousecomparison_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/clickhousecomparison"
)

// TestIndexDefinitions_DefinitionChanged compares a data-skipping index's key
// as the one expression system.data_skipping_indices reports, and nothing
// else of the definition: an index has no uniqueness and no key direction on
// ClickHouse, and its type and granularity are the owner's facet.
func TestIndexDefinitions_DefinitionChanged(t *testing.T) {
	tests := []struct {
		name     string
		declared schemamodel.Index
		reported catalog.Index
		want     bool
	}{
		{
			name:     "two fields against the reported list",
			declared: schemamodel.Index{Fields: []string{"a", "b"}},
			reported: catalog.Index{Parts: []catalog.IndexPart{{Expr: "a, b"}}},
			want:     false,
		},
		{
			name:     "an expression in the server's formatting",
			declared: schemamodel.Index{Parts: []schemamodel.IndexPart{{Expr: "cityHash64(a)%10"}}},
			reported: catalog.Index{Parts: []catalog.IndexPart{{Expr: "cityHash64(a) % 10"}}},
			want:     false,
		},
		{
			name:     "uniqueness is not compared",
			declared: schemamodel.Index{Unique: true, Fields: []string{"s"}},
			reported: catalog.Index{Columns: []string{"s"}},
			want:     false,
		},
		{
			name:     "another key",
			declared: schemamodel.Index{Fields: []string{"a"}},
			reported: catalog.Index{Parts: []catalog.IndexPart{{Expr: "b"}}},
			want:     true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			changed := clickhousecomparison.IndexDefinitions{}.DefinitionChanged(test.declared, test.reported, identifier.ForDialect("clickhouse"))
			c.Assert(changed, qt.Equals, test.want)
		})
	}
}

// TestRules pins the rules ClickHouse registers: whole index definitions, and
// nothing it leaves to the shared comparison.
func TestRules(t *testing.T) {
	c := qt.New(t)
	rules := clickhousecomparison.Rules()
	c.Assert(rules.IndexDefinitions, qt.Equals, clickhousecomparison.IndexDefinitions{})
	c.Assert(rules.Indexes, qt.IsNil)
	c.Assert(rules.IndexPredicates, qt.IsNil)
	c.Assert(rules.ForeignKeyIndexes, qt.IsNil)
	c.Assert(rules.GeneratedExpressions, qt.IsNil)
	c.Assert(rules.Columns, qt.IsNil)
	c.Assert(rules.Views, qt.IsNil)
	c.Assert(rules.Routines, qt.IsNil)
}
