package sqlservercomparison_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlservercomparison"
)

// TestIndexDefinitions_DefinitionChanged compares a SQL Server index's
// uniqueness and its key columns with their directions, in order, under the
// target's column-name comparison, and nothing else of the definition.
func TestIndexDefinitions_DefinitionChanged(t *testing.T) {
	tests := []struct {
		name     string
		declared schemamodel.Index
		reported catalog.Index
		want     bool
	}{
		{
			name:     "the same index",
			declared: schemamodel.Index{Fields: []string{"email"}},
			reported: catalog.Index{Columns: []string{"email"}},
			want:     false,
		},
		{
			name:     "a unique index against a plain one",
			declared: schemamodel.Index{Unique: true, Fields: []string{"email"}},
			reported: catalog.Index{Columns: []string{"email"}},
			want:     true,
		},
		{
			name:     "another column",
			declared: schemamodel.Index{Fields: []string{"status"}},
			reported: catalog.Index{Columns: []string{"email"}},
			want:     true,
		},
		{
			name:     "another direction",
			declared: schemamodel.Index{Parts: []schemamodel.IndexPart{{Name: "email", Desc: true}}},
			reported: catalog.Index{Parts: []catalog.IndexPart{{Name: "email"}}},
			want:     true,
		},
		{
			name:     "another number of columns",
			declared: schemamodel.Index{Fields: []string{"email", "status"}},
			reported: catalog.Index{Columns: []string{"email"}},
			want:     true,
		},
		{
			name:     "an access method is not compared",
			declared: schemamodel.Index{Type: "NONCLUSTERED", Fields: []string{"email"}},
			reported: catalog.Index{Method: "CLUSTERED", Columns: []string{"email"}},
			want:     false,
		},
	}
	semantics := identifier.ForDialect("sqlserver")
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			changed := sqlservercomparison.IndexDefinitions{}.DefinitionChanged(test.declared, test.reported, semantics)
			c.Assert(changed, qt.Equals, test.want)
		})
	}
}

// TestIndexPredicates_Spelling removes the decorations SQL Server adds to the
// filter_definition it stores: brackets around names and parentheses around a
// bare number, and nothing inside a string literal.
func TestIndexPredicates_Spelling(t *testing.T) {
	tests := []struct {
		name      string
		predicate string
		want      string
	}{
		{name: "what the catalog stores", predicate: "([status]=(1))", want: "(status=1)"},
		{name: "a declaration", predicate: "status = 1", want: "status = 1"},
		{name: "a negative number", predicate: "([delta]>( -2.5 ))", want: "(delta>-2.5)"},
		{name: "brackets in a string literal", predicate: "([name]='[x]')", want: "(name='[x]')"},
		{name: "a doubled quote in a string literal", predicate: "([name]='it''s (1)')", want: "(name='it''s (1)')"},
		{name: "a parenthesized expression is kept", predicate: "([a]=(1+2))", want: "(a=(1+2))"},
		{name: "nested decorations", predicate: "(((1)))", want: "1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlservercomparison.IndexPredicates{}.Spelling(test.predicate), qt.Equals, test.want)
		})
	}
}

// TestGeneratedExpressions_Spelling reads a computed column's expression the
// way SQL Server prints it back: names out of their brackets, without spaces,
// in lower case, and a string literal as written.
func TestGeneratedExpressions_Spelling(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		want       string
	}{
		{name: "a declaration", expression: "Price * Quantity", want: "price*quantity"},
		{name: "what the catalog prints", expression: "[Price]*[Quantity]", want: "price*quantity"},
		{name: "a bracketed name with a space", expression: "[Unit Price] * 2", want: "unit price*2"},
		{name: "a doubled bracket in a name", expression: "[a]]b]", want: "a]b"},
		{name: "a string literal keeps its case", expression: "CONCAT([a], 'Hello World')", want: "concat(a,'Hello World')"},
		{name: "a doubled quote in a string literal", expression: "'It''s'", want: "'It''s'"},
		{name: "an unclosed bracket", expression: "[Ab C", want: "ab c[abc"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlservercomparison.GeneratedExpressions{}.Spelling(test.expression), qt.Equals, test.want)
		})
	}
}

// TestRules pins the rules SQL Server registers: whole index definitions,
// filtered index predicates and computed column expressions, and nothing it
// leaves to the shared comparison.
func TestRules(t *testing.T) {
	c := qt.New(t)
	rules := sqlservercomparison.Rules()
	c.Assert(rules.IndexDefinitions, qt.Equals, sqlservercomparison.IndexDefinitions{})
	c.Assert(rules.IndexPredicates, qt.Equals, sqlservercomparison.IndexPredicates{})
	c.Assert(rules.GeneratedExpressions, qt.Equals, sqlservercomparison.GeneratedExpressions{})
	c.Assert(rules.Indexes, qt.IsNil)
	c.Assert(rules.ForeignKeyIndexes, qt.IsNil)
	c.Assert(rules.Columns, qt.IsNil)
	c.Assert(rules.Views, qt.IsNil)
	c.Assert(rules.Routines, qt.IsNil)
}
