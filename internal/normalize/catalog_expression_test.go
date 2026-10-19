package normalize_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/normalize"
)

// TestCatalogExpression folds an expression to the spelling a catalog that
// reprints expressions shares with its declaration. The quotes that open a
// string decide whether the content of a double-quoted token is folded too.
func TestCatalogExpression(t *testing.T) {
	tests := []struct {
		name         string
		expression   string
		stringQuotes string
		want         string
	}{
		{name: "spacing and case", expression: "Price * Quantity", stringQuotes: "'", want: "price*quantity"},
		{name: "backtick quoting", expression: "`Price` + 1", stringQuotes: "'", want: "price+1"},
		{name: "double-quoted names", expression: `"Price" + 1`, stringQuotes: "'", want: "price+1"},
		{name: "double-quoted strings", expression: `concat("Ab C", a)`, stringQuotes: `'"`, want: `concat("Ab C",a)`},
		{name: "a single-quoted string keeps its case and spacing", expression: "concat('Ab C', A)", stringQuotes: "'", want: "concat('Ab C',a)"},
		{name: "a doubled quote in a string", expression: "'It''s' || A", stringQuotes: "'", want: "'It''s'||a"},
		{name: "a backslash in a string", expression: `'a\'b' || A`, stringQuotes: "'", want: `'a\'b'||a`},
		{name: "a parenthesized name", expression: "(a) + (b.c)", stringQuotes: "'", want: "a+b.c"},
		{name: "nested parentheses around a name", expression: "((a))", stringQuotes: "'", want: "a"},
		{name: "a call keeps its parentheses", expression: "lower(a)", stringQuotes: "'", want: "lower(a)"},
		{name: "an expression keeps its parentheses", expression: "(a + b) * 2", stringQuotes: "'", want: "(a+b)*2"},
		{name: "an unclosed string", expression: "'abc", stringQuotes: "'", want: "'abc"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(normalize.CatalogExpression(test.expression, test.stringQuotes), qt.Equals, test.want)
		})
	}
}

// TestRenameFunction replaces the calls of one function with calls of
// another, and nothing that merely contains the name.
func TestRenameFunction(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		want       string
	}{
		{name: "a call", expression: "lcase(a)", want: "lower(a)"},
		{name: "a nested call", expression: "concat(lcase(a), lcase(b))", want: "concat(lower(a), lower(b))"},
		{name: "a longer name", expression: "xlcase(a)", want: "xlcase(a)"},
		{name: "a name without a call", expression: "lcase + 1", want: "lcase + 1"},
		{name: "in a single-quoted string", expression: "'lcase(a)'", want: "'lcase(a)'"},
		{name: "in a double-quoted string", expression: `"lcase(a)"`, want: `"lcase(a)"`},
		{name: "another case is another name", expression: "LCASE(a)", want: "LCASE(a)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(normalize.RenameFunction(test.expression, "lcase", "lower"), qt.Equals, test.want)
		})
	}
}
