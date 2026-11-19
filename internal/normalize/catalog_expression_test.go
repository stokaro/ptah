package normalize_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/normalize"
)

// TestCatalogExpression folds an expression to the spelling a catalog that
// reprints expressions shares with its declaration. The quotes that open a
// string decide whether the content of a double-quoted token is folded too,
// and the backslash rules decide where a string ends.
func TestCatalogExpression(t *testing.T) {
	standard := normalize.Quoting{}
	postgres := normalize.Quoting{EscapeStrings: true}
	mysql := normalize.Quoting{Strings: `'"`, BackslashEscapes: true}
	tests := []struct {
		name       string
		expression string
		quoting    normalize.Quoting
		want       string
	}{
		{name: "spacing and case", expression: "Price * Quantity", quoting: standard, want: "price*quantity"},
		{name: "backtick quoting", expression: "`Price` + 1", quoting: standard, want: "price+1"},
		{name: "double-quoted names", expression: `"Price" + 1`, quoting: standard, want: "price+1"},
		{name: "double-quoted strings", expression: `concat("Ab C", a)`, quoting: mysql, want: `concat("Ab C",a)`},
		{name: "a single-quoted string keeps its case and spacing", expression: "concat('Ab C', A)", quoting: standard, want: "concat('Ab C',a)"},
		{name: "a doubled quote in a string", expression: "'It''s' || A", quoting: standard, want: "'It''s'||a"},
		{name: "a backslash that escapes a quote", expression: `'a\'b' || A`, quoting: mysql, want: `'a\'b'||a`},
		{name: "a backslash in a standard string", expression: `'p\' || ' A  B'`, quoting: standard, want: `'p\'||' A  B'`},
		{name: "a backslash in a PostgreSQL string", expression: `'p\' || ' A  B'`, quoting: postgres, want: `'p\'||' A  B'`},
		{name: "a backslash in a PostgreSQL escape string", expression: `E'a\'b' || ' A  B'`, quoting: postgres, want: `e'a\'b'||' A  B'`},
		{name: "a name ending in E is no escape string", expression: `typE'a\' || ' A  B'`, quoting: postgres, want: `type'a\'||' A  B'`},
		{name: "a parenthesized name", expression: "(a) + (b.c)", quoting: standard, want: "a+b.c"},
		{name: "nested parentheses around a name", expression: "((a))", quoting: standard, want: "a"},
		{name: "a call keeps its parentheses", expression: "lower(a)", quoting: standard, want: "lower(a)"},
		{name: "an expression keeps its parentheses", expression: "(a + b) * 2", quoting: standard, want: "(a+b)*2"},
		{name: "an unclosed string", expression: "'abc", quoting: standard, want: "'abc"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(normalize.CatalogExpression(test.expression, test.quoting), qt.Equals, test.want)
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
			c.Assert(normalize.RenameFunction(test.expression, "lcase", "lower", normalize.Quoting{Strings: `'"`, BackslashEscapes: true}), qt.Equals, test.want)
		})
	}
}
