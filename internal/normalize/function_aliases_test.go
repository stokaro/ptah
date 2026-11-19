package normalize_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/normalize"
)

var caseAliases = map[string]string{"ucase": "upper", "lcase": "lower"}

// A call to an alias takes the name it stands for, in any case, and the same
// word inside a string, as a name without a call, or as part of a longer word
// is kept.
func TestFunctionAliases(t *testing.T) {
	rows := []struct {
		name, expression string
		backslashEscapes bool
		want             string
	}{
		{name: "a call", expression: "ucase('abc')", want: "upper('abc')"},
		{name: "upper case and nested", expression: "LCASE(UCASE(a))", want: "lower(upper(a))"},
		{name: "a name without a call", expression: "ucase + 1", want: "ucase + 1"},
		{name: "a space before the parenthesis", expression: "ucase ('a')", want: "ucase ('a')"},
		{name: "a longer word", expression: "my_ucase('a')", want: "my_ucase('a')"},
		{name: "inside a string", expression: "concat('ucase(', ucase('x'))", want: "concat('ucase(', upper('x'))"},
		{name: "a doubled quote", expression: "concat('it''s ucase(', lcase('X'))", want: "concat('it''s ucase(', lower('X'))"},
		{
			name: "a quote behind a backslash", expression: `concat('a\' ucase(', lcase('X'))`, backslashEscapes: true,
			want: `concat('a\' ucase(', lower('X'))`,
		},
		{
			name: "a backslash that escapes nothing", expression: `concat('a\', ucase('x'))`,
			want: `concat('a\', upper('x'))`,
		},
		{name: "an unclosed string", expression: "ucase('abc", want: "upper('abc"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(normalize.FunctionAliases(row.expression, caseAliases, row.backslashEscapes), qt.Equals, row.want)
		})
	}
}

// No aliases keeps the expression as written.
func TestFunctionAliases_None(t *testing.T) {
	c := qt.New(t)
	c.Assert(normalize.FunctionAliases("ucase('abc')", nil, true), qt.Equals, "ucase('abc')")
}
