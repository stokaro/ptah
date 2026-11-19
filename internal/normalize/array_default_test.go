package normalize_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/normalize"
)

// TestDefaultValue_ReadsAnArrayDefaultAsItsText pins the text an array
// default of an array column compares as: the one PostgreSQL prints, whether
// the default is an array literal or an ARRAY constructor. An element keeps
// double quotes where PostgreSQL prints them, so the string NULL and no value
// stay apart. A default the reading does not take is compared as written.
func TestDefaultValue_ReadsAnArrayDefaultAsItsText(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		typeName string
		want     string
	}{
		{name: "an empty constructor", input: "ARRAY[]", typeName: "text[]", want: "{}"},
		{name: "an empty literal", input: "'{}'", typeName: "text[]", want: "{}"},
		{name: "an empty literal cast to its type", input: "'{}'::text[]", typeName: "text[]", want: "{}"},
		{name: "a constructor of strings", input: "ARRAY['a', 'b']", typeName: "text[]", want: "{a,b}"},
		{name: "a constructor of strings cast to the element type", input: "ARRAY['a'::text, 'b'::text]", typeName: "text[]", want: "{a,b}"},
		{name: "a literal of strings", input: "'{a,b}'", typeName: "text[]", want: "{a,b}"},
		{name: "a literal with spaces around its elements", input: `'{ a , "b c" }'`, typeName: "text[]", want: `{a,"b c"}`},
		{name: "a constructor with an element that needs quotes", input: "ARRAY['b c', NULL]", typeName: "text[]", want: `{"b c",NULL}`},
		{name: "the string NULL", input: `'{"NULL"}'`, typeName: "text[]", want: `{"NULL"}`},
		{name: "no value", input: "'{NULL}'", typeName: "text[]", want: "{NULL}"},
		{name: "a doubled quote in a constructor", input: "ARRAY['it''s']", typeName: "text[]", want: "{it's}"},
		{name: "a doubled quote in a literal", input: "'{it''s}'", typeName: "text[]", want: "{it's}"},
		{name: "a backslash and a double quote", input: `ARRAY['a\b"c']`, typeName: "text[]", want: `{"a\\b\"c"}`},
		{name: "escapes in a literal", input: `'{"a\\b\"c"}'`, typeName: "text[]", want: `{"a\\b\"c"}`},
		{name: "a constructor of numbers", input: "ARRAY[1, -2]", typeName: "bigint[]", want: "{1,-2}"},
		{name: "a literal of numbers", input: "'{1,-2}'", typeName: "integer[]", want: "{1,-2}"},
		{name: "an array named with ARRAY", input: "ARRAY[]", typeName: "text ARRAY", want: "{}"},
		{name: "a call is compared as written", input: "ARRAY[lower('A')]", typeName: "text[]", want: "ARRAY[lower('A')]"},
		{name: "a nested literal is compared as written", input: "'{{1,2},{3,4}}'", typeName: "integer[][]", want: "{{1,2},{3,4}}"},
		{name: "an element cast to another type is compared as written", input: "ARRAY['1.5'::integer]", typeName: "numeric[]", want: "ARRAY['1.5'::integer]"},
		{name: "a literal on a column that holds no array", input: "'{}'", typeName: "jsonb", want: "{}"},
		{name: "a constructor on a column that holds no array", input: "ARRAY[]", typeName: "jsonb", want: "ARRAY[]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(normalize.DefaultValue(test.input, test.typeName), qt.Equals, test.want)
		})
	}
}

// TestDefaultValue_ReadsACatalogsGroupingAndCalls pins two CockroachDB forms:
// a negative number in parentheses, and CURRENT_DATE written as a call.
func TestDefaultValue_ReadsACatalogsGroupingAndCalls(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		typeName string
		want     string
	}{
		{name: "a negative integer in parentheses", input: "(-1)", typeName: "bigint", want: "-1"},
		{name: "a negative decimal in parentheses", input: "(-2.5)", typeName: "numeric", want: "-2.5"},
		{name: "a string in parentheses", input: "('x')", typeName: "text", want: "x"},
		{name: "a sum in parentheses stays", input: "(1 + 2)", typeName: "integer", want: "(1 + 2)"},
		{name: "a negation of a group stays", input: "-(2.5)", typeName: "numeric", want: "-(2.5)"},
		{name: "CURRENT_DATE as a call", input: "current_date()", typeName: "date", want: "CURRENT_DATE"},
		{name: "CURRENT_DATE", input: "CURRENT_DATE", typeName: "date", want: "CURRENT_DATE"},
		{name: "CURRENT_DATE on a text column stays", input: "current_date()", typeName: "text", want: "current_date()"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(normalize.DefaultValue(test.input, test.typeName), qt.Equals, test.want)
		})
	}
}
