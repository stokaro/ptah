package chindexkey_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/chindexkey"
)

// TestSame_ReadsClickHouseFormattingAsOneKey holds a declared key against the
// one ClickHouse 26.9.14 reports for it in system.data_skipping_indices.
func TestSame_ReadsClickHouseFormattingAsOneKey(t *testing.T) {
	tests := []struct {
		name     string
		declared []string
		reported []string
	}{
		{name: "a column", declared: []string{"s"}, reported: []string{"s"}},
		{name: "two fields against the reported list", declared: []string{"a", "b"}, reported: []string{"a, b"}},
		{name: "a written tuple", declared: []string{"(a,b)"}, reported: []string{"a, b"}},
		{name: "a tuple() call", declared: []string{"tuple(a, b)"}, reported: []string{"a, b"}},
		{name: "a function name in upper case", declared: []string{"LOWER(a)"}, reported: []string{"lower(a)"}},
		{name: "grouping the server adds", declared: []string{"b * 2 + 1"}, reported: []string{"(b * 2) + 1"}},
		{name: "spacing around an operator", declared: []string{"cityHash64(a)%10"}, reported: []string{"cityHash64(a) % 10"}},
		{name: "parentheses the server keeps", declared: []string{"(b)"}, reported: []string{"(b)"}},
		{name: "a string literal", declared: []string{"concat(a, 'X Y')"}, reported: []string{"concat(a, 'X Y')"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(chindexkey.Same(test.declared, test.reported), qt.IsTrue)
		})
	}
}

// TestSame_SeesAChangedKey is the other half: a key that indexes something
// else is a different key, whatever the formatting.
func TestSame_SeesAChangedKey(t *testing.T) {
	tests := []struct {
		name     string
		declared []string
		reported []string
	}{
		{name: "another column", declared: []string{"b"}, reported: []string{"s"}},
		{name: "a column against an expression of it", declared: []string{"lower(s)"}, reported: []string{"s"}},
		{name: "another function", declared: []string{"upper(a)"}, reported: []string{"lower(a)"}},
		{name: "the keys in another order", declared: []string{"b", "a"}, reported: []string{"a, b"}},
		{name: "a key fewer", declared: []string{"a"}, reported: []string{"a, b"}},
		{name: "another constant", declared: []string{"b * 3 + 1"}, reported: []string{"(b * 2) + 1"}},
		{name: "another string literal", declared: []string{"concat(a, 'x y')"}, reported: []string{"concat(a, 'X Y')"}},
		{name: "a call against a bare name", declared: []string{"f(a)"}, reported: []string{"f a"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(chindexkey.Same(test.declared, test.reported), qt.IsFalse)
		})
	}
}

// TestExpression joins key parts into the expression ADD INDEX takes.
func TestExpression(t *testing.T) {
	tests := []struct {
		name  string
		parts []string
		want  string
	}{
		{name: "no parts", parts: nil, want: ""},
		{name: "one column", parts: []string{"s"}, want: "s"},
		{name: "one call", parts: []string{"lower(s)"}, want: "lower(s)"},
		{name: "several parts", parts: []string{"a", "b"}, want: "(a, b)"},
		{name: "a reported list", parts: []string{"a, b"}, want: "(a, b)"},
		{name: "a comma inside a call", parts: []string{"f(a, b)"}, want: "f(a, b)"},
		{name: "a comma inside a literal", parts: []string{"concat(a, ',')"}, want: "concat(a, ',')"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(chindexkey.Expression(test.parts), qt.Equals, test.want)
		})
	}
}
