package routineargs_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/routineargs"
)

// CutDefault finds the clause at the top level of one argument, in either
// spelling, and nowhere else: not inside a string literal, a quoted name or
// the default's own parentheses. PostgreSQL 18.6 prints `=` as DEFAULT.
func TestCutDefault_HappyPath(t *testing.T) {
	tests := []struct {
		name            string
		argument        string
		wantDeclaration string
		wantExpression  string
	}{
		{name: "the keyword", argument: "b text DEFAULT 'X'", wantDeclaration: "b text", wantExpression: "'X'"},
		{name: "the keyword in lower case", argument: "b text default 'X'", wantDeclaration: "b text", wantExpression: "'X'"},
		{name: "the equals sign", argument: "b text = 'X'", wantDeclaration: "b text", wantExpression: "'X'"},
		{name: "a line break before the keyword", argument: "b text\n\tDEFAULT 'X'", wantDeclaration: "b text", wantExpression: "'X'"},
		{name: "an equals sign inside the default", argument: "b boolean DEFAULT (1 = 1)", wantDeclaration: "b boolean", wantExpression: "(1 = 1)"},
		{name: "the keyword inside a literal", argument: "b text = 'x DEFAULT y'", wantDeclaration: "b text", wantExpression: "'x DEFAULT y'"},
		{name: "an equals sign in a quoted name", argument: `"a=b" text DEFAULT 'x'`, wantDeclaration: `"a=b" text`, wantExpression: "'x'"},
		{name: "a type modifier", argument: "c numeric(10,2) = 1.5", wantDeclaration: "c numeric(10,2)", wantExpression: "1.5"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			declaration, expression, found := routineargs.CutDefault(test.argument)

			c.Assert(found, qt.IsTrue)
			c.Assert(declaration, qt.Equals, test.wantDeclaration)
			c.Assert(expression, qt.Equals, test.wantExpression)
		})
	}
}

// An argument without a default is returned whole. A word that only contains
// the keyword is not the keyword.
func TestCutDefault_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		argument string
	}{
		{name: "no default", argument: "a integer"},
		{name: "a name starting with the keyword", argument: "defaults integer"},
		{name: "a name ending in the keyword", argument: "nodefault integer"},
		{name: "a quoted name that is the keyword", argument: `"default" integer`},
		{name: "a type modifier", argument: "a numeric(10,2)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			declaration, expression, found := routineargs.CutDefault(test.argument)

			c.Assert(found, qt.IsFalse)
			c.Assert(declaration, qt.Equals, test.argument)
			c.Assert(expression, qt.Equals, "")
		})
	}
}

// A comma inside a default is not one between arguments: in a string literal,
// in an array's brackets, in a call's parentheses, or in a quoted name.
func TestSplit_KeepsADefaultWhole(t *testing.T) {
	tests := []struct {
		name      string
		arguments string
		want      []string
	}{
		{name: "a literal", arguments: "a text DEFAULT 'a,b', c int", want: []string{"a text DEFAULT 'a,b'", "c int"}},
		{name: "an array", arguments: "d int[] DEFAULT ARRAY[1, 2], e int", want: []string{"d int[] DEFAULT ARRAY[1, 2]", "e int"}},
		{name: "a call", arguments: "f text DEFAULT concat('a', 'b'), g int", want: []string{"f text DEFAULT concat('a', 'b')", "g int"}},
		{name: "a quoted name", arguments: `"a,b" int, c int`, want: []string{`"a,b" int`, "c int"}},
		{name: "a type modifier", arguments: "a numeric(10,2), b int", want: []string{"a numeric(10,2)", "b int"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(routineargs.Split(test.arguments), qt.DeepEquals, test.want)
		})
	}
}
