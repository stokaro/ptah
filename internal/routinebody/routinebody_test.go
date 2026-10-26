package routinebody_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/routinebody"
)

// TestOf_TellsASQLStandardBodyFromAString pins which bodies are SQL-standard:
// a LANGUAGE sql routine's that opens with BEGIN ATOMIC or RETURN as a
// keyword, and the BEGIN ATOMIC body of a routine that names no language.
func TestOf_TellsASQLStandardBodyFromAString(t *testing.T) {
	tests := []struct {
		name     string
		language string
		body     string
		want     routinebody.Kind
	}{
		{name: "an atomic body", language: "sql", body: "BEGIN ATOMIC SELECT 1; END", want: routinebody.Atomic},
		{name: "an atomic body as the server prints it", language: "sql", body: "BEGIN ATOMIC\n SELECT 1;\nEND", want: routinebody.Atomic},
		{name: "an atomic body in lower case", language: "SQL", body: "  begin  atomic select 1; end", want: routinebody.Atomic},
		{name: "a return body", language: "sql", body: "RETURN a + b", want: routinebody.Return},
		{name: "a return body that opens a parenthesis", language: "sql", body: "RETURN(1)", want: routinebody.Return},
		{name: "a quoted body", language: "sql", body: "SELECT 1", want: routinebody.Quoted},
		{name: "a bare BEGIN", language: "sql", body: "BEGIN SELECT 1; END", want: routinebody.Quoted},
		{name: "a word that starts with RETURN", language: "sql", body: "RETURNING_ROWS()", want: routinebody.Quoted},
		{name: "a PL/pgSQL body that starts with RETURN", language: "plpgsql", body: "RETURN 1", want: routinebody.Quoted},
		{name: "a PL/pgSQL block", language: "plpgsql", body: "BEGIN ATOMIC SELECT 1; END", want: routinebody.Quoted},
		{name: "an atomic body of a routine that names no language", language: "", body: "BEGIN ATOMIC SELECT 1; END", want: routinebody.Atomic},
		{name: "a return body of a routine that names no language", language: "", body: "RETURN 1", want: routinebody.Quoted},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := routinebody.Of(test.language, test.body)

			c.Assert(got, qt.Equals, test.want)
			c.Assert(routinebody.SQLStandard(test.language, test.body), qt.Equals, test.want != routinebody.Quoted)
		})
	}
}

// TestReturnExpression_DropsTheKeyword pins the expression a RETURN body
// writes after its keyword, and that any other body is kept whole.
func TestReturnExpression_DropsTheKeyword(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "a return body", body: "RETURN a + b", want: "a + b"},
		{name: "a return body with a parenthesis", body: "RETURN(SELECT 1)", want: "(SELECT 1)"},
		{name: "an atomic body", body: "BEGIN ATOMIC SELECT 1; END", want: "BEGIN ATOMIC SELECT 1; END"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := routinebody.ReturnExpression(test.body)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}
