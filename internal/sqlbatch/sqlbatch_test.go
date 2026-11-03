package sqlbatch_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlbatch"
)

// TestSeparatorAt_HappyPath reads a GO that stands alone on its line, with
// the count written after it, or 1.
func TestSeparatorAt_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int
	}{
		{name: "alone on its line", input: "SELECT 1\nGO\nSELECT 2", want: 1},
		{name: "at the end of the input", input: "SELECT 1\nGO", want: 1},
		{name: "indented", input: "SELECT 1\n\t  GO\n", want: 1},
		{name: "after a carriage return", input: "SELECT 1\r\nGO\r\n", want: 1},
		{name: "with a repeat count", input: "SELECT 1\nGO 3\n", want: 3},
		{name: "zero", input: "SELECT 1\nGO 0\n", want: 0},
		{name: "with a line comment", input: "SELECT 1\nGO -- next batch\n", want: 1},
		{name: "with a block comment, then a count", input: "SELECT 1\nGO /* twice */ 2\n", want: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			start := strings.Index(test.input, "GO")

			count, ok := sqlbatch.SeparatorAt(test.input, start, start+len("GO"))

			c.Assert(ok, qt.IsTrue)
			c.Assert(count, qt.Equals, test.want)
		})
	}
}

// TestSeparatorAt_FailurePath leaves a GO that shares its line with anything
// but a count and comments as an ordinary word.
func TestSeparatorAt_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "a word before it on the line", input: "SELECT 1 AS GO\n"},
		{name: "a word after it on the line", input: "SELECT 1\nGO TO\n"},
		{name: "a second count", input: "SELECT 1\nGO 2 2\n"},
		{name: "a count too large for an int", input: "SELECT 1\nGO 99999999999999999999\n"},
		{name: "a block comment never closed", input: "SELECT 1\nGO /* next batch\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			start := strings.Index(test.input, "GO")

			count, ok := sqlbatch.SeparatorAt(test.input, start, start+len("GO"))

			c.Assert(ok, qt.IsFalse)
			c.Assert(count, qt.Equals, 0)
		})
	}
}
