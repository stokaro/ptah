package parser

// White-box testing required: these predicates classify raw input at token
// positions before any statement is parsed; their edge cases (comment
// trailers, repeat counts, expression tails) are not distinguishable through
// the exported Parse API.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestIsSQLServerGoBatchSeparatorAt(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "go alone on its line",
			input:    "SELECT 1\nGO\nSELECT 2",
			expected: true,
		},
		{
			name:     "go at end of input",
			input:    "SELECT 1\nGO",
			expected: true,
		},
		{
			name:     "indented go",
			input:    "SELECT 1\n\t  GO\n",
			expected: true,
		},
		{
			name:     "go with repeat count",
			input:    "SELECT 1\nGO 3\n",
			expected: true,
		},
		{
			name:     "go zero",
			input:    "SELECT 1\nGO 0\n",
			expected: true,
		},
		{
			name:     "go with line comment trailer",
			input:    "SELECT 1\nGO -- next batch\n",
			expected: true,
		},
		{
			name:     "go with block comment then count",
			input:    "SELECT 1\nGO /* twice */ 2\n",
			expected: true,
		},
		{
			name:     "unterminated block comment trailer",
			input:    "SELECT 1\nGO /* next batch\n",
			expected: false,
		},
		{
			name:     "identifier before go on the line",
			input:    "SELECT 1 AS GO\n",
			expected: false,
		},
		{
			name:     "identifier after go on the line",
			input:    "SELECT 1\nGO TO\n",
			expected: false,
		},
		{
			name:     "second count is not a trailer",
			input:    "SELECT 1\nGO 2 2\n",
			expected: false,
		},
		{
			name:     "count too large for int",
			input:    "SELECT 1\nGO 99999999999999999999\n",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			start := strings.Index(tt.input, "GO")
			c.Assert(start >= 0, qt.IsTrue)

			c.Assert(isSQLServerGoBatchSeparatorAt(tt.input, start, start+len("GO")), qt.Equals, tt.expected)
		})
	}
}

func TestIsScalarIFExpressionFragment(t *testing.T) {
	tests := []struct {
		name     string
		fragment string
		expected bool
	}{
		{
			name:     "scalar function call",
			fragment: "(seen = 1, 'yes', 'no') AS result",
			expected: true,
		},
		{
			name:     "procedural parenthesized condition",
			fragment: "(seen = 0) THEN",
			expected: false,
		},
		{
			name:     "procedural condition with trivia",
			fragment: " /* leading */ (seen = 0) /* trailing */ THEN",
			expected: false,
		},
		{
			name:     "nested scalar expression",
			fragment: "(IF(enabled, 1, 0), 'yes', 'no')",
			expected: true,
		},
		{
			name:     "unterminated call stays scalar",
			fragment: "(",
			expected: true,
		},
		{
			name:     "not an IF call tail",
			fragment: " condition THEN",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(isScalarIFExpressionFragment(tt.fragment), qt.Equals, tt.expected)
		})
	}
}
