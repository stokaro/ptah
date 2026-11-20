package parser

// White-box testing required: these predicates classify raw input at token
// positions before any statement is parsed; their edge cases (expression
// tails) are not distinguishable through the exported Parse API.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lexer"
	"ptah.run/internal/routineext/compound"
)

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

			c.Assert(compound.ScalarIF(tt.fragment, lexer.Options{}), qt.Equals, tt.expected)
		})
	}
}

// A # banner after the condition is a comment on MySQL, so the THEN behind it
// makes the IF a block, and the same text with the # as an operator is not
// affected: the fragment is read with the options of the dialect.
func TestScalarIF_HashComment(t *testing.T) {
	fragment := "(x > 1) #=== banner\n THEN SET y = 1; END IF;"

	t.Run("mysql reads a comment", func(t *testing.T) {
		c := qt.New(t)

		c.Assert(compound.ScalarIF(fragment, lexerOptions("mysql")), qt.IsFalse)
	})
}
