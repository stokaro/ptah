package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/syntaxext/builtin"
)

// The shared lexer, which a descriptor names with the empty string, a lexer
// no service serves, and YQL, which its engine's provider reads, so no
// built-in service serves it, have no parser of their own.
func TestLookup_SharedLexer(t *testing.T) {
	for _, lexer := range []string{"", "antlr", descriptor.LexerYQL} {
		t.Run(lexer, func(t *testing.T) {
			c := qt.New(t)
			services := builtin.Lookup(lexer)
			c.Assert(services.Lexers, qt.IsNil)
			c.Assert(services.Parser, qt.IsNil)
		})
	}
}
