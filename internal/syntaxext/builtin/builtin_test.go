package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/syntaxext/builtin"
)

// Every lexer a descriptor may name has a parser, so a document of a dialect
// read by a lexer of its own is parsed in that lexer's grammar.
func TestLookup_ServesEveryDeclaredLexer(t *testing.T) {
	for _, lexer := range descriptor.Lexers() {
		t.Run(lexer, func(t *testing.T) {
			c := qt.New(t)
			services := builtin.Lookup(lexer)
			c.Assert(services.Lexers, qt.DeepEquals, []string{lexer})
			c.Assert(services.Parser, qt.IsNotNil)
		})
	}
}

// The shared lexer, which a descriptor names with the empty string, and a
// lexer no service serves have no parser of their own.
func TestLookup_SharedLexer(t *testing.T) {
	for _, lexer := range []string{"", "antlr"} {
		t.Run(lexer, func(t *testing.T) {
			c := qt.New(t)
			services := builtin.Lookup(lexer)
			c.Assert(services.Lexers, qt.IsNil)
			c.Assert(services.Parser, qt.IsNil)
		})
	}
}
