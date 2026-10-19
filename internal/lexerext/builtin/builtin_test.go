package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/lexerext/builtin"
	"ptah.run/internal/lexerext/yql"
)

// Every lexer a descriptor may name is served, so a dialect read by a lexer
// of its own always gets that lexer's scanner.
func TestLookup_ServesEveryDeclaredLexer(t *testing.T) {
	for _, lexer := range descriptor.Lexers() {
		t.Run(lexer, func(t *testing.T) {
			c := qt.New(t)
			services := builtin.Lookup(lexer)
			c.Assert(services.Lexers, qt.Contains, lexer)
			c.Assert(services.Scanner, qt.IsNotNil)
		})
	}
}

// The YQL lexer is served by the YQL scanner.
func TestLookup_YQL(t *testing.T) {
	c := qt.New(t)
	services := builtin.Lookup(descriptor.LexerYQL)
	c.Assert(services.Lexers, qt.DeepEquals, []string{descriptor.LexerYQL})
	c.Assert(services.Scanner, qt.Equals, any(yql.Scanner{}))
}

// The shared lexer, which a descriptor names with the empty string, and a
// lexer no service serves bring nothing.
func TestLookup_SharedLexer(t *testing.T) {
	for _, lexer := range []string{"", "antlr"} {
		t.Run(lexer, func(t *testing.T) {
			c := qt.New(t)
			services := builtin.Lookup(lexer)
			c.Assert(services.Lexers, qt.IsNil)
			c.Assert(services.Scanner, qt.IsNil)
		})
	}
}
