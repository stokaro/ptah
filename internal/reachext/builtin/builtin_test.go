package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/reachext/builtin"
)

// Every lexer a descriptor may name is served, so a dialect read by a lexer
// of its own always gets that lexer's rules. The YQL lexer's are the rules
// internal/sqlreach asked on a dialect whose text is YQL, in their order.
func TestLookup_ServesEveryDeclaredLexer(t *testing.T) {
	c := qt.New(t)
	c.Assert(descriptor.Lexers(), qt.DeepEquals, []string{descriptor.LexerYQL})
	services := builtin.Lookup(descriptor.LexerYQL)
	c.Assert(services.Lexers, qt.DeepEquals, []string{descriptor.LexerYQL})
	c.Assert(services.BackslashEscapes, qt.IsTrue)
	var constructs []string
	for _, rule := range services.Rules {
		constructs = append(constructs, rule.Construct)
	}
	c.Assert(constructs, qt.DeepEquals, []string{
		"YQL translation setting", "PRAGMA", "YQL UDF call", "YQL file or secret function",
		"YQL code or UDF function", "YQL PostgreSQL function", "YQL external source",
	})
}

// The shared lexer, which a descriptor names with the empty string, and a
// lexer no service serves bring nothing.
func TestLookup_SharedLexer(t *testing.T) {
	for _, lexer := range []string{"", "antlr"} {
		t.Run(lexer, func(t *testing.T) {
			c := qt.New(t)
			services := builtin.Lookup(lexer)
			c.Assert(services.Lexers, qt.IsNil)
			c.Assert(services.BackslashEscapes, qt.IsFalse)
			c.Assert(services.Rules, qt.IsNil)
		})
	}
}
