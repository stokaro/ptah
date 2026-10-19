// Package reachext declares what a lexer of an engine's own brings to the
// scan of internal/sqlreach: the constructs of the text it reads that reach
// outside the database, and whether its strings take backslash escapes.
// internal/sqlreach asks for them by the lexer a dialect's descriptor names
// (core/platform/descriptor.Lexical.Lexer), so it names no engine and no
// lexer, and internal/reachext/builtin lists the services of the lexers Ptah
// ships.
//
// A construct whose shape the shared SQL lexer's tokens already show is
// descriptor data instead (core/platform/descriptor.Reach). A service holds
// the rules of a grammar the descriptor's shapes do not describe.
package reachext

import (
	"strings"

	"ptah.run/internal/lexer"
)

// Services is what one lexer brings to a reach scan. The zero value brings
// nothing, which is also what a dialect the shared lexer reads gets.
type Services struct {
	// Lexers are the descriptor.Lexical.Lexer names these services serve.
	Lexers []string
	// BackslashEscapes reports that the lexer reads a backslash as an escape
	// in every string, so a scan's default reading of the text is the
	// backslash one.
	BackslashEscapes bool
	// Rules are the constructs of the lexer's grammar that reach outside the
	// database, asked in order after every construct the descriptors state.
	Rules []Rule
}

// Rule is one construct a lexer's grammar has that reaches outside the
// database.
type Rule struct {
	// Construct names the construct in a refusal.
	Construct string
	// Reach says what the construct can touch, as a refusal prints it after
	// the construct.
	Reach string
	// Match reports whether a statement uses the construct. tokens are the
	// statement's tokens as the lexer reads them, without whitespace and
	// comments; a translation setting the lexer emits as an unknown token is
	// kept. Match only reads the slice.
	Match func(tokens []lexer.Token) bool
}

// CallableName returns the upper-cased name a token gives a callable,
// unwrapping its quoted spellings: double-quoted names arrive from the shared
// lexer as strings, and backticked and bracketed ones keep their quotes as
// identifiers. A token that cannot name a callable, or names an empty one,
// answers false.
func CallableName(token lexer.Token) (string, bool) {
	value := token.Value
	switch token.Type {
	case lexer.TokenIdentifier:
		value = strings.Trim(value, "`[]")
	case lexer.TokenString:
		if !strings.HasPrefix(value, `"`) {
			return "", false
		}
		value = strings.Trim(value, `"`)
	default:
		return "", false
	}
	if value == "" {
		return "", false
	}
	return strings.ToUpper(value), true
}
