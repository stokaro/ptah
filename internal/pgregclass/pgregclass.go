// Package pgregclass reads the regclass constants of a PostgreSQL expression:
// the relations, such as sequences, that a stored expression names in a way
// the server records a dependency on.
//
// PostgreSQL writes a stored column default back with each such name as a
// string constant cast to regclass, `nextval('s'::regclass)`, and records a
// dependency of the default on the relation. That dependency is what `DROP
// SEQUENCE s CASCADE` follows when it removes the default. A name passed as
// text, `nextval('s'::text)`, is resolved when the expression runs, carries no
// dependency, and is not a constant here.
package pgregclass

import (
	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// Constants returns the text of each regclass constant in expression, in the
// order they appear: s and app.s for `nextval('s'::regclass) +
// currval('app.s'::regclass)`. The text is the relation name exactly as the
// constant spells it, double quotes included, so `'"Seq"'::regclass` gives
// `"Seq"`. An expression without one, and one the lexer cannot read, gives
// none.
func Constants(expression string) []string {
	opts := dialectlexer.Options(platform.Postgres)
	scanner := lexer.NewLexerWithOptions(expression, opts)
	var tokens []lexer.Token
	for {
		token := scanner.NextToken()
		if token.Type == lexer.TokenEOF {
			break
		}
		if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenComment {
			tokens = append(tokens, token)
		}
	}
	var constants []string
	for i := 0; i+3 < len(tokens); i++ {
		// The lexer reads `::` as two operators, and only the two written
		// together are the cast.
		cast := tokens[i+1].MatchOperatorValue(":") && tokens[i+2].MatchOperatorValue(":") &&
			tokens[i+2].Start == tokens[i+1].End
		if tokens[i].Type != lexer.TokenString || !cast || !tokens[i+3].MatchIdentifierValue("regclass") {
			continue
		}
		if text, ok := lexer.StringValue(tokens[i].Value, opts); ok && text != "" {
			constants = append(constants, text)
		}
	}
	return constants
}
