package pgtableprobe

import (
	"errors"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// errEvaluatedBound is why a bound that is not built from constants is not
// probed. PostgreSQL evaluates the expressions of a bound when it creates the
// partition, so a function in one runs in the probe: measured on PostgreSQL
// 18, `FOR VALUES IN (nextval('s') + length(dblink_exec(...)) - 1)` advanced
// the sequence and created a table on another connection, which the probe's
// rollback cannot take back. An operator is a function too, and a user can
// define one for any types.
var errEvaluatedBound = errors.New("its bound is not built from constants alone, " +
	"and the server evaluates a bound when it creates the partition")

// boundHeadWords are the words of a bound outside its value lists.
var boundHeadWords = []string{"FOR", "VALUES", "IN", "FROM", "TO", "WITH", "DEFAULT"}

// boundValueWords are the words that stand for a constant inside a value
// list, or that name one in the hash form.
var boundValueWords = []string{"MINVALUE", "MAXVALUE", "MODULUS", "REMAINDER", "NULL", "TRUE", "FALSE"}

// stringPrefixes are the letters that, written against a string, make it a
// bit string or a national character string.
var stringPrefixes = []string{"B", "X", "N"}

// constantBound reports whether bound, a bound [pgtable.ParsePartitionBound]
// returned, is built from constants alone, so creating a partition with it
// runs no function.
//
// A value is a string in any of its spellings, a number, a minus sign that
// opens a value before a number, which the server folds into the constant, or
// one of [boundValueWords], and it may be cast with `::` to a type named by one
// word, with integer modifiers or array brackets. Anything else, a function
// call, an operator other than that minus sign, a name or a subquery, is not a
// constant. Both directions are exact for the forms listed: a bound in another
// spelling of a constant, such as `1e3` or `timestamp '2026-01-01'`, is not
// probed and is compared as written.
//
// bound is one the probe text check accepted, so its parentheses balance.
func constantBound(bound string) bool {
	tokens := boundTokens(bound)
	depth := 0
	for i := 0; i < len(tokens); i++ {
		token := tokens[i]
		switch {
		case token.Type == lexer.TokenString, isNumber(token):
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")"):
			depth--
		case token.MatchOperatorValue(","):
		case token.MatchOperatorValue("-") && opensValue(tokens, i) && i+1 < len(tokens) && isNumber(tokens[i+1]):
			i++
		case depth == 0 && matchesWord(token, boundHeadWords):
		case depth > 0 && matchesWord(token, boundValueWords):
		case matchesWord(token, stringPrefixes) && i+1 < len(tokens) &&
			tokens[i+1].Type == lexer.TokenString && tokens[i+1].Start == token.End:
			i++
		case isCast(tokens, i):
			end, ok := castEnd(tokens, i+2)
			if !ok {
				return false
			}
			i = end - 1
		default:
			return false
		}
	}
	return true
}

// opensValue reports whether the token at i starts a value of a list: it
// follows the parenthesis that opens the list or the comma before it. A minus
// sign anywhere else subtracts, which is an operator the server evaluates.
func opensValue(tokens []lexer.Token, i int) bool {
	return i > 0 && (tokens[i-1].MatchOperatorValue("(") || tokens[i-1].MatchOperatorValue(","))
}

// boundTokens is bound as the PostgreSQL lexer reads it, without white space
// and comments.
func boundTokens(bound string) []lexer.Token {
	scan := lexer.NewLexerWithOptions(bound, dialectlexer.Options(platform.Postgres))
	var tokens []lexer.Token
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenComment {
			tokens = append(tokens, token)
		}
	}
	return tokens
}

// isNumber reports whether token is an unsigned decimal number: digits, with
// at most one point.
func isNumber(token lexer.Token) bool {
	if token.Type != lexer.TokenIdentifier || token.Value == "" || token.Value == "." {
		return false
	}
	points := 0
	for _, ch := range token.Value {
		switch {
		case ch == '.':
			points++
		case ch < '0' || ch > '9':
			return false
		}
	}
	return points <= 1
}

// matchesWord reports whether token is one of words, in any case.
func matchesWord(token lexer.Token, words []string) bool {
	return slices.ContainsFunc(words, token.MatchIdentifierValue)
}

// isCast reports whether the tokens at i are `::`.
func isCast(tokens []lexer.Token, i int) bool {
	return i+1 < len(tokens) && tokens[i].MatchOperatorValue(":") &&
		tokens[i+1].MatchOperatorValue(":") && tokens[i+1].Start == tokens[i].End
}

// castEnd reads the type of a cast from tokens[start]: one word, then integer
// modifiers in parentheses and array brackets. It returns the position after
// the type, and false when what follows `::` is not such a type.
func castEnd(tokens []lexer.Token, start int) (int, bool) {
	if start >= len(tokens) || !isTypeWord(tokens[start]) {
		return 0, false
	}
	i := start + 1
	if i < len(tokens) && tokens[i].MatchOperatorValue("(") {
		i++
		for i < len(tokens) && (isNumber(tokens[i]) || tokens[i].MatchOperatorValue(",")) {
			i++
		}
		if i >= len(tokens) || !tokens[i].MatchOperatorValue(")") {
			return 0, false
		}
		i++
	}
	for i+1 < len(tokens) && tokens[i].MatchOperatorValue("[") && tokens[i+1].MatchOperatorValue("]") {
		i += 2
	}
	return i, true
}

// isTypeWord reports whether token can name a type on its own: a name, quoted
// or not, that is not a number.
func isTypeWord(token lexer.Token) bool {
	if token.Type != lexer.TokenIdentifier || token.Value == "" || isNumber(token) {
		return false
	}
	return !strings.ContainsAny(token.Value[:1], "0123456789$")
}
