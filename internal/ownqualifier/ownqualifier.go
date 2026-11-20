// Package ownqualifier finds the schema qualifier a PostgreSQL expression
// writes in front of its own table and a column, as the `public` of
// `public.notes.owner` in a CHECK, an index or a policy on public.notes.
//
// The server reads such a reference as the table the object is on and stores
// it as `owner`. The normalization probes create the object on a copy of the
// table in pg_temp, where the server refuses the qualified reference as an
// invalid reference to a FROM-clause entry, so a probe that sent it as written
// would answer nothing, and the comparison would plan the object again on
// every run. Each probe sends the expression with the qualifier taken out, and
// the policy text comparison drops it too. They share [Qualifiers] because they
// have to agree on the set.
package ownqualifier

import (
	"strings"

	"ptah.run/core/platform/identifier"
	"ptah.run/internal/lexer"
)

// Matcher reports whether token names one part of the expression's own table:
// its schema or its name.
type Matcher func(token lexer.Token) bool

// FoldUnquoted is an unquoted identifier as PostgreSQL stores it: ASCII
// letters in lower case and every other character as written, so `Ärzte`
// stays `Ärzte`. Folding by Unicode would read that as `ärzte`, and a
// subquery exposing the table's name in it would slip past [Qualifiers].
func FoldUnquoted(name string) string {
	return identifier.ComparisonASCIIInsensitive.IdentityKey(name)
}

// StoredName matches a name as the server stores it: an unquoted identifier
// that folds to it, or a quoted one that spells it.
func StoredName(name string) Matcher {
	return func(token lexer.Token) bool {
		switch token.Type {
		case lexer.TokenIdentifier:
			return name != "" && FoldUnquoted(token.Value) == name
		case lexer.TokenString:
			inner, quoted := strings.CutPrefix(token.Value, `"`)
			inner, closed := strings.CutSuffix(inner, `"`)
			return quoted && closed && name != "" && strings.ReplaceAll(inner, `""`, `"`) == name
		}
		return false
	}
}

// Tokens reads expression into its tokens without whitespace and comments,
// each with its byte offsets in expression. Its only caller canonicalizes
// PostgreSQL row-level security clauses, so the text is read with PostgreSQL's
// rules for text with no line comment marker beside "--" declared: a # is an
// operator, not a comment.
func Tokens(expression string) []lexer.Token {
	var tokens []lexer.Token
	scanner := lexer.NewLexerWithOptions(expression, lexer.Options{StandardStrings: true, LineCommentsDeclared: true})
	for token := scanner.NextToken(); token.Type != lexer.TokenEOF; token = scanner.NextToken() {
		if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenComment {
			tokens = append(tokens, token)
		}
	}
	return tokens
}

// Qualifiers returns, in order, the index of each token that is the table's
// schema written in front of the table and a column: schema, a dot, the table,
// and another dot. A qualifier that is itself qualified, as in
// `db.public.notes.owner`, is not one, and neither is one whose column is
// followed by a dot or a parenthesis: `public.notes.c.f` and `public.notes.f(1)`
// are four-part names or calls, and without `public` they would read as another
// schema's.
//
// It returns none when the table's name appears in tokens anywhere else. A
// subquery may expose the same name, as `FROM other.notes` or `AS notes` does,
// and there `S.notes.id` names the outer table while `notes.id` names the inner
// one: taking the qualifier out would change what the expression reads, and a
// probe would answer for a different expression. Any such exposure writes the
// name, so an expression that mentions the table only behind its own schema
// cannot hold one.
func Qualifiers(tokens []lexer.Token, schema, table Matcher) []int {
	var found []int
	behind := make(map[int]bool)
	for i := range tokens {
		if i+3 < len(tokens) && (i == 0 || !isDot(tokens[i-1])) &&
			schema(tokens[i]) && isDot(tokens[i+1]) && table(tokens[i+2]) && isDot(tokens[i+3]) &&
			(i+5 >= len(tokens) || !isDot(tokens[i+5]) && !isOperator(tokens[i+5], "(")) {
			found = append(found, i)
			behind[i+2] = true
		}
	}
	for i, token := range tokens {
		if !behind[i] && table(token) {
			return nil
		}
	}
	return found
}

// Strip is expression with each of its [Qualifiers] taken out, and every
// other byte as written.
func Strip(expression string, schema, table Matcher) string {
	tokens := Tokens(expression)
	found := Qualifiers(tokens, schema, table)
	if len(found) == 0 {
		return expression
	}
	var stripped strings.Builder
	written := 0
	for _, i := range found {
		stripped.WriteString(expression[written:tokens[i].Start])
		written = tokens[i+2].Start
	}
	stripped.WriteString(expression[written:])
	return stripped.String()
}

func isDot(token lexer.Token) bool {
	return isOperator(token, ".")
}

func isOperator(token lexer.Token, value string) bool {
	return token.Type == lexer.TokenOperator && token.Value == value
}
