package compare

import (
	"slices"
	"strings"

	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// foldViewJoins reads the joins of a normalized view body in the one spelling
// a server that rewrites them stores, where dialect's descriptor states
// [descriptor.Comparison.ViewJoinsParenthesized]; any other dialect gets body
// unchanged.
//
// MySQL 26.10 stores `SELECT t.id FROM t INNER JOIN u AS a ON a.id = t.id`
// as select `t`.`id` AS `id` from (`t` join `u` `a` on((`a`.`id` = `t`.`id`))),
// and MariaDB 12.3 writes on(...) with one pair of parentheses. The fold is
// applied to the declared and the stored body alike, and each step keeps the
// query the body asks:
//
//   - INNER and CROSS are left out of a join, and OUTER out of LEFT and RIGHT
//     OUTER JOIN;
//   - AS is left out between a relation and its alias;
//   - the parentheses around a whole ON condition are left out, so `on((c))`,
//     `on(c)` and `on c` read alike, while `ON (a OR b) AND c` keeps its group;
//   - the parentheses a FROM clause starts with are left out where they hold a
//     join, since joins group from the left: `from ((t join u on c) join w on
//     d)` is `from t join u on c join w on d`. A join nested on the right, `t
//     join (u join w on c) on d`, keeps its parentheses, and so does a
//     subquery.
//
// Without the fold a declared join never reads as the stored one, and every
// plan replaces a view nobody changed (stokaro/ptah#6757). The server also
// writes `t RIGHT JOIN u` as u left join t, USING as an ON condition and
// `FROM t, u` as from t join u; the fold does not read those, so such a
// declaration still compares as another body.
func foldViewJoins(body, dialect string) string {
	if !comparisonOf(dialect).ViewJoinsParenthesized {
		return body
	}
	for range len(body) {
		folded, changed := foldOneJoin(body, viewTokens(body, dialect))
		if !changed {
			break
		}
		body = folded
	}
	return body
}

// viewTokens is body's tokens without white space and comments, read as
// dialect's lexer reads them.
func viewTokens(body, dialect string) []lexer.Token {
	var tokens []lexer.Token
	l := lexer.NewLexerWithOptions(body, dialectlexer.Options(dialect))
	for token := l.NextToken(); token.Type != lexer.TokenEOF; token = l.NextToken() {
		switch token.Type {
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		}
		tokens = append(tokens, token)
	}
	return tokens
}

// foldOneJoin applies the first step of [foldViewJoins] that reaches a token
// of body, and reports whether one did.
func foldOneJoin(body string, tokens []lexer.Token) (string, bool) {
	for i := range tokens {
		switch {
		case isWord(tokens, i, "inner", "cross") && isWord(tokens, i+1, "join"),
			isWord(tokens, i, "outer") && isWord(tokens, i-1, "left", "right") && isWord(tokens, i+1, "join"):
			return body[:tokens[i].Start] + body[tokens[i+1].Start:], true
		case isWord(tokens, i, "as") && relationAlias(tokens, i):
			return body[:tokens[i].Start] + body[tokens[i+1].Start:], true
		case isWord(tokens, i, "on") && isOperator(tokens, i+1, "("):
			closing := matchingParenthesis(tokens, i+1)
			if closing < 0 {
				return body, false
			}
			if endsCondition(tokens, closing+1) {
				return withoutParentheses(body, tokens[i+1], tokens[closing]), true
			}
			if tokens[i+1].Start == tokens[i].End {
				return body[:tokens[i].End] + " " + body[tokens[i].End:], true
			}
		case isWord(tokens, i, "from") && isOperator(tokens, i+1, "("):
			closing := matchingParenthesis(tokens, i+1)
			if closing > 0 && !isWord(tokens, i+2, "select", "with", "values", "table") && holdsJoin(tokens, i+1, closing) {
				return withoutParentheses(body, tokens[i+1], tokens[closing]), true
			}
		}
	}
	return body, false
}

// relationAlias reports whether the AS at index stands between a relation
// named right after FROM, JOIN or a parenthesis that opens a join, and its
// alias.
func relationAlias(tokens []lexer.Token, index int) bool {
	if !isName(tokens, index+1) {
		return false
	}
	start := index - 1
	if !isName(tokens, start) {
		return false
	}
	for isOperator(tokens, start-1, ".") && isName(tokens, start-2) {
		start -= 2
	}
	return isWord(tokens, start-1, "from", "join") || isOperator(tokens, start-1, "(") && isWord(tokens, start-2, "from", "join")
}

// endsCondition reports whether the token at index ends an ON condition: the
// end of the body, a parenthesis that closes, a comma, or a word that starts
// another join or another clause.
func endsCondition(tokens []lexer.Token, index int) bool {
	return index >= len(tokens) || isOperator(tokens, index, ")") || isOperator(tokens, index, ",") ||
		tokens[index].Type == lexer.TokenSemicolon ||
		isWord(tokens, index, "join", "left", "right", "inner", "cross", "natural", "straight_join",
			"where", "group", "order", "having", "limit", "union", "except", "intersect", "window", "for", "lock", "into")
}

// holdsJoin reports whether the parentheses at open and closing hold a join
// at their own level, rather than inside a group they hold.
func holdsJoin(tokens []lexer.Token, open, closing int) bool {
	depth := 0
	for i := open + 1; i < closing; i++ {
		switch {
		case isOperator(tokens, i, "("):
			depth++
		case isOperator(tokens, i, ")"):
			depth--
		case depth == 0 && isWord(tokens, i, "join", "straight_join"):
			return true
		}
	}
	return false
}

// matchingParenthesis is the index of the parenthesis that closes the one at
// open, or -1 where none does.
func matchingParenthesis(tokens []lexer.Token, open int) int {
	depth := 0
	for i := open; i < len(tokens); i++ {
		switch {
		case isOperator(tokens, i, "("):
			depth++
		case isOperator(tokens, i, ")"):
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// withoutParentheses is body without the parentheses open and closing, each
// replaced by a space where it stood between two words and left out
// otherwise.
func withoutParentheses(body string, open, closing lexer.Token) string {
	body = withoutToken(body, closing)
	return withoutToken(body, open)
}

// withoutToken is body without token, which a space replaces where the text
// on both sides of it would otherwise join into one word.
func withoutToken(body string, token lexer.Token) string {
	separator := ""
	if token.Start > 0 && token.End < len(body) && isWordByte(body[token.Start-1]) && isWordByte(body[token.End]) {
		separator = " "
	}
	return body[:token.Start] + separator + body[token.End:]
}

// isWordByte reports whether b can end or start a word, a quoted name or a
// string.
func isWordByte(b byte) bool {
	return b == '_' || b == '`' || b == '"' || b == '\'' || b >= '0' && b <= '9' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= 0x80
}

// isWord reports whether the token at index is an unquoted word among words,
// compared in either case.
func isWord(tokens []lexer.Token, index int, words ...string) bool {
	if index < 0 || index >= len(tokens) || tokens[index].Type != lexer.TokenIdentifier {
		return false
	}
	return slices.ContainsFunc(words, func(word string) bool { return strings.EqualFold(tokens[index].Value, word) })
}

// isName reports whether the token at index names something: a word or a
// quoted name.
func isName(tokens []lexer.Token, index int) bool {
	return index >= 0 && index < len(tokens) && tokens[index].Type == lexer.TokenIdentifier &&
		!isWord(tokens, index, "select", "from", "join", "on", "where", "as", "using", "left", "right", "inner", "cross")
}

// isOperator reports whether the token at index is the operator value.
func isOperator(tokens []lexer.Token, index int, value string) bool {
	return index >= 0 && index < len(tokens) && tokens[index].MatchOperatorValue(value)
}
