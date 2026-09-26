package pgname

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// tokenList is an expression PostgreSQL's lexer has read, whitespace and
// comments left out. Its methods answer what one position holds, and false or
// the position unchanged past the end.
type tokenList []lexer.Token

// significantTokens reads expression the way PostgreSQL's lexer does and keeps
// every token but whitespace and comments.
func significantTokens(expression string) tokenList {
	scanner := lexer.NewLexerWithOptions(expression, dialectlexer.Options(platform.Postgres))
	var tokens tokenList
	for {
		token := scanner.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return tokens
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		default:
			tokens = append(tokens, token)
		}
	}
}

// skipQualifiedName returns the position after the dotted name that starts at
// position.
func (t tokenList) skipQualifiedName(position int) int {
	if _, ok := t.nameAt(position); !ok {
		return position
	}
	position++
	for t.operatorAt(position, ".") {
		if _, ok := t.nameAt(position + 1); !ok {
			return position
		}
		position += 2
	}
	return position
}

// skipBalanced returns the position after the closer that balances the opener
// at position.
func (t tokenList) skipBalanced(position int, opener, closer string) int {
	depth := 0
	for ; position < len(t); position++ {
		switch {
		case t.operatorAt(position, opener):
			depth++
		case t.operatorAt(position, closer):
			depth--
			if depth == 0 {
				return position + 1
			}
		}
	}
	return position
}

func (t tokenList) operatorAt(position int, value string) bool {
	return position >= 0 && position < len(t) && t[position].MatchOperatorValue(value)
}

func (t tokenList) stringLiteralAt(position int) bool {
	if position >= len(t) {
		return false
	}
	token := t[position]
	return token.Type == lexer.TokenString && !strings.HasPrefix(token.Value, `"`)
}

// keywordAt reports whether the token at position is word written unquoted.
func (t tokenList) keywordAt(position int, word string) bool {
	return position < len(t) && t[position].Type == lexer.TokenIdentifier &&
		foldUnquoted(t[position].Value) == word
}

func (t tokenList) keywordIn(position int, words []string) bool {
	for _, word := range words {
		if t.keywordAt(position, word) {
			return true
		}
	}
	return false
}

// nameAt answers the name the token at position spells, as the server stores
// it: a quoted identifier keeps its case, an unquoted one is folded. A number
// is not a name.
func (t tokenList) nameAt(position int) (string, bool) {
	if position >= len(t) {
		return "", false
	}
	token := t[position]
	switch {
	case token.Type == lexer.TokenString && strings.HasPrefix(token.Value, `"`):
		return unquoteIdentifier(token.Value), true
	case token.Type == lexer.TokenIdentifier && token.Value != "" && !isDigit(token.Value[0]):
		return foldUnquoted(token.Value), true
	default:
		return "", false
	}
}

// foldUnquoted folds an unquoted identifier as PostgreSQL does: ASCII letters
// to lower case, every other character kept.
func foldUnquoted(value string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return r
	}, value)
}

// unquoteIdentifier removes the double quotes around an identifier and undoes
// the doubling of a quote inside it.
func unquoteIdentifier(value string) string {
	value = strings.TrimPrefix(value, `"`)
	value = strings.TrimSuffix(value, `"`)
	return strings.ReplaceAll(value, `""`, `"`)
}

func isDigit(character byte) bool {
	return character >= '0' && character <= '9'
}
