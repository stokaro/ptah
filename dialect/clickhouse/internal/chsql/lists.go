package chsql

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// listItem is one top-level element of a comma-separated clause: its text as
// written and its significant tokens.
type listItem struct {
	text   string
	tokens []string
}

// significant lexes text as ClickHouse SQL without whitespace and comments.
func significant(text string) []lexer.Token {
	lex := lexer.NewLexerWithOptions(text, dialectlexer.Options(platform.ClickHouse))
	var tokens []lexer.Token
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return tokens
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		}
		tokens = append(tokens, token)
	}
}

// splitList splits tokens of text at commas outside parentheses, brackets and
// braces; a comma inside a string literal is part of the literal's token. Each
// item keeps the text between its first and last token. An empty item, as in
// `a,,b`, has no tokens, which a caller reads as a malformed list.
func splitList(text string, tokens []lexer.Token) []listItem {
	if len(tokens) == 0 {
		return nil
	}
	var items []listItem
	var current []lexer.Token
	closeItem := func() {
		item := listItem{}
		if len(current) > 0 {
			item.text = text[current[0].Start:current[len(current)-1].End]
		}
		for _, token := range current {
			item.tokens = append(item.tokens, token.Value)
		}
		items = append(items, item)
		current = nil
	}
	depth := 0
	for _, token := range tokens {
		switch {
		case token.MatchOperatorValue("(") || token.MatchOperatorValue("[") || token.MatchOperatorValue("{"):
			depth++
		case token.MatchOperatorValue(")") || token.MatchOperatorValue("]") || token.MatchOperatorValue("}"):
			depth--
		case depth == 0 && token.MatchOperatorValue(","):
			closeItem()
			continue
		}
		current = append(current, token)
	}
	closeItem()
	return items
}

// keyItems returns the expressions of a key clause. The outer tuple grouping
// and an empty tuple() are read the way [SameKey] reads them, so a key and its
// catalog spelling yield the same expressions: `(a, b)` and `a, b` both hold a
// and b, and `tuple()` holds none.
func keyItems(key string) []listItem {
	tokens := significant(key)
	for enclosesKey(values(tokens)) {
		tokens = tokens[1 : len(tokens)-1]
	}
	if len(tokens) == 3 && strings.EqualFold(tokens[0].Value, "tuple") && tokens[1].Value == "(" && tokens[2].Value == ")" {
		return nil
	}
	return splitList(key, tokens)
}

func values(tokens []lexer.Token) []string {
	result := make([]string, len(tokens))
	for i, token := range tokens {
		result[i] = token.Value
	}
	return result
}
