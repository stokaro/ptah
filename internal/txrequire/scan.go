package txrequire

import (
	"slices"
	"strings"

	"go.5x5.cz/ptah/internal/dialectlexer"
	"go.5x5.cz/ptah/internal/lexer"
)

// tokenize renders a statement as the word sequence the rules scan.
//
// Bare words are upper-cased; string literals and quoted identifiers keep
// their quotes and their case, so a literal containing CONCURRENTLY, or a
// column named "type", can never impersonate a keyword. It is the same shape
// migration/lint's own scanner produces, built here on the lexer core/sqlutil
// already splits statements with, so one tokenization rule serves both.
func tokenize(dialect, sql string) []string {
	return scan(sql, dialectlexer.Options(dialect))
}

// tokenizeSource tokenizes without folding case, so a name can be reported
// the way its author spelled it.
//
// A diagnostic that renames the object it is about is worse than one that
// omits the name: on PostgreSQL a quoted `"mood"` and `MOOD` are different
// types, and telling an operator to look at the wrong one costs more than
// telling them nothing.
func tokenizeSource(dialect, sql string) []string {
	lex := lexer.NewLexerWithOptions(sql, dialectlexer.Options(dialect))
	words := make([]string, 0, 16)
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return words
		case lexer.TokenComment, lexer.TokenWhitespace:
			continue
		default:
			words = append(words, token.Value)
		}
	}
}

func scan(sql string, options lexer.Options) []string {
	lex := lexer.NewLexerWithOptions(sql, options)
	words := make([]string, 0, 16)
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return words
		case lexer.TokenComment, lexer.TokenWhitespace:
			continue
		case lexer.TokenString:
			words = append(words, token.Value)
		case lexer.TokenIdentifier:
			words = append(words, identifierWord(token.Value))
		default:
			words = append(words, token.Value)
		}
	}
}

// identifierWord upper-cases a bare identifier and leaves a quoted one alone.
func identifierWord(value string) string {
	if isQuoted(value) {
		return value
	}
	return strings.ToUpper(value)
}

func isQuoted(value string) bool {
	if len(value) < 2 {
		return false
	}
	switch value[0] {
	case '"', '`', '[':
		return true
	}
	return false
}

// hasPrefixWords reports whether the word sequence starts with the keywords.
func hasPrefixWords(words []string, keywords ...string) bool {
	if len(words) < len(keywords) {
		return false
	}
	for i, keyword := range keywords {
		if words[i] != keyword {
			return false
		}
	}
	return true
}

func containsWord(words []string, word string) bool {
	return slices.Contains(words, word)
}
