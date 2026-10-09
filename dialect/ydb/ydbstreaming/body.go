package ydbstreaming

import (
	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// SameBody compares YQL tokens, preserving literal contents and operators.
// YDB 26.2 removes comments from stored streaming-query bodies on ALTER.
// Comparing raw text would repeatedly reset the checkpoint for a comment.
func SameBody(a, b string) bool {
	left := lexer.NewLexerWithOptions(a, dialectlexer.Options(platform.YDB))
	right := lexer.NewLexerWithOptions(b, dialectlexer.Options(platform.YDB))
	for {
		x, y := bodyToken(left), bodyToken(right)
		if x.Type != y.Type || x.Value != y.Value {
			return false
		}
		if x.Type == lexer.TokenEOF {
			return true
		}
	}
}

func bodyToken(scanner *lexer.Lexer) lexer.Token {
	for {
		token := scanner.NextToken()
		if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenComment {
			return token
		}
	}
}
