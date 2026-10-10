package ydbstreaming

import (
	"slices"
	"strings"

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

// NamedPaths returns the scheme paths a query body names as a source or a
// target -- the name after FROM, JOIN or INTO -- each once, in the order the
// body first names it. A backticked name is returned without its backticks
// and escapes, a bare one as written. The body does not say what each path
// holds, so a caller keeps only the ones that name objects it manages.
//
// A name a data source qualifies (`source`.`topic`), a table function, a
// named expression and a subquery name no path of the database and are left
// out. A body that sets TablePathPrefix resolves its names against a prefix
// Ptah does not follow, so it names none.
func NamedPaths(text string) []string {
	scanner := lexer.NewLexerWithOptions(text, dialectlexer.Options(platform.YDB))
	var tokens []lexer.Token
	for token := bodyToken(scanner); token.Type != lexer.TokenEOF; token = bodyToken(scanner) {
		if token.Type == lexer.TokenIdentifier && strings.EqualFold(token.Value, "TablePathPrefix") {
			return nil
		}
		tokens = append(tokens, token)
	}
	var paths []string
	for index := 0; index+1 < len(tokens); index++ {
		keyword := tokens[index]
		if keyword.Type != lexer.TokenIdentifier || !slices.ContainsFunc([]string{"FROM", "JOIN", "INTO"}, func(word string) bool {
			return strings.EqualFold(keyword.Value, word)
		}) {
			continue
		}
		name := tokens[index+1]
		if name.Type != lexer.TokenIdentifier || strings.HasPrefix(name.Value, "$") {
			continue
		}
		if index+2 < len(tokens) && (tokens[index+2].MatchOperatorValue(".") || tokens[index+2].MatchOperatorValue("(")) {
			continue
		}
		path := unquotePath(name.Value)
		if path != "" && !slices.Contains(paths, path) {
			paths = append(paths, path)
		}
	}
	return paths
}

// unquotePath is the path a YQL identifier names: a backticked one without
// its backticks and escapes, and a bare one as written.
func unquotePath(identifier string) string {
	if len(identifier) < 2 || identifier[0] != '`' || identifier[len(identifier)-1] != '`' {
		return identifier
	}
	inner := identifier[1 : len(identifier)-1]
	var b strings.Builder
	for i := 0; i < len(inner); i++ {
		switch {
		case inner[i] == '\\' && i+1 < len(inner):
			i++
			b.WriteByte(inner[i])
		case inner[i] == '`' && i+1 < len(inner) && inner[i+1] == '`':
			i++
			b.WriteByte('`')
		default:
			b.WriteByte(inner[i])
		}
	}
	return b.String()
}
