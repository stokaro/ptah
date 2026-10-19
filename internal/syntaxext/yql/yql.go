// Package yql serves the syntax of YDB's query language: a document a
// dialect's descriptor says the YQL lexer reads (descriptor.LexerYQL) is
// parsed by internal/yqlparse.
package yql

import (
	"ptah.run/core/ast"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/syntaxext"
	"ptah.run/internal/yqlparse"
)

// Services returns the YQL lexer's syntax services.
func Services() syntaxext.Services {
	return syntaxext.Services{Lexers: []string{descriptor.LexerYQL}, Parser: parser{}}
}

// parser reads YQL with internal/yqlparse, which takes no owners,
// capabilities or timeout.
type parser struct{}

func (parser) Parse(text string) (*ast.StatementList, error) {
	return yqlparse.Parse(text)
}
