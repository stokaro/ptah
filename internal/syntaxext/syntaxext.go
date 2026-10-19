// Package syntaxext declares what a lexer of an engine's own brings to a
// consumer of SQL syntax: the statements a document in its text parses to.
// internal/sqlsyntax asks for a parser by the lexer a dialect's descriptor
// names (core/platform/descriptor.Lexical.Lexer), so it names no engine and
// no lexer, and internal/syntaxext/builtin lists the parsers of the lexers
// Ptah ships.
//
// The parser runs in process. Whether a provider served over the protocol may
// also serve a syntax tree, and in what form, is open under stokaro/ptah#4204.
package syntaxext

import "ptah.run/core/ast"

// Services is what one lexer brings to a syntax consumer. The zero value
// brings nothing, and a document its dialect reads with the shared lexer is
// read by the shared parser.
type Services struct {
	// Lexers are the descriptor.Lexical.Lexer names these services serve.
	Lexers []string
	// Parser reads a document of the lexer's text.
	Parser Parser
}

// Parser reads a document into the statements a syntax consumer reads.
type Parser interface {
	// Parse returns the statements text parses to. An error says why the
	// text could not be read; it carries no statements.
	Parse(text string) (*ast.StatementList, error)
}
