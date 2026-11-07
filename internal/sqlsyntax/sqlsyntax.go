// Package sqlsyntax parses a SQL document into the statements a syntax
// consumer reads, in the grammar of the document's dialect: the parser of the
// lexer its descriptor names, where the descriptor names a lexer of its own
// and a service serves it (internal/syntaxext), and the shared grammar of
// internal/parser for every other dialect and for a document with no dialect.
//
// It is the one place a consumer of SQL syntax, rather than of the schema a
// document declares, reaches a syntax tree of an engine's own grammar, so the
// shared parser holds no such reading. The schema read does not come through
// here: an engine with a grammar of its own reads its schema with that
// grammar in its own packages.
package sqlsyntax

import (
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/parser"
	"ptah.run/internal/syntaxext/builtin"
)

// Reads reports whether [Parse] holds a grammar for dialect, so that its refusal
// says something about the text. A run with no dialect reads the shared grammar.
// An engine Ptah has no descriptor for is unknown to it, and [Parse] hands that to
// the shared grammar too, which refuses what the engine accepts.
func Reads(dialect string) bool {
	if dialect == "" {
		return true
	}
	described, known := platform.Lookup(dialect)
	return known && ReadsDescribed(described)
}

// ReadsDescribed is [Reads] for a descriptor. A dialect whose descriptor names a
// lexer no built-in service serves is read by its engine's provider.
func ReadsDescribed(described descriptor.Dialect) bool {
	return described.Lexical.Lexer == "" || builtin.Lookup(described.Lexical.Lexer).Parser != nil
}

// Parse returns the statements text parses to in the grammar of dialect. A
// document of a dialect whose descriptor names a lexer with a parser of its
// own is read by that parser, and options do not apply to it. Any other
// dialect, and an empty one, is read by internal/parser with options. An
// error says why the text could not be read; it carries no statements.
func Parse(text, dialect string, options ...parser.Option) (*ast.StatementList, error) {
	described, _ := platform.Lookup(dialect)
	if own := builtin.Lookup(described.Lexical.Lexer).Parser; own != nil {
		return own.Parse(text)
	}
	return parser.NewParser(text, append([]parser.Option{parser.WithDialect(dialect)}, options...)...).Parse()
}
