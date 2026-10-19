// Package sqlsyntax parses a SQL document into the statements a syntax
// consumer reads, in the grammar of the document's dialect: YQL for a dialect
// whose descriptor names the YQL lexer, as YDB's does, and the shared grammar
// of internal/parser for every other dialect and for a document with no
// dialect.
//
// It is the one place a consumer of SQL syntax, rather than of the schema a
// document declares, reaches a YQL syntax tree, so the shared parser holds no
// YQL reading. The schema read does not come through here: the YDB owner
// reads YQL with its own grammar (internal/ydbsql).
//
// The dispatch to YQL is plugin debt. A provider served over the protocol
// returns declarations, not syntax trees, so whether a grammar owner may also
// serve a syntax tree, and in what form, is still open under
// stokaro/ptah#4204. Until it is decided, the YQL tree comes from
// internal/yqlparse in process.
package sqlsyntax

import (
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/parser"
	"ptah.run/internal/yqlparse"
)

// Parse returns the statements text parses to in the grammar of dialect. A
// document of a dialect whose text is YQL is read as YQL, and options do not
// apply to it: the YQL reader takes no owners, capabilities or timeout. Any
// other dialect, and an empty one, is read by internal/parser with options.
// An error says why the text could not be read; it carries no statements.
func Parse(text, dialect string, options ...parser.Option) (*ast.StatementList, error) {
	if described, _ := platform.Lookup(dialect); described.Lexical.Lexer == descriptor.LexerYQL {
		return yqlparse.Parse(text)
	}
	return parser.NewParser(text, append([]parser.Option{parser.WithDialect(dialect)}, options...)...).Parse()
}
