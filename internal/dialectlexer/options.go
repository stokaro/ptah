// Package dialectlexer provides shared SQL lexer configuration for database
// dialects used by internal parsing and analysis packages. It reads what a
// dialect's descriptor states about its text, descriptor.Lexical, and names no
// engine.
package dialectlexer

import (
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/descriptortable"
	"ptah.run/internal/lexer"
)

// Options returns the lexer behavior for dialect-sensitive SQL tokenization:
// [FromDescriptor] over the descriptor the process registry holds for
// dialect. An empty or unknown dialect gets SQL-standard strings and nothing
// else.
func Options(dialect string) lexer.Options {
	return optionsByDialect.Get(dialect)
}

// optionsByDialect holds the answer of [FromDescriptor] for every dialect.
// Options runs every time a lexer is built for a dialect, so the answers are
// derived once per registry rather than copied out of a descriptor per call.
var optionsByDialect = descriptortable.New(FromDescriptor)

// FromDescriptor returns the lexer behavior a dialect descriptor states in its
// Lexical section. Every dialect the shared lexer reads gets SQL-standard
// strings. A dialect whose descriptor names the YQL lexer gets that lexer
// alone: it reads none of the fields the other dialects set, so it sets none
// of them either, since a field that looked as if it applied would say
// something false about how the text is read.
//
// The shared lexer reads PostgreSQL's continued and U&'...' strings as one
// rule, so a descriptor that states one of the two has neither read as one
// constant, and such text is refused rather than read by a rule the server
// may not share.
func FromDescriptor(described descriptor.Dialect) lexer.Options {
	lexical := described.Lexical
	if lexical.Lexer == descriptor.LexerYQL {
		return lexer.Options{YQL: true}
	}
	return lexer.Options{
		StandardStrings:                true,
		BackslashEscapes:               lexical.BackslashEscapes,
		PostgreSQLEscapeStrings:        lexical.EscapeStrings,
		PostgreSQLStringConstants:      lexical.ContinuedStrings && lexical.UnicodeStrings,
		RequireWhitespaceAfterDashDash: lexical.DashCommentNeedsSpace,
		ExecutableComments:             executableComments(lexical.ExecutableComments),
		BracketIdentifiers:             lexical.BracketIdentifiers,
		DisableHashComments:            lexical.HashNames,
	}
}

// executableComments is the lexer's spelling of a descriptor's executable
// comments. A registry refuses a value the descriptor package does not
// declare, so an unknown one reaches here only from a descriptor nobody
// validated, and every block comment of such a dialect is a comment.
func executableComments(style descriptor.ExecutableComments) lexer.ExecutableCommentStyle {
	switch style {
	case descriptor.ExecutableCommentsBang:
		return lexer.ExecutableCommentsMySQL
	case descriptor.ExecutableCommentsMBang:
		return lexer.ExecutableCommentsMariaDB
	default:
		return lexer.ExecutableCommentsNone
	}
}
