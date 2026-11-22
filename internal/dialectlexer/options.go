// Package dialectlexer provides shared SQL lexer configuration for database
// dialects used by internal parsing and analysis packages. It reads what a
// dialect's descriptor states about its text, descriptor.Lexical, and names no
// engine.
package dialectlexer

import (
	"slices"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/backslashmode"
	"ptah.run/internal/clientdelimiter"
	"ptah.run/internal/descriptortable"
	"ptah.run/internal/lexer"
	"ptah.run/internal/lexerext"
	"ptah.run/internal/lexerext/builtin"
)

// Options returns the lexer behavior for dialect-sensitive SQL tokenization:
// the scanner a provider registered for dialect (lexerext.Provide), the
// rules a provider declared for it ([RegisterLexical]), or [FromDescriptor]
// over the descriptor the process registry holds for dialect. An empty or
// unknown dialect gets SQL-standard strings and nothing else.
func Options(dialect string) lexer.Options {
	if scanner := lexerext.Provided(dialect); scanner != nil {
		return lexer.Options{Scanner: scanner}
	}
	if options, found := declaredOptions(dialect); found {
		options.LineComments = slices.Clone(options.LineComments)
		return options
	}
	options := optionsByDialect.Get(dialect)
	options.LineComments = slices.Clone(options.LineComments)
	if platform.NormalizeDialect(dialect) == "" {
		// A blank or unknown name states no markers, and an empty list is a
		// statement too, so such text keeps the lexer's heuristic for #.
		options.LineCommentsDeclared, options.LineComments = false, nil
	}
	return options
}

// ForSession is [Options] for text a session with caps reads: a session whose
// sql_mode names NO_BACKSLASH_ESCAPES reads a backslash in a string as the
// character, whatever the dialect's descriptor says; see
// [backslashmode.Escapes]. A provider's own scanner is returned as Options
// returns it.
func ForSession(dialect string, caps capability.Capabilities) lexer.Options {
	options := Options(dialect)
	if options.Scanner == nil {
		options.BackslashEscapes = backslashmode.Escapes(options.BackslashEscapes, caps)
	}
	return options
}

// optionsByDialect holds the answer of [FromDescriptor] for every dialect.
// Options runs every time a lexer is built for a dialect, so the answers are
// derived once per registry rather than copied out of a descriptor per call.
var optionsByDialect = descriptortable.New(FromDescriptor)

// FromDescriptor returns the lexer behavior a dialect descriptor states in its
// Lexical section. Every dialect the shared lexer reads gets SQL-standard
// strings. A dialect whose descriptor names a lexer of its own that a service
// serves (internal/lexerext) gets that lexer's scanner alone: it reads none of
// the fields the other dialects set, so it sets none of them either, since a
// field that looked as if it applied would say something false about how the
// text is read.
//
// The line comment markers the descriptor lists are the whole rule: the lexer
// reads each as a comment wherever it stands, whatever follows it, and reads a
// # that is not listed as an operator character. A dialect that lists
// none, such as PostgreSQL, has no # comments.
//
// The shared lexer reads PostgreSQL's continued and U&'...' strings as one
// rule, so a descriptor that states one of the two has neither read as one
// constant, and such text is refused rather than read by a rule the server
// may not share.
func FromDescriptor(described descriptor.Dialect) lexer.Options {
	lexical := described.Lexical
	if scanner := builtin.Lookup(lexical.Lexer).Scanner; scanner != nil {
		return lexer.Options{Scanner: scanner}
	}
	return lexer.Options{
		StandardStrings:                true,
		BackslashEscapes:               lexical.BackslashEscapes,
		PostgreSQLEscapeStrings:        lexical.EscapeStrings,
		PostgreSQLStringConstants:      lexical.ContinuedStrings && lexical.UnicodeStrings,
		RequireWhitespaceAfterDashDash: lexical.DashCommentNeedsSpace,
		ExecutableComments:             executableComments(lexical.ExecutableComments),
		BracketIdentifiers:             lexical.BracketIdentifiers,
		LineCommentsDeclared:           true,
		LineComments:                   slices.Clone(lexical.LineComments),
		DisableHashComments:            lexical.HashNames,
		NestedBlockComments:            lexical.NestedComments,
		DollarTags:                     DollarTags(lexical),
	}
}

// DollarTags is the [lexer.DollarTagRule] lexical states: the ClickHouse
// heredoc rule where it states heredoc tags, and the PostgreSQL rule
// otherwise, the zero descriptor of a text no dialect names included. Every
// reader of dollar-quoted strings takes its rule from here, so the lexer and
// the readers that keep a scanner of their own read one tag set per dialect.
func DollarTags(lexical descriptor.Lexical) lexer.DollarTagRule {
	if lexical.HeredocDollarTags {
		return lexer.HeredocDollarTags
	}
	return lexer.PostgreSQLDollarTags
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

// ClientDelimiterRules is how a script written for dialect spells its
// literals, for the client delimiter rewrite: where a literal or comment ends
// decides whether a line that reads like a directive stands inside one. The
// backslash rule is the session's ([ForSession]) and the line comment markers
// are the descriptor's. A `$tag$` body quotes in every dialect except MySQL and
// MariaDB, where a dollar sign is a name character: the shared lexer reads
// one for the rest, PostgreSQL's dollar quotes and ClickHouse's heredocs
// among them, and the rewrite reads its tag by the lexer's rule. A blank
// dialect has no rules to read; ok is false and the caller uses the
// permissive set [clientdelimiter.Normalize] reads with.
func ClientDelimiterRules(dialect string, caps capability.Capabilities) (rules clientdelimiter.Rules, ok bool) {
	if dialect == "" {
		return clientdelimiter.Rules{}, false
	}
	options := ForSession(dialect, caps)
	return clientdelimiter.Rules{
		BackslashEscapes: options.BackslashEscapes,
		EscapeStrings:    options.PostgreSQLEscapeStrings,
		// No descriptor field states the dollar rule itself. Executable
		// comments are what the descriptors set on MySQL and MariaDB alone,
		// so they stand in for it.
		DollarQuotes:         options.ExecutableComments == lexer.ExecutableCommentsNone,
		LineComments:         options.LineComments,
		LineCommentsDeclared: true,
		BracketIdentifiers:   options.BracketIdentifiers,
		DollarTags:           options.DollarTags,
	}, true
}
