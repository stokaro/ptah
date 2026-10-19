package descriptor

import (
	"fmt"
	"reflect"
	"slices"
)

// Lexical describes the engine's SQL text where it differs from the standard.
// internal/dialectlexer configures the shared SQL lexer from it, and the
// scanner of routine and view bodies in internal/deporder reads it too. A
// field names the reader that honors it where only one of them does.
type Lexical struct {
	// Lexer names a lexer of the engine's own its text is read with, such as
	// [LexerYQL]. A service of that lexer reads the text in place of the
	// shared SQL lexer, which holds no rule of it (internal/lexerext). Empty
	// reads the shared SQL lexer, with the rules below. A descriptor that
	// names a lexer states none of those rules, since that lexer reads none of
	// them.
	Lexer string `json:"lexer,omitempty"`
	// LineComments are the markers, beside "--", that open a comment running
	// to the end of the line, such as "#" and "//". The body scanner honors
	// them. The shared lexer reads a # that no operator character follows as
	// a comment on every engine whose names cannot begin with one (see
	// HashNames), and reads // as two operators everywhere.
	LineComments []string `json:"line_comments,omitempty"`
	// BackslashEscapes reports that a backslash escapes the next character
	// inside a string literal, quoted with ' or ". An engine that reads the
	// rule from session state, as MySQL reads it from sql_mode, states its
	// default.
	BackslashEscapes bool `json:"backslash_escapes,omitempty"`
	// IdentifierBackslashEscapes reports that a backslash also escapes the
	// next character inside a quoted identifier, as it does on ClickHouse.
	// The body scanner honors it. On MySQL it does not: `a\` names a table
	// whose name ends in a backslash.
	IdentifierBackslashEscapes bool `json:"identifier_backslash_escapes,omitempty"`
	// EscapeStrings reports that E'...' is a string literal in which a
	// backslash escapes the next character whatever BackslashEscapes says,
	// as in PostgreSQL.
	EscapeStrings bool `json:"escape_strings,omitempty"`
	// ContinuedStrings reports that two quoted strings separated only by
	// whitespace holding a newline are one constant, where a -- comment counts
	// as whitespace and a block comment does not (section 4.1.2.2 of the
	// PostgreSQL manual). The shared lexer reads this form only together with
	// UnicodeStrings: an engine that states one of the two has neither read as
	// one constant, so such text is two tokens and is refused rather than read
	// by a rule the server may not share.
	ContinuedStrings bool `json:"continued_strings,omitempty"`
	// UnicodeStrings reports that U&'...' is a string literal whose escapes
	// name code points, with the UESCAPE clause that may follow it. The
	// shared lexer reads it only together with ContinuedStrings.
	UnicodeStrings bool `json:"unicode_strings,omitempty"`
	// DashCommentNeedsSpace reports that -- opens a comment only when
	// whitespace or a control character follows it, as on MySQL, where
	// `1--1` is a subtraction.
	DashCommentNeedsSpace bool `json:"dash_comment_needs_space,omitempty"`
	// ExecutableComments is how a block comment marks text the server runs:
	// [ExecutableCommentsBang] or [ExecutableCommentsMBang]. Empty means every
	// block comment is a comment.
	ExecutableComments ExecutableComments `json:"executable_comments,omitempty"`
	// BracketIdentifiers reports that a pair of square brackets quotes an
	// identifier, as on SQL Server, where [a b] is the name `a b`.
	BracketIdentifiers bool `json:"bracket_identifiers,omitempty"`
	// HashNames reports that a name may begin with #, as the temporary tables
	// #t and ##t of SQL Server do, so # opens no comment.
	HashNames bool `json:"hash_names,omitempty"`
	// AlternativeQuoting reports that q'...' and nq'...' are string literals
	// that choose their own closing delimiter, as Oracle's are. The shared
	// lexer does not read them, so a scan that has to agree with the server
	// about where a string ends refuses one.
	AlternativeQuoting bool `json:"alternative_quoting,omitempty"`
}

// LexerYQL is the [Lexical.Lexer] of YDB's query language.
const LexerYQL = "yql"

// Lexers returns every declared [Lexical.Lexer] value.
func Lexers() []string {
	return []string{LexerYQL}
}

// ExecutableComments is a syntax in which a block comment carries text the
// server runs.
type ExecutableComments string

const (
	// ExecutableCommentsBang is /*! ... */, which the server runs, with an
	// optional version number after the ! below which it is a comment, as on
	// MySQL.
	ExecutableCommentsBang ExecutableComments = "bang"
	// ExecutableCommentsMBang is MariaDB's syntax: /*M! ... */ beside
	// /*! ... */, where a /*! whose version has five digits and lies between
	// 50700 and 99999 is a comment.
	ExecutableCommentsMBang ExecutableComments = "m-bang"
)

// ExecutableCommentStyles returns every declared executable-comment syntax.
func ExecutableCommentStyles() []ExecutableComments {
	return []ExecutableComments{ExecutableCommentsBang, ExecutableCommentsMBang}
}

// validateLexical checks the fields declared in this file.
func (d Dialect) validateLexical() error {
	lexical := d.Lexical
	if lexical.Lexer != "" {
		if !slices.Contains(Lexers(), lexical.Lexer) {
			return fmt.Errorf("%w: dialect %q: lexer %q is not one of %q",
				ErrInvalidDescriptor, d.Name, lexical.Lexer, Lexers())
		}
		rules := lexical
		rules.Lexer, rules.LineComments = "", nil
		if len(lexical.LineComments) > 0 || !reflect.DeepEqual(rules, Lexical{}) {
			return fmt.Errorf("%w: dialect %q: lexer %q reads none of the shared lexer's rules, so the descriptor states none",
				ErrInvalidDescriptor, d.Name, lexical.Lexer)
		}
	}
	if lexical.ExecutableComments != "" && !slices.Contains(ExecutableCommentStyles(), lexical.ExecutableComments) {
		return fmt.Errorf("%w: dialect %q: executable comments %q is not one of %q",
			ErrInvalidDescriptor, d.Name, lexical.ExecutableComments, ExecutableCommentStyles())
	}
	for _, marker := range lexical.LineComments {
		if marker == "" || marker == "--" {
			return fmt.Errorf("%w: dialect %q: line comment marker %q is empty or the standard one",
				ErrInvalidDescriptor, d.Name, marker)
		}
	}
	return nil
}
