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
	// to the end of the line, such as "#" and "//". The shared lexer, the
	// client delimiter rewrite and the body scanner read each marker listed as
	// a comment wherever it stands, whatever follows it, and no marker that is
	// not listed. An engine that lists no "#" reads it as an operator
	// character, as PostgreSQL does for XOR and the #> and #- operators, or as
	// the start of a name (see HashNames).
	LineComments []string `json:"line_comments,omitempty"`
	// BackslashEscapes reports that a backslash escapes the next character
	// inside a string literal, quoted with ' or ". An engine that reads the
	// rule from session state, as MySQL reads it from sql_mode, states its
	// default.
	BackslashEscapes bool `json:"backslash_escapes,omitempty"`
	// IdentifierBackslashEscapes reports that a backslash also escapes the
	// next character inside a quoted identifier, as it does on ClickHouse.
	// The body scanner honors it, and so does the shared lexer for an engine
	// whose [Quoting] style is [QuoteBacktick]: where such an engine does not
	// state it, a backslash inside a backtick-quoted name is a character, as
	// on MySQL, where `a\` names a table whose name ends in a backslash.
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
	// NestedComments reports that a block comment may hold block comments,
	// each closed by its own */, as on PostgreSQL. Without it a comment ends
	// at the first */, which is the SQL standard.
	NestedComments bool `json:"nested_comments,omitempty"`
	// AlternativeQuoting reports that q'...' and nq'...' are string literals
	// that choose their own closing delimiter, as Oracle's are. The shared
	// lexer does not read them, so a scan that has to agree with the server
	// about where a string ends refuses one.
	AlternativeQuoting bool `json:"alternative_quoting,omitempty"`
	// LineCommentsEndAtLineFeed reports that a line comment, a -- comment or
	// one a LineComments marker opens, ends at a line feed alone, so a
	// carriage return is part of it, as on MySQL, where "CREATE #\rX\nTRIGGER"
	// reads as CREATE TRIGGER. The shared lexer reads it. Unset, a carriage
	// return ends a line comment too.
	LineCommentsEndAtLineFeed bool `json:"line_comments_end_at_line_feed,omitempty"`
	// DollarNames reports that a dollar sign is a name character wherever it
	// stands and opens no dollar-quoted string, as on MySQL, where `$`, `$$`
	// and the host in root@$ are names. The shared lexer and the client
	// delimiter rewrite read it; HeredocDollarTags has no effect beside it.
	DollarNames bool `json:"dollar_names,omitempty"`
	// DigitLeadingNames reports that a run of digits followed by a name
	// character is one name, as on MySQL, where `1other.t` names a table in
	// the database 1other. A run followed by an exponent, such as 1e5, stays a
	// number. The shared lexer reads it.
	DigitLeadingNames bool `json:"digit_leading_names,omitempty"`
	// NonASCIINames reports that every character outside ASCII is a name
	// character, so none of them is whitespace and none closes the --
	// marker DashCommentNeedsSpace reads, as on MySQL with a UTF-8 client
	// character set, where U+00A0 is part of a name. The shared lexer reads
	// it.
	NonASCIINames bool `json:"non_ascii_names,omitempty"`
	// HeredocDollarTags reports that a dollar-quoted string, `$tag$ ...
	// $tag$`, is read as a ClickHouse heredoc. Its tag is any run of ASCII
	// letters, digits, underscores and characters outside ASCII, so it may
	// begin with a digit: `$1abc$` opens a heredoc there. A tag that no closing
	// tag follows opens nothing, so `SELECT 1 AS $ab$` names a column.
	//
	// ClickHouse 24.10 opens a heredoc at a tag holding any character but a
	// dollar sign, `$a-b$` among them, and this rule does not read that tag.
	// 26.9 and 26.10 refuse it, and a tag outside ASCII, with code 62; neither
	// reads a character outside ASCII anywhere but in a heredoc or a quote, so
	// reading one as part of a tag changes nothing those servers accept.
	//
	// Unset, a tag is spelled as the PostgreSQL family spells it: an ASCII
	// letter, an underscore or any character outside ASCII, and a digit after
	// the first, so `$1abc$` is the parameter `$1` followed by a name. The
	// shared lexer and the client delimiter rewrite read it.
	HeredocDollarTags bool `json:"heredoc_dollar_tags,omitempty"`
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
