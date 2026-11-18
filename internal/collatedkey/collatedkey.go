// Package collatedkey spells an index key that is a column under a COLLATE of
// its own, `code COLLATE NOCASE`, in the one form a SQL document and a read of
// the database both give it. The model has no slot for a per-key collation, so
// the SQL frontend and the SQLite reader keep such a key as its text, and texts
// that differ only in quoting, an ASC, a comment or spacing are one key to the
// server. Without one spelling the comparison takes them for two keys and plans
// a drop and a create of an identical index.
package collatedkey

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlident"
)

// Key is a collated index key. Text is `column COLLATE collation`, and Desc
// reports a DESC after it. ASC is the default direction and leaves no trace.
type Key struct {
	Text string
	Desc bool
}

// Read reads element, one entry of a CREATE INDEX key list as dialect writes
// it, as a column under a COLLATE of its own. The column is written bare when
// it is a plain name and quoted otherwise, as [sqlident.BareOrQuoted] spells it
// for dialect; the collation is written as the element writes it; comments and
// spacing are dropped.
//
// It reports false for any other shape, such as an expression or a column with
// no COLLATE, and for every element on a dialect where a bare and a quoted
// spelling of one name can name two columns: one whose descriptor folds an
// unquoted name (Names.Unquoted) or writes every plain name bare
// (Quoting.Bare), and one with a lexer of its own. There the caller keeps the
// text as written.
func Read(dialect, element string) (Key, bool) {
	options, ok := unquotingOptions(dialect)
	if !ok {
		return Key{}, false
	}
	tokens := significantTokens(element, options)
	if len(tokens) < 3 || len(tokens) > 4 || !tokens[1].MatchIdentifierValue("COLLATE") {
		return Key{}, false
	}
	column, ok := identifierName(tokens[0])
	if !ok {
		return Key{}, false
	}
	if _, ok := identifierName(tokens[2]); !ok {
		return Key{}, false
	}
	key := Key{Text: sqlident.BareOrQuoted(dialect, column) + " COLLATE " + tokens[2].Value}
	if len(tokens) == 4 {
		switch {
		case tokens[3].MatchIdentifierValue("DESC"):
			key.Desc = true
		case tokens[3].MatchIdentifierValue("ASC"):
		default:
			return Key{}, false
		}
	}
	return key, true
}

// unquotingOptions returns the lexer options for dialect when a quoted plain
// name and the bare one name the same column there.
func unquotingOptions(dialect string) (lexer.Options, bool) {
	described, found := platform.Lookup(dialect)
	if !found || described.Names.Unquoted != "" || described.Quoting.Bare == descriptor.BareEverywhere {
		return lexer.Options{}, false
	}
	options := dialectlexer.Options(dialect)
	return options, options.Scanner == nil
}

func significantTokens(element string, options lexer.Options) []lexer.Token {
	scanner := lexer.NewLexerWithOptions(element, options)
	var tokens []lexer.Token
	for {
		token := scanner.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return tokens
		case lexer.TokenWhitespace, lexer.TokenComment:
		default:
			tokens = append(tokens, token)
		}
	}
}

// identifierName reads the name a token spells: bare, double-quoted,
// backticked or bracketed. A string literal, a number and an operator name
// nothing.
func identifierName(token lexer.Token) (string, bool) {
	value := token.Value
	switch {
	case token.Type == lexer.TokenString && quotedBy(value, '"', '"'):
		return strings.ReplaceAll(value[1:len(value)-1], `""`, `"`), true
	case token.Type != lexer.TokenIdentifier || value == "":
		return "", false
	case quotedBy(value, '`', '`'):
		return strings.ReplaceAll(value[1:len(value)-1], "``", "`"), true
	case quotedBy(value, '[', ']'):
		return strings.ReplaceAll(value[1:len(value)-1], "]]", "]"), true
	case value[0] >= '0' && value[0] <= '9':
		return "", false
	default:
		return value, true
	}
}

func quotedBy(value string, open, closing byte) bool {
	return len(value) >= 2 && value[0] == open && value[len(value)-1] == closing
}
