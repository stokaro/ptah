// Package collatedkey spells an index key that is a column under a COLLATE of
// its own, `code COLLATE NOCASE`, in the one form a SQL document and a read of
// the database both give it. The model has no slot for a per-key collation, so
// the SQL frontend and the SQLite reader keep such a key as its text, and texts
// that differ only in quoting, an ASC, a comment or spacing are one key to the
// server. Without one spelling the comparison takes them for two keys and plans
// a drop and a create of an identical index.
//
// The spelling is applied only on a dialect whose descriptor states
// Comparison.CollatedKeysRespelled; any other dialect keeps the text as
// written.
package collatedkey

import (
	"strings"

	"ptah.run/core/platform"
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
// it, as a column under a COLLATE of its own. The column and the collation are
// each written bare when they are a plain name and quoted otherwise, as
// [sqlident.BareOrQuoted] spells them for dialect, and comments and spacing are
// dropped.
//
// It reports false for any other shape, such as an expression or a column with
// no COLLATE, and for every element of a dialect whose descriptor does not
// state Comparison.CollatedKeysRespelled. There the caller keeps the text as
// written.
func Read(dialect, element string) (Key, bool) {
	described, found := platform.Lookup(dialect)
	if !found || !described.Comparison.CollatedKeysRespelled {
		return Key{}, false
	}
	tokens := significantTokens(element, dialectlexer.Options(dialect))
	if len(tokens) < 3 || len(tokens) > 4 || !tokens[1].MatchIdentifierValue("COLLATE") {
		return Key{}, false
	}
	column, ok := identifierName(tokens[0])
	if !ok {
		return Key{}, false
	}
	collation, ok := identifierName(tokens[2])
	if !ok {
		return Key{}, false
	}
	key := Key{Text: sqlident.BareOrQuoted(dialect, column) + " COLLATE " + sqlident.BareOrQuoted(dialect, collation)}
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
// backquoted or bracketed. A string literal, a number and an operator name
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
		// A bracketed name runs to the first closing bracket and has no
		// escape: sqlite3 refuses [a]]b] with "unrecognized token", where the
		// shared lexer reads the pair as SQL Server's escaped bracket.
		name := value[1 : len(value)-1]
		return name, !strings.Contains(name, "]")
	case value[0] >= '0' && value[0] <= '9':
		return "", false
	default:
		return value, true
	}
}

func quotedBy(value string, open, closing byte) bool {
	return len(value) >= 2 && value[0] == open && value[len(value)-1] == closing
}
