// Package collatedkey spells an index key that is a column under a COLLATE of
// its own, `code COLLATE NOCASE`, in the one form a SQL document and a read of
// the database both give it. The model has no slot for a per-key collation, so
// the SQL frontend and the SQLite reader keep such a key as its text, and texts
// that differ only in quoting, an ASC, a comment or spacing are one key to the
// server. Without one spelling the comparison takes them for two keys and plans
// a drop and a create of an identical index.
//
// [ColumnCollation] reads the same clause where it follows a column in a
// table definition, which is the only place SQLite keeps a column's collation.
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
	collation, ok := collationName(tokens[2])
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

// ColumnCollation reads the collation definition declares, where definition
// is one column of a CREATE TABLE as dialect writes it: `code TEXT NOT NULL
// COLLATE NOCASE`. The name comes back as written, unquoted, from any of the
// spellings [Read] accepts for it. A COLLATE inside
// parentheses belongs to an expression, a CHECK or a generated column's AS,
// and is not the column's; of two COLLATE clauses the last one is, which is
// what sqlite3 3.51.0 applies.
//
// It reports false for a definition that declares no collation, for one whose
// clause names something other than a name, and for every definition
// of a dialect whose descriptor does not state
// Comparison.CollatedKeysRespelled.
func ColumnCollation(dialect, definition string) (string, bool) {
	described, found := platform.Lookup(dialect)
	if !found || !described.Comparison.CollatedKeysRespelled {
		return "", false
	}
	tokens := significantTokens(definition, dialectlexer.Options(dialect))
	collation, declared, depth := "", false, 0
	// The first token is the column's name, which may itself be a quoted
	// "COLLATE".
	for position := 1; position < len(tokens); position++ {
		token := tokens[position]
		switch {
		case token.Type == lexer.TokenOperator && token.Value == "(":
			depth++
		case token.Type == lexer.TokenOperator && token.Value == ")":
			depth--
		case depth == 0 && token.MatchIdentifierValue("COLLATE") && position+1 < len(tokens):
			name, ok := collationName(tokens[position+1])
			if !ok {
				// Not the earlier clause: the last one is what the column
				// compares with, so an earlier name would report a
				// collation the column does not have. The schema holds
				// only text SQLite accepted, so a name this reader cannot
				// spell is one to refuse to guess at.
				return "", false
			}
			collation, declared = name, true
			position++
		}
	}
	return collation, declared
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

// collationName reads the name a token after COLLATE spells: an identifier,
// or a single-quoted string, which sqlite3 3.51.0 takes for the same name
// there, `COLLATE 'NOCASE'` as `COLLATE NOCASE`, in a key and in a column.
func collationName(token lexer.Token) (string, bool) {
	if token.Type == lexer.TokenString && quotedBy(token.Value, '\'', '\'') {
		return strings.ReplaceAll(token.Value[1:len(token.Value)-1], "''", "'"), true
	}
	return identifierName(token)
}

func quotedBy(value string, open, closing byte) bool {
	return len(value) >= 2 && value[0] == open && value[len(value)-1] == closing
}
