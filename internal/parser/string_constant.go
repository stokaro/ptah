package parser

import (
	"fmt"
	"strings"

	"ptah.run/core/ptaherr"
	"ptah.run/internal/lexer"
)

// stringConstant reads the string constant at the current token, whatever
// statement it stands in, and answers the text it stands for.
//
// A string constant is one grammar wherever a statement takes one: a comment,
// an enum label, a password, an extension version and a column default all
// accept the same spellings, so they are read by one function rather than
// each unquoting the token its own way. The lexer decides which spellings make
// one token -- under PostgreSQL a string continued on the next line, `E'...'`,
// `U&'...'` with its UESCAPE clause, and a dollar-quoted string -- and
// [lexer.StringValue] reads the text. Stripped of its quotes by hand instead,
// a continued string reads as everything between its first and last quote,
// and a dollar-quoted one keeps its dollars (stokaro/ptah#3731).
//
// A double-quoted token is an identifier, not a string constant, and is
// refused, as is a string whose escapes the server refuses.
func (p *Parser) stringConstant(what string) (string, error) {
	p.skipWhitespace()
	if p.current.Type != lexer.TokenString || isDoubleQuotedIdentifierToken(p.current) {
		return "", ptaherr.At(p.current.Start, fmt.Errorf("expected a string constant for %s", what))
	}
	text, ok := lexer.StringValue(p.current.Value, p.stringLiteralOptions())
	if !ok {
		return "", ptaherr.At(p.current.Start, fmt.Errorf("%s is not a string constant the server reads, for %s",
			abbreviatedToken(p.current.Value), what))
	}
	p.advance()
	return text, nil
}

// quotedStringConstant writes text as a standard single-quoted string
// constant. Only the quote is doubled, so a server with
// standard_conforming_strings on, the default, reads back exactly text.
func quotedStringConstant(text string) string {
	return "'" + strings.ReplaceAll(text, "'", "''") + "'"
}

// escapedStringConstant writes text as a server that escapes with a backslash stores it:
// ClickHouse reports `'it”s'` as `'it\'s'`, and a newline as `\n`.
func escapedStringConstant(text string) string {
	return "'" + storedStringEscapes.Replace(text) + "'"
}

var storedStringEscapes = strings.NewReplacer(
	`\`, `\\`, `'`, `\'`, "\n", `\n`, "\t", `\t`, "\r", `\r`, "\b", `\b`, "\f", `\f`, "\x00", `\0`,
)

func abbreviatedToken(token string) string {
	const limit = 40
	if len(token) <= limit {
		return token
	}
	return token[:limit] + "..."
}
