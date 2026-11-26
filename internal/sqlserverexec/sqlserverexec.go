// Package sqlserverexec renders the EXEC statements the SQL Server renderer
// writes where T-SQL has no DDL for a change, and reads them back.
//
// SQL Server renames an object through the sp_rename procedure, and drops or
// replaces a column's default through a batch that sp_executesql runs,
// because the default is a constraint whose name the server chose. These are
// EXEC statements, and the dev replay guard in internal/devclean/sqlserverreplay
// refuses EXEC because it can run anything. The guard accepts them in the
// shape the renderer writes them, and in no other: [ReadRename],
// [ReadDefaultDrop] and [ReadDefaultReplace] read a statement back into the
// values that render it, render those values again and compare the two, so a
// statement the renderer would not write reads as none of them.
//
// It is a package rather than a function inside the renderer because two
// callers need the same text, and the guard cannot import the renderer, which
// lives under engine/builtin/internal. A second copy of a shape agrees when it
// is written and stops agreeing when the first one changes, and the guard
// would then refuse what the renderer writes, or accept what it does not
// (internal/sqlservertype is the same shape).
package sqlserverexec

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// QuoteName returns name in brackets, as QUOTENAME writes it: a closing
// bracket in the name is doubled. name is the name itself, without quotes.
func QuoteName(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

// QuoteQualifiedName joins the parts of a name, each quoted by [QuoteName],
// with dots.
func QuoteQualifiedName(parts []string) string {
	quoted := make([]string, len(parts))
	for i, part := range parts {
		quoted[i] = QuoteName(part)
	}
	return strings.Join(quoted, ".")
}

// UnicodeLiteral quotes s as an N'...' literal, the only string sp_executesql
// takes and the one that keeps a name outside the database's code page.
func UnicodeLiteral(s string) string {
	return "N'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// parseQualifiedName reads the parts of a name written as [QuoteQualifiedName]
// writes it, and reports false for any other text. It reads the brackets
// alone: an unquoted part, a double-quoted one and a dot outside the brackets
// next to another dot are not that spelling.
func parseQualifiedName(text string) ([]string, bool) {
	var parts []string
	for {
		part, rest, ok := parseQuotedName(text)
		if !ok {
			return nil, false
		}
		parts = append(parts, part)
		if rest == "" {
			return parts, true
		}
		var dotted bool
		text, dotted = strings.CutPrefix(rest, ".")
		if !dotted {
			return nil, false
		}
	}
}

// parseQuotedName reads one bracketed part from the start of text, undoing
// the doubled closing brackets, and returns the text after it.
func parseQuotedName(text string) (part, rest string, ok bool) {
	body, found := strings.CutPrefix(text, "[")
	if !found {
		return "", "", false
	}
	var name strings.Builder
	for i := 0; i < len(body); i++ {
		if body[i] != ']' {
			name.WriteByte(body[i])
			continue
		}
		if i+1 < len(body) && body[i+1] == ']' {
			name.WriteByte(']')
			i++
			continue
		}
		return name.String(), body[i+1:], true
	}
	return "", "", false
}

// tokens returns the tokens of stmt that carry meaning, read as SQL Server
// reads them: whitespace, comments and semicolons are left out, as the replay
// guard leaves them out.
func tokens(stmt string) []lexer.Token {
	lex := lexer.NewLexerWithOptions(stmt, lexerOptions())
	var read []lexer.Token
	for token := lex.NextToken(); token.Type != lexer.TokenEOF; token = lex.NextToken() {
		switch token.Type {
		case lexer.TokenWhitespace, lexer.TokenComment, lexer.TokenSemicolon:
			continue
		default:
			read = append(read, token)
		}
	}
	return read
}

// sameStatement reports whether stmt and rendered read as the same tokens,
// each of the same type and text. A string literal is one token, so its text
// is compared byte for byte.
func sameStatement(stmt, rendered string) bool {
	read, want := tokens(stmt), tokens(rendered)
	if len(read) != len(want) {
		return false
	}
	for i := range read {
		if read[i].Type != want[i].Type || read[i].Value != want[i].Value {
			return false
		}
	}
	return true
}

// unicodeLiteralAt reads the N'...' literal at index of tokens, and the index
// after it. The lexer reads a double-quoted text as a string too, which SQL
// Server reads as a name unless QUOTED_IDENTIFIER is off, so the quote is
// held to the one UnicodeLiteral writes.
func unicodeLiteralAt(tokens []lexer.Token, index int) (string, int, bool) {
	if index+1 >= len(tokens) ||
		tokens[index].Type != lexer.TokenIdentifier || tokens[index].Value != "N" ||
		!strings.HasPrefix(tokens[index+1].Value, "'") {
		return "", index, false
	}
	value, ok := lexer.StringValue(tokens[index+1].Value, lexerOptions())
	return value, index + 2, ok
}

func lexerOptions() lexer.Options {
	return dialectlexer.Options(platform.SQLServer)
}
