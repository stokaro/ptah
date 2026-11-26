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
	"ptah.run/internal/sqlserverbatch"
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
		if !strings.HasPrefix(text, "[") {
			return nil, false
		}
		part, rest, ok := readNamePart(text)
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

// SplitName reads the parts of a name the server reads from a string, such as
// the table DBCC CHECKIDENT takes: parts joined with dots, each in brackets, in
// double quotes, or a name the server reads unquoted (see
// [sqlserverbatch.RegularIdentifier]). A part left out, as the schema in
// db..table, is empty; the last part cannot be. It reports false for any other
// text, a space beside a dot among it, so a caller that cannot read a name
// can refuse it rather than guess which database it names.
func SplitName(text string) ([]string, bool) {
	var parts []string
	for {
		part, rest, ok := readNamePart(text)
		dotted := strings.HasPrefix(rest, ".")
		if !ok || part == "" && !dotted {
			return nil, false
		}
		parts = append(parts, part)
		if rest == "" {
			return parts, true
		}
		if !dotted {
			return nil, false
		}
		text = rest[1:]
	}
}

// readNamePart reads one part of a name from the start of text and returns
// the text after it. A part in brackets or double quotes ends at its closing
// quote and undoes a doubled one; an unquoted part ends at the next dot and
// may be empty.
func readNamePart(text string) (part, rest string, ok bool) {
	closing := map[byte]byte{'[': ']', '"': '"'}
	if text == "" || closing[text[0]] == 0 {
		end := strings.IndexByte(text, '.')
		if end < 0 {
			end = len(text)
		}
		part = text[:end]
		return part, text[end:], part == "" || sqlserverbatch.RegularIdentifier(part)
	}
	quote := text[0]
	var name strings.Builder
	body := text[1:]
	for i := 0; i < len(body); i++ {
		if body[i] != closing[quote] {
			name.WriteByte(body[i])
			continue
		}
		if i+1 < len(body) && body[i+1] == closing[quote] {
			name.WriteByte(body[i])
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
