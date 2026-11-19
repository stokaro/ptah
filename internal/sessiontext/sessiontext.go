// Package sessiontext writes a statement Ptah wrote in its own reading of a
// dialect's text as the session that receives it reads it. It sits between a
// plan and the executor that sends the plan: the plan, the files written from
// it and every scan of them keep Ptah's reading, and only the text sent to a
// session whose settings read it differently is rewritten. A statement an
// apply sends was split, scanned and shown in Ptah's reading, so rewriting it
// is what makes the session run the statement that was checked and approved.
//
// One such setting exists. Ptah reads a MySQL-family string literal as the
// default sql_mode does, with a backslash starting an escape, and a session
// whose sql_mode names NO_BACKSLASH_ESCAPES reads the backslash as the
// character; [capability.NoBackslashEscapes] marks that session.
package sessiontext

import (
	"strings"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// For returns statement as a session with caps reads it, statement being text
// in Ptah's reading of dialect. Where caps carry
// [capability.NoBackslashEscapes], each string literal that holds a backslash
// is written with the characters it stands for and its quotes doubled, so
// `'a\\b'` is sent as `'a\b'` and the escaped quote in `'it\'s'` as two
// quotes. Everything else, and every statement for any other session, is
// returned byte for byte.
//
// The literals are found by the lexer that reads dialect, so a backslash in a
// comment or in a quoted identifier is not touched.
func For(dialect string, caps capability.Capabilities, statement string) string {
	if !caps.Has(capability.NoBackslashEscapes) || !strings.Contains(statement, `\`) {
		return statement
	}
	opts := dialectlexer.Options(dialect)
	tokens := lexer.NewLexerWithOptions(statement, opts)
	var b strings.Builder
	written := 0
	for token := tokens.NextToken(); token.Type != lexer.TokenEOF; token = tokens.NextToken() {
		text := statement[token.Start:token.End]
		quote := strings.IndexAny(text, `'"`)
		if token.Type != lexer.TokenString || quote < 0 || !strings.Contains(text, `\`) {
			continue
		}
		value, ok := lexer.StringValue(text[quote:], opts)
		if !ok {
			continue
		}
		mark := text[quote : quote+1]
		b.WriteString(statement[written:token.Start])
		b.WriteString(text[:quote])
		b.WriteString(mark + strings.ReplaceAll(value, mark, mark+mark) + mark)
		written = token.End
	}
	b.WriteString(statement[written:])
	return b.String()
}
