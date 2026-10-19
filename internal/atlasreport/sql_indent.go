package atlasreport

import (
	"strings"

	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// indentSQL adds presentation whitespace only outside quoted SQL tokens: the
// string literals and quoted identifiers that dialect's lexer reads. A newline
// inside one of them is part of a literal's value, a name or a routine body,
// so indenting it would change the statement rather than its layout.
//
// The statements are read as the dialect they were planned for, because the
// quoting rules differ by engine and a reading under another engine's rules
// moves the boundary of a literal. YQL reads `@@...@@` as a string, for
// example, so a T-SQL body that names two `@@` variables read as YQL would lose
// its indentation between them, and a YQL string read as T-SQL would gain it.
//
// Every report a command renders names its dialect. An empty or unknown one is
// read under the dialect-neutral rules [dialectlexer.Options] answers with.
func indentSQL(dialect, sql, indent string) string {
	protected := quotedNewlines(dialect, sql)
	var output strings.Builder
	output.WriteString(indent)
	for index := range len(sql) {
		output.WriteByte(sql[index])
		if sql[index] == '\n' && !protected[index] {
			output.WriteString(indent)
		}
	}
	return output.String()
}

// quotedNewlines marks the newlines of sql that dialect's lexer reads inside a
// string or a quoted identifier.
func quotedNewlines(dialect, sql string) []bool {
	protected := make([]bool, len(sql))
	lex := lexer.NewLexerWithOptions(sql, dialectlexer.Options(dialect))
	for {
		token := lex.NextToken()
		if token.Type == lexer.TokenEOF {
			return protected
		}
		// Backtick and bracket quoting produce identifiers; dollar-quoted
		// routine bodies and double-quoted identifiers produce strings.
		if token.Type != lexer.TokenString && token.Type != lexer.TokenIdentifier {
			continue
		}
		for index := token.Start; index < token.End; index++ {
			if sql[index] == '\n' {
				protected[index] = true
			}
		}
	}
}
