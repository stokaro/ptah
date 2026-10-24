package atlasreport

import (
	"strings"

	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// renderIndent is the indentation unit Ptah's renderers write inside a
// statement: the columns of a CREATE TABLE, for example, sit two spaces in.
const renderIndent = "  "

// indentSQL lays sql out as the `sql` template helper of the pinned community
// binary does when it is given an indent. That binary reads the indent as the
// unit a statement's inner lines are indented by, not as a prefix: a statement
// and the comment above it start at the first column, and the columns of a
// CREATE TABLE sit one indent in. Measured on v1.0.0 against PostgreSQL 15.19,
// `{{ sql . "\t" }}` writes
//
//	-- Create "owners" table
//	CREATE TABLE "owners" (
//	<TAB>"id" bigint NOT NULL,
//	...
//	);
//
// So each leading [renderIndent] of a line becomes indent, and nothing else
// moves. With indent equal to [renderIndent], which is the default and what an
// atlas.hcl `{{ sql . "  " }}` asks for, the layout is the renderer's own.
//
// Only lines that start outside a quoted SQL token are reindented: the string
// literals and quoted identifiers that dialect's lexer reads. A line that
// starts inside one is part of a literal's value, a name or a routine body,
// so moving it would change the statement rather than its layout.
//
// The statements are read as the dialect they were planned for, because the
// quoting rules differ by engine and a reading under another engine's rules
// moves the boundary of a literal. YQL reads `@@...@@` as a string, for
// example, so a T-SQL body that names two `@@` variables read as YQL would keep
// the layout between them, and a YQL string read as T-SQL would lose it.
//
// Every report a command renders names its dialect. An empty or unknown one is
// read under the dialect-neutral rules [dialectlexer.Options] answers with.
func indentSQL(dialect, sql, indent string) string {
	protected := quotedNewlines(dialect, sql)
	var output strings.Builder
	lineStart := true
	for index := 0; index < len(sql); {
		if lineStart {
			lineStart = false
			for strings.HasPrefix(sql[index:], renderIndent) {
				output.WriteString(indent)
				index += len(renderIndent)
			}
			continue
		}
		output.WriteByte(sql[index])
		if sql[index] == '\n' && !protected[index] {
			lineStart = true
		}
		index++
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
