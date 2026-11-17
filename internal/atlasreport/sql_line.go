package atlasreport

import (
	"strings"

	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// oneLine writes statement on one line, as the pinned community binary holds it
// in a change's `.Cmd` and writes it through the `sql` template helper given no
// indent. Measured on v1.3.0 against PostgreSQL 18, `migrate diff` without
// --format, with `{{ sql . }}` and with `{{ sql . "" }}`, and `schema diff`
// without --format each write
//
//	CREATE TABLE "u" ("id" integer NOT NULL, PRIMARY KEY ("id"), CONSTRAINT "u_fk" FOREIGN KEY ...);
//	ALTER TABLE "c" ADD CONSTRAINT "c_fk" FOREIGN KEY ...;
//
// where `{{ sql . "  " }}` breaks the first after its opening parenthesis and
// after each comma, and the second after ADD. So a line break and the
// indentation after it become one space, and nothing after an opening
// parenthesis or before a closing one.
//
// Only the statements that binary writes over several lines are joined: a
// table's creation and a change to a table. A view, a routine, a trigger or a
// declared row is a statement only Ptah plans, and some engines keep a body as
// it is written, so it keeps its layout.
//
// A line break inside a string literal, a quoted identifier or a dollar-quoted
// body is part of the value and stays, and so does the one that ends a line
// comment, which would otherwise take the rest of the statement into the
// comment. The statement is read by the quoting rules of dialect, as
// [indentSQL] reads it.
func oneLine(dialect, statement string) string {
	if !strings.Contains(statement, "\n") {
		return statement
	}
	kept, joined := lineBreaksKept(dialect, statement)
	if !joined {
		return statement
	}
	var out strings.Builder
	var last byte
	for at := 0; at < len(statement); {
		char := statement[at]
		if char != '\n' || kept[at] {
			out.WriteByte(char)
			last = char
			at++
			continue
		}
		next := at + 1
		for next < len(statement) && isLayoutSpace(statement[next]) && !kept[next] {
			next++
		}
		if next == len(statement) {
			out.WriteString(statement[at:])
			break
		}
		if last != '(' && statement[next] != ')' {
			out.WriteByte(' ')
			last = ' '
		}
		at = next
	}
	return out.String()
}

// lineBreaksKept marks the line breaks of statement that [oneLine] keeps, and
// reports whether statement is one it joins at all.
func lineBreaksKept(dialect, statement string) (kept []bool, joined bool) {
	kept = make([]bool, len(statement))
	lex := lexer.NewLexerWithOptions(statement, dialectlexer.Options(dialect))
	var head []string
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return kept, joinsStatement(head)
		case lexer.TokenWhitespace:
			continue
		case lexer.TokenString, lexer.TokenIdentifier, lexer.TokenComment:
			for at := token.Start; at < token.End; at++ {
				if statement[at] == '\n' {
					kept[at] = true
				}
			}
			if token.Type == lexer.TokenComment && token.End < len(statement) && statement[token.End] == '\n' {
				kept[token.End] = true
			}
		}
		if token.Type != lexer.TokenComment && len(head) < 2 {
			head = append(head, strings.ToUpper(token.Value))
		}
	}
}

// joinsStatement reports whether a statement that starts with head is one
// [oneLine] joins.
func joinsStatement(head []string) bool {
	if len(head) < 2 {
		return false
	}
	switch head[0] + " " + head[1] {
	case "CREATE TABLE", "ALTER TABLE":
		return true
	default:
		return false
	}
}

// isLayoutSpace reports whether char is whitespace a renderer writes between
// the parts of a statement.
func isLayoutSpace(char byte) bool {
	return char == ' ' || char == '\t' || char == '\r' || char == '\n'
}
