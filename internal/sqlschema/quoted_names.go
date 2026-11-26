package sqlschema

import (
	"fmt"
	"strings"

	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlident"
)

// refuseUnreadableQuotedNames refuses data when its dialect reads a backslash
// inside a quoted name as an escape (descriptor
// Lexical.IdentifierBackslashEscapes, as on ClickHouse) and a quoted name in
// the document is not one [sqlident.ReadName] reads. Each such name is one the
// server refuses or one it reads differently by release: measured on
// ClickHouse 24.10 and 26.10, `a\x4` is refused with code 26, and \x before a
// byte that is not a hexadecimal digit decodes to a byte that differs between
// the two. Read by the SQL standard's quoting instead, `a\tb\xZZ` became a
// name with two backslashes in it, while the server creates the column with a
// tab, so a plan added the one and dropped the other, with its data.
//
// The check reads every token of the document before any statement is
// converted, so no part [foldedIdentifierPart] reads on such a dialect is a
// quoted name ReadName refuses. source names the document in the error.
func refuseUnreadableQuotedNames(data []byte, dialect, source string) error {
	if !factsOf(dialect).backslashNames {
		return nil
	}
	text := string(data)
	lex := lexer.NewLexerWithOptions(text, dialectlexer.Options(dialect))
	for token := lex.NextToken(); token.Type != lexer.TokenEOF; token = lex.NextToken() {
		if !quotedName(token) {
			continue
		}
		if _, ok := sqlident.ReadName(dialect, token.Value); !ok {
			return fmt.Errorf("%s: the quoted name %s is not a %s name Ptah reads as the server does: "+
				"inside quotes a backslash starts an escape, and this escape is incomplete, refused by the server, "+
				"or read differently by server release; write a backslash the name holds as two",
				directiveLocation(source, text, token.Start), token.Value, factsOf(dialect).name)
		}
	}
	return nil
}

// quotedName reports whether token is a backquoted or double-quoted name. The
// lexer reads a double-quoted one as a string, and a dialect that escapes a
// backslash in a name reads it as a name.
func quotedName(token lexer.Token) bool {
	switch token.Type {
	case lexer.TokenIdentifier:
		return strings.HasPrefix(token.Value, "`")
	case lexer.TokenString:
		return strings.HasPrefix(token.Value, `"`)
	default:
		return false
	}
}
