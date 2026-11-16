// Package pgregclass reads the regclass constants of PostgreSQL text: the
// relations, such as sequences, that an expression or a statement names as a
// string constant the server converts to a relation when it reads the text.
//
// PostgreSQL writes a stored column default back with each such name as a
// string constant cast to regclass, `nextval('s'::regclass)`, and records a
// dependency of the default on the relation. That dependency is what `DROP
// SEQUENCE s CASCADE` follows when it removes the default. A name passed as
// text, `nextval('s'::text)`, is resolved when the expression runs, carries no
// dependency, and is not a constant here.
package pgregclass

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// Constants returns the text of each regclass constant in expression, in the
// order they appear: s and app.s for `nextval('s'::regclass) +
// currval('app.s'::regclass)`. The text is the relation name exactly as the
// constant spells it, double quotes included, so `'"Seq"'::regclass` gives
// `"Seq"`. An expression without one, and one the lexer cannot read, gives
// none.
func Constants(expression string) []string {
	opts := dialectlexer.Options(platform.Postgres)
	tokens := codeTokens(expression, opts)
	var constants []string
	for i := range tokens {
		if !castToRegclass(tokens, i) {
			continue
		}
		if text, ok := lexer.StringValue(tokens[i].Value, opts); ok && text != "" {
			constants = append(constants, text)
		}
	}
	return constants
}

// Relation is a relation a regclass constant names, as the server looks it
// up.
type Relation struct {
	// Schema is the schema the constant qualifies the name with. Empty means
	// the constant names the relation alone, and the server finds it along
	// the session's search_path.
	Schema string
	// Name is the relation's own name.
	Name string
}

// Relations returns the relations text names through regclass constants, in
// the order they appear, read with dialect's lexical rules. Such a constant is
// one the server converts to regclass as it reads the statement, so it looks
// the relation up then and refuses the statement when no relation has the
// name:
//
//   - a string constant cast to regclass, as in 'app.ids'::regclass and
//     CAST('app.ids' AS regclass);
//   - the first argument of nextval, currval or setval written as a string
//     constant alone, as in nextval('app.ids'), since that parameter is a
//     regclass. The function may be qualified by pg_catalog and by no other
//     schema.
//
// The constant's text is read as the server reads a relation's name: split
// into parts at each dot outside double quotes, an unquoted part folded to
// lower case, and a quoted part kept as written with its doubled quotes
// undone. A name of three parts is database.schema.relation, and the database
// is left out.
//
// Nothing else gives a relation: a constant cast to another type, such as
// nextval('app.ids'::text), whose name the server looks up only when the
// expression runs; a name computed from strings; a string in any other place,
// such as DEFAULT 'app.ids'; text in a comment; an OID written in digits; and
// a constant that is not a valid name.
func Relations(text, dialect string) []Relation {
	opts := dialectlexer.Options(dialect)
	tokens := codeTokens(text, opts)
	var relations []Relation
	for i := range tokens {
		if !castToRegclass(tokens, i) && !castByName(tokens, i) && !sequenceArgument(tokens, i) {
			continue
		}
		value, ok := lexer.StringValue(tokens[i].Value, opts)
		if !ok {
			continue
		}
		if relation, ok := relationName(value); ok {
			relations = append(relations, relation)
		}
	}
	return relations
}

// codeTokens returns the tokens of text but white space and comments.
func codeTokens(text string, opts lexer.Options) []lexer.Token {
	scanner := lexer.NewLexerWithOptions(text, opts)
	var tokens []lexer.Token
	for {
		token := scanner.NextToken()
		if token.Type == lexer.TokenEOF {
			return tokens
		}
		if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenComment {
			tokens = append(tokens, token)
		}
	}
}

// isConstant reports whether token is a string constant. The lexer reads a
// double-quoted name as a string too, and that is a name, not a constant.
func isConstant(token lexer.Token) bool {
	return token.Type == lexer.TokenString && !strings.HasPrefix(token.Value, `"`)
}

// word is token as a name: an unquoted name folded to lower case, as the
// server folds it, and a double-quoted one as written. Any other token is no
// name, and gives "".
func word(token lexer.Token) string {
	switch {
	case token.Type == lexer.TokenIdentifier:
		return foldASCII(token.Value)
	case token.Type == lexer.TokenString && strings.HasPrefix(token.Value, `"`) && len(token.Value) >= 2:
		return strings.ReplaceAll(token.Value[1:len(token.Value)-1], `""`, `"`)
	default:
		return ""
	}
}

// isOperator reports whether token is the operator op.
func isOperator(token lexer.Token, op string) bool {
	return token.MatchOperatorValue(op)
}

// at returns tokens[i], or the zero token when i is out of range.
func at(tokens []lexer.Token, i int) lexer.Token {
	if i < 0 || i >= len(tokens) {
		return lexer.Token{}
	}
	return tokens[i]
}

// castToRegclass reports whether tokens[i] is a string constant cast to
// regclass with ::. The lexer reads `::` as two operators, and only the two
// written together are the cast.
func castToRegclass(tokens []lexer.Token, i int) bool {
	colon, second := at(tokens, i+1), at(tokens, i+2)
	_, typed := regclassEnd(tokens, i+3)
	return isConstant(at(tokens, i)) && colon.MatchOperatorValue(":") && second.MatchOperatorValue(":") &&
		second.Start == colon.End && typed
}

// castByName reports whether tokens[i] is the string constant of
// CAST(... AS regclass).
func castByName(tokens []lexer.Token, i int) bool {
	if !isConstant(at(tokens, i)) || !isOperator(at(tokens, i-1), "(") || word(at(tokens, i-2)) != "cast" ||
		word(at(tokens, i+1)) != "as" {
		return false
	}
	end, typed := regclassEnd(tokens, i+2)
	return typed && isOperator(at(tokens, end), ")")
}

// regclassEnd reports whether tokens spell the type regclass from i, alone or
// qualified by pg_catalog, and returns the index after it.
func regclassEnd(tokens []lexer.Token, i int) (int, bool) {
	if word(at(tokens, i)) == "pg_catalog" && isOperator(at(tokens, i+1), ".") {
		i += 2
	}
	return i + 1, word(at(tokens, i)) == "regclass"
}

// sequenceArgument reports whether tokens[i] is a string constant that is
// the whole first argument of nextval, currval or setval: an opening
// parenthesis before it, behind the function's name, and a closing
// parenthesis or a comma after it.
func sequenceArgument(tokens []lexer.Token, i int) bool {
	if !isConstant(at(tokens, i)) || !isOperator(at(tokens, i-1), "(") {
		return false
	}
	if next := at(tokens, i+1); !isOperator(next, ")") && !isOperator(next, ",") {
		return false
	}
	switch word(at(tokens, i-2)) {
	case "nextval", "currval", "setval":
	default:
		return false
	}
	return !isOperator(at(tokens, i-3), ".") || word(at(tokens, i-4)) == "pg_catalog"
}

// relationName reads text as the server reads the name in a regclass
// constant: "-" and a string of digits are an OID, not a name, and a name has
// one part, schema.relation, or database.schema.relation.
func relationName(text string) (Relation, bool) {
	if text == "-" || isDigits(text) {
		return Relation{}, false
	}
	parts, ok := nameParts(text)
	if !ok {
		return Relation{}, false
	}
	switch len(parts) {
	case 1:
		return Relation{Name: parts[0]}, true
	case 2:
		return Relation{Schema: parts[0], Name: parts[1]}, true
	case 3:
		return Relation{Schema: parts[1], Name: parts[2]}, true
	default:
		return Relation{}, false
	}
}

// nameParts splits text into the parts of a qualified name as PostgreSQL's
// SplitIdentifierString does: white space around each part is skipped, a
// part in double quotes keeps its case with each doubled quote read as one,
// an unquoted part runs to the next dot or white space and folds to lower
// case, and parts are separated by dots. Text with an empty part, an
// unterminated quote or anything else between parts is no name; the server
// refuses all but an empty quoted part, which names no relation either.
func nameParts(text string) ([]string, bool) {
	var parts []string
	rest := strings.TrimLeft(text, nameSpace)
	if rest == "" {
		return nil, false
	}
	for {
		read := unquotedPart
		if strings.HasPrefix(rest, `"`) {
			read = quotedPart
		}
		part, after, ok := read(rest)
		if !ok {
			return nil, false
		}
		parts = append(parts, part)
		rest = strings.TrimLeft(after, nameSpace)
		switch {
		case rest == "":
			return parts, true
		case rest[0] == '.':
			rest = strings.TrimLeft(rest[1:], nameSpace)
		default:
			return nil, false
		}
	}
}

// quotedPart reads the double-quoted part rest starts with and returns it with
// each doubled quote read as one, and the text after its closing quote. An
// unterminated quote and an empty part are no part.
func quotedPart(rest string) (part, after string, ok bool) {
	var b strings.Builder
	rest = rest[1:]
	for {
		end := strings.IndexByte(rest, '"')
		if end < 0 {
			return "", "", false
		}
		b.WriteString(rest[:end])
		rest = rest[end+1:]
		if !strings.HasPrefix(rest, `"`) {
			return b.String(), rest, b.Len() > 0
		}
		b.WriteByte('"')
		rest = rest[1:]
	}
}

// unquotedPart reads the unquoted part rest starts with, up to the next dot or
// white space, and returns it folded to lower case with the text after it.
// Text that starts with a dot is no part.
func unquotedPart(rest string) (part, after string, ok bool) {
	end := strings.IndexAny(rest, "."+nameSpace)
	if end < 0 {
		end = len(rest)
	}
	return foldASCII(rest[:end]), rest[end:], end > 0
}

// nameSpace are the bytes PostgreSQL's scanner_isspace reads as white space.
const nameSpace = " \t\n\r\f\v"

// foldASCII folds the ASCII letters of name to lower case and leaves every
// other byte as it is, as the server folds an unquoted name in a UTF-8
// database.
func foldASCII(name string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return r
	}, name)
}

func isDigits(text string) bool {
	return text != "" && strings.Trim(text, "0123456789") == ""
}
