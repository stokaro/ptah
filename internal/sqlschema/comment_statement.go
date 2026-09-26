package sqlschema

import (
	"strings"
	"unicode"
)

// commentStatement is one COMMENT ON statement as Parser.parseCommentStatement
// writes it: `COMMENT ON <target> IS <literal>`, with the target kept as the
// file spelled it. This splits the target into the parts the objects Ptah
// models are addressed by.
type commentStatement struct {
	// kind is the object kind in upper case, one word or several:
	// `TABLE`, `MATERIALIZED VIEW`.
	kind string
	// name is the object's name as written, qualified where it was written so.
	name string
	// arguments is a routine's argument list, and nil where none was written.
	arguments *string
	// table is the table a trigger, a policy or a constraint is named ON, or
	// the domain a constraint is named ON DOMAIN.
	table string
	// onDomain reports `CONSTRAINT ... ON DOMAIN`.
	onDomain bool
	// comment is the text of the literal.
	comment string
}

// multiWordCommentKinds are the kinds COMMENT ON spells in more than one word.
// Every other kind is the target's first word. The list covers every kind
// PostgreSQL 18 documents, so a kind Ptah does not model is still read whole
// and refused by its name rather than by its first word.
var multiWordCommentKinds = []string{
	"MATERIALIZED VIEW", "FOREIGN TABLE", "FOREIGN DATA WRAPPER", "EVENT TRIGGER", "ACCESS METHOD",
	"OPERATOR CLASS", "OPERATOR FAMILY", "PROCEDURAL LANGUAGE", "LARGE OBJECT", "TRANSFORM FOR",
	"TEXT SEARCH CONFIGURATION", "TEXT SEARCH DICTIONARY", "TEXT SEARCH PARSER", "TEXT SEARCH TEMPLATE",
}

// parseCommentStatement splits a COMMENT ON text into its parts. It answers
// false for a text it cannot read, which the caller refuses rather than
// ignores.
func parseCommentStatement(text string) (commentStatement, bool) {
	rest, found := strings.CutPrefix(text, "COMMENT ON ")
	if !found {
		return commentStatement{}, false
	}
	target, literal, found := cutCommentLiteral(rest)
	if !found {
		return commentStatement{}, false
	}
	statement := commentStatement{comment: unquoteSQLStringLiteral(literal)}
	statement.kind, rest = cutCommentKind(target)
	statement.name, rest = cutQualifiedName(rest)
	if strings.HasPrefix(rest, "(") {
		arguments, remainder, closed := cutParenthesized(rest)
		if !closed {
			return commentStatement{}, false
		}
		statement.arguments = &arguments
		rest = strings.TrimSpace(remainder)
	}
	if on, remainder := cutWord(rest); strings.EqualFold(on, "ON") {
		if domain, afterDomain := cutWord(remainder); strings.EqualFold(domain, "DOMAIN") {
			statement.onDomain = true
			remainder = afterDomain
		}
		statement.table, rest = cutQualifiedName(remainder)
	}
	if statement.kind == "" || statement.name == "" || rest != "" {
		// A form this reader does not split, such as CAST (a AS b): the kind
		// is still known, and a kind Ptah models is never written this way.
		statement.name = strings.TrimSpace(strings.TrimPrefix(target, statement.kind))
	}
	return statement, true
}

// cutCommentLiteral splits the text after COMMENT ON at the IS that precedes
// the literal: the first one outside double quotes and parentheses and
// followed by a string. What follows the literal's opening quote cannot end
// the target, so a literal holding the word IS is read whole.
func cutCommentLiteral(text string) (target, literal string, found bool) {
	depth, quoted := 0, false
	for i := 0; i < len(text); i++ {
		switch {
		case text[i] == '"':
			quoted = !quoted
		case quoted:
		case text[i] == '(':
			depth++
		case text[i] == ')':
			depth--
		case depth == 0 && isSpace(text[i]) && i+3 < len(text) && strings.EqualFold(text[i+1:i+3], "IS") &&
			isSpace(text[i+3]):
			literal = strings.TrimSpace(text[i+3:])
			if strings.HasPrefix(literal, "'") {
				return strings.TrimSpace(text[:i]), literal, true
			}
		}
	}
	return "", "", false
}

// cutCommentKind reads the kind at the start of a target, in upper case, and
// returns what follows it. The words of a multi-word kind may be separated by
// any whitespace, as the statement allows.
func cutCommentKind(target string) (kind, rest string) {
	for _, candidate := range multiWordCommentKinds {
		rest, matched := target, true
		for want := range strings.FieldsSeq(candidate) {
			var word string
			word, rest = cutWord(rest)
			if !strings.EqualFold(word, want) {
				matched = false
				break
			}
		}
		if matched {
			return candidate, rest
		}
	}
	word, rest := cutWord(target)
	return strings.ToUpper(word), rest
}

// cutWord returns the first whitespace-separated word and what follows it.
func cutWord(text string) (word, rest string) {
	text = strings.TrimSpace(text)
	end := strings.IndexFunc(text, unicode.IsSpace)
	if end < 0 {
		return text, ""
	}
	return text[:end], strings.TrimSpace(text[end:])
}

// cutQualifiedName reads a name of one or more dot-separated parts, each bare
// or double-quoted, and returns it with what follows it. It stops at
// whitespace or an opening parenthesis outside quotes.
func cutQualifiedName(text string) (name, rest string) {
	text = strings.TrimSpace(text)
	quoted := false
	for i := 0; i < len(text); i++ {
		switch {
		case text[i] == '"':
			quoted = !quoted
		case quoted:
		case isSpace(text[i]) || text[i] == '(':
			return text[:i], strings.TrimSpace(text[i:])
		}
	}
	return text, ""
}

// cutParenthesized reads the balanced parenthesized group text starts with
// and returns its contents and what follows it.
func cutParenthesized(text string) (inner, rest string, closed bool) {
	depth, quoted := 0, false
	for i := 0; i < len(text); i++ {
		switch {
		case text[i] == '"':
			quoted = !quoted
		case quoted:
		case text[i] == '(':
			depth++
		case text[i] == ')':
			depth--
			if depth == 0 {
				return strings.TrimSpace(text[1:i]), text[i+1:], true
			}
		}
	}
	return "", "", false
}

func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}
