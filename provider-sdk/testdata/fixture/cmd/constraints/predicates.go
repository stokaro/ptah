package main

import (
	"regexp"
	"strings"
)

// heldPredicate finds the predicate of a partial index in the statement
// SQLite keeps for it: what follows WHERE after the key list.
var heldPredicate = regexp.MustCompile(`(?is)\)\s+WHERE\s+(.*)$`)

// indexPredicate is the predicate of the index SQLite keeps as statement,
// without WHERE and surrounding white space, or empty for a full index.
func indexPredicate(statement string) string {
	match := heldPredicate.FindStringSubmatch(statement)
	if match == nil {
		return ""
	}
	return strings.TrimSpace(match[1])
}

// predicateSpelling is the fixture's spelling of a predicate, the one it
// builds an index with and so reads back: every bare number quoted, as a
// string literal SQLite compares with an INTEGER column as the number, and
// the whole in one pair of parentheses. It stands for a server that keeps a
// predicate in a form of its own: `score > 0` is held as `(score > '0')`,
// which the host's shared comparison of predicates reads as another one. A
// predicate already in the spelling keeps it.
func predicateSpelling(predicate string) string {
	text := strings.TrimSpace(predicate)
	for strings.HasPrefix(text, "(") && strings.HasSuffix(text, ")") && balanced(text[1:len(text)-1]) {
		text = strings.TrimSpace(text[1 : len(text)-1])
	}
	var spelled strings.Builder
	quoted := false
	for i := 0; i < len(text); i++ {
		c := text[i]
		if c == '\'' {
			quoted = !quoted
		}
		if quoted || !isDigit(c) || i > 0 && isNameByte(text[i-1]) {
			spelled.WriteByte(c)
			continue
		}
		end := i
		for end < len(text) && (isDigit(text[end]) || text[end] == '.') {
			end++
		}
		if end < len(text) && isNameByte(text[end]) {
			spelled.WriteString(text[i:end])
		} else {
			spelled.WriteString("'" + text[i:end] + "'")
		}
		i = end - 1
	}
	return "(" + spelled.String() + ")"
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// isNameByte reports whether c continues a name, so a digit after it is part
// of the name rather than a number.
func isNameByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || isDigit(c) || c == '_'
}
