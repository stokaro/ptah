// Package routinebody tells a PostgreSQL routine body written as SQL from one
// written as a string.
//
// PostgreSQL 14 and later accept a SQL-standard body for a LANGUAGE sql
// routine, `BEGIN ATOMIC ... END` or `RETURN expression`. The server parses
// it when the routine is created, records what it reads, keeps it parsed in
// pg_proc.prosqlbody with prosrc empty, and prints it back with
// pg_get_function_sqlbody in its own spelling. Such a body is written after
// the routine's attributes, never inside dollar quotes. The renderer, the
// comparison's server probe and the PostgreSQL planner's drop ordering all
// ask the same question about a body, and they ask it here.
package routinebody

import (
	"strings"
	"unicode"
)

// Kind is the form a routine's body is written in.
type Kind int

const (
	// Quoted is a body written as a string, inside dollar quotes or a literal.
	Quoted Kind = iota
	// Atomic is a SQL-standard `BEGIN ATOMIC ... END` body.
	Atomic
	// Return is a SQL-standard `RETURN expression` body.
	Return
)

// Of is the form of body for a routine written in language. Only a LANGUAGE
// sql routine has a SQL-standard body; any other language keeps its body as a
// string. A routine that names no language has one when its body opens with
// BEGIN ATOMIC, which PostgreSQL takes as LANGUAGE sql. Keywords compare
// without case, and a body is read from its first word: `BEGIN ATOMIC` opens
// an atomic body, and `RETURN` followed by white space or a parenthesis a
// return body. A bare `BEGIN`, as a MySQL or PL/pgSQL block opens, is not one.
func Of(language, body string) Kind {
	language = strings.TrimSpace(language)
	sql := strings.EqualFold(language, "sql")
	if !sql && language != "" {
		return Quoted
	}
	text := strings.TrimSpace(body)
	if rest, ok := cutKeyword(text, "BEGIN"); ok {
		if _, atomic := cutKeyword(strings.TrimLeftFunc(rest, unicode.IsSpace), "ATOMIC"); atomic {
			return Atomic
		}
		return Quoted
	}
	if _, ok := cutKeyword(text, "RETURN"); ok && sql {
		return Return
	}
	return Quoted
}

// SQLStandard reports whether body, of a routine written in language, is a
// SQL-standard body; see [Of].
func SQLStandard(language, body string) bool {
	return Of(language, body) != Quoted
}

// ReturnExpression is the expression of a `RETURN expression` body, without
// the keyword, and body itself for any other form.
func ReturnExpression(body string) string {
	text := strings.TrimSpace(body)
	if rest, ok := cutKeyword(text, "RETURN"); ok {
		return strings.TrimSpace(rest)
	}
	return text
}

// cutKeyword reports whether text starts with keyword, in any case, followed
// by the end of the text, white space or an opening parenthesis, and returns
// what follows the keyword.
func cutKeyword(text, keyword string) (string, bool) {
	if len(text) < len(keyword) || !strings.EqualFold(text[:len(keyword)], keyword) {
		return "", false
	}
	rest := text[len(keyword):]
	if rest == "" || rest[0] == '(' || unicode.IsSpace(rune(rest[0])) {
		return rest, true
	}
	return "", false
}
