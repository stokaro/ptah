package mssqlschema

import (
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Agreement is what an offline comparison of two spellings establishes.
type Agreement int

const (
	// Agree means the spellings denote the same thing.
	Agree Agreement = iota + 1
	// Differ means they denote different things.
	Differ
	// Undecided means SQL Server may store one as the other, and only the
	// server can tell.
	Undecided
)

// CompareArgument compares a declared predicate argument with the spelling
// the catalog reports for it, without a server.
//
// SQL Server stores a predicate's arguments rewritten. Measured on SQL Server
// 2025, sys.security_predicates reports `tenant_id` as `[tenant_id]`, `1` as
// `(1)`, `(tenant_id)` as `[tenant_id]`, `-tenant_id` as ` -[tenant_id]`, and
// `CAST(tenant AS int) + 0` as `CONVERT([int],[tenant])+(0)`. The comparison
// reads both spellings as T-SQL tokens, so brackets, white space and
// parentheses around a single token do not count, and then:
//
//   - two arguments that are one identifier or one string literal each Agree
//     when the tokens are equal and Differ otherwise, since the catalog keeps
//     an identifier's spelling and a string as written;
//   - two equal token sequences Agree;
//   - anything else is Undecided: the server rewrites expressions, and an
//     offline reading cannot tell a rewrite from a different expression.
//
// Identifiers compare by their exact spelling, which is how the catalog keeps
// them. A spelling the reader cannot tokenize, such as one holding a comment,
// is Undecided unless the two texts are equal.
func CompareArgument(declared, observed string) Agreement {
	if declared == observed {
		return Agree
	}
	left, leftOK := argumentTokens(declared)
	right, rightOK := argumentTokens(observed)
	switch {
	case !leftOK || !rightOK:
		return Undecided
	case slices.Equal(left, right):
		return Agree
	case len(left) == 1 && len(right) == 1 && left[0].settled() && right[0].settled():
		return Differ
	}
	return Undecided
}

// tokenKind classifies a T-SQL token for comparison.
type tokenKind int

const (
	identifierToken tokenKind = iota + 1
	keywordToken
	stringToken
	numberToken
	symbolToken
)

// token is one T-SQL token in comparable form: an identifier without its
// brackets or quotes, a keyword in upper case, a literal as written.
type token struct {
	kind  tokenKind
	value string
}

// settled reports whether a token on its own keeps its spelling in the
// catalog: an identifier, a keyword, a string literal, or an integer written
// without a leading zero. Another number may be respelled, such as a float
// literal with an exponent.
func (t token) settled() bool {
	switch t.kind {
	case identifierToken, keywordToken, stringToken:
		return true
	case numberToken:
		return t.value == "0" || t.value[0] != '0' && strings.Trim(t.value, "0123456789") == ""
	}
	return false
}

// keywords are the reserved words an argument expression may hold. A bare one
// is a keyword and a bracketed one an identifier, so `NULL` and `[NULL]`
// differ.
var keywords = []string{
	"ALL", "AND", "ANY", "AS", "BETWEEN", "CASE", "CAST", "COALESCE", "COLLATE", "CONVERT", "CURRENT_DATE",
	"CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "ELSE", "END", "ESCAPE", "EXISTS", "IN", "IS", "LEFT",
	"LIKE", "NOT", "NULL", "NULLIF", "OR", "RIGHT", "SELECT", "SESSION_USER", "SOME", "SYSTEM_USER", "THEN",
	"TRY_CONVERT", "USER", "WHEN",
}

// argumentTokens tokenizes one argument and removes the parentheses that do
// not change it: around the whole argument, and around a single token that is
// not a call's argument list.
func argumentTokens(text string) ([]token, bool) {
	tokens, ok := tokenize(text)
	if !ok || len(tokens) == 0 {
		return nil, false
	}
	for changed := true; changed; {
		changed = false
		for len(tokens) >= 2 && tokens[0] == (token{kind: symbolToken, value: "("}) && closing(tokens, 0) == len(tokens)-1 {
			tokens, changed = tokens[1:len(tokens)-1], true
		}
		for i := 0; i+2 < len(tokens); i++ {
			if tokens[i] != (token{kind: symbolToken, value: "("}) || tokens[i+2] != (token{kind: symbolToken, value: ")"}) ||
				tokens[i+1].kind == symbolToken || (i > 0 && callee(tokens[i-1])) {
				continue
			}
			tokens, changed = slices.Delete(slices.Delete(tokens, i+2, i+3), i, i+1), true
		}
	}
	return tokens, len(tokens) > 0
}

// callee reports whether a token before an opening parenthesis makes it an
// argument list or a list operand rather than grouping.
func callee(t token) bool {
	return t.kind == identifierToken || t.kind == keywordToken || t == token{kind: symbolToken, value: ")"}
}

// closing returns the index of the parenthesis that closes the one at open,
// or -1.
func closing(tokens []token, open int) int {
	depth := 0
	for i := open; i < len(tokens); i++ {
		switch tokens[i] {
		case token{kind: symbolToken, value: "("}:
			depth++
		case token{kind: symbolToken, value: ")"}:
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// tokenize splits T-SQL text into tokens. It refuses what it does not read:
// comments, unterminated quotes, and characters outside the expression
// grammar.
func tokenize(text string) ([]token, bool) {
	var tokens []token
	for i := 0; i < len(text); {
		r, size := utf8.DecodeRuneInString(text[i:])
		var next token
		var width int
		var ok bool
		switch {
		case unicode.IsSpace(r):
			i += size
			continue
		case r == '[':
			next, width, ok = delimited(text[i:], ']')
		case r == '"':
			next, width, ok = delimited(text[i:], '"')
		case r == '\'':
			next, width, ok = quotedString(text[i:], "")
		case (r == 'N' || r == 'n') && strings.HasPrefix(text[i+1:], "'"):
			next, width, ok = quotedString(text[i+1:], "N")
			width++
		case r >= '0' && r <= '9' || r == '.' && i+1 < len(text) && text[i+1] >= '0' && text[i+1] <= '9':
			next, width, ok = number(text[i:])
		case unicode.IsLetter(r) || r == '_' || r == '#':
			next, width, ok = word(text[i:])
		default:
			next, width, ok = symbol(text[i:])
		}
		if !ok {
			return nil, false
		}
		tokens = append(tokens, next)
		i += width
	}
	return tokens, true
}

// delimited reads a bracketed or double-quoted identifier, in which the
// closing character is escaped by doubling it.
func delimited(text string, closer byte) (token, int, bool) {
	var value strings.Builder
	for i := 1; i < len(text); i++ {
		if text[i] != closer {
			value.WriteByte(text[i])
			continue
		}
		if i+1 < len(text) && text[i+1] == closer {
			value.WriteByte(closer)
			i++
			continue
		}
		return token{kind: identifierToken, value: value.String()}, i + 1, true
	}
	return token{}, 0, false
}

// quotedString reads a string literal and keeps it as written, with prefix.
func quotedString(text, prefix string) (token, int, bool) {
	_, width, ok := delimited(text, '\'')
	if !ok {
		return token{}, 0, false
	}
	return token{kind: stringToken, value: prefix + text[:width]}, width, true
}

// number reads a numeric or binary literal as written.
func number(text string) (token, int, bool) {
	if strings.HasPrefix(strings.ToLower(text), "0x") {
		end := skip(text, 2, "0123456789abcdefABCDEF")
		return token{kind: numberToken, value: strings.ToLower(text[:end])}, end, true
	}
	end := skip(text, 0, "0123456789.")
	if end < len(text) && (text[end] == 'e' || text[end] == 'E') {
		exponent := end + 1
		if exponent < len(text) && (text[exponent] == '+' || text[exponent] == '-') {
			exponent++
		}
		if digits := skip(text, exponent, "0123456789"); digits > exponent {
			end = digits
		}
	}
	return token{kind: numberToken, value: text[:end]}, end, true
}

// skip returns the index of the first byte of text at or after start that is
// not in set.
func skip(text string, start int, set string) int {
	for start < len(text) && strings.IndexByte(set, text[start]) >= 0 {
		start++
	}
	return start
}

// word reads a regular identifier or a keyword.
func word(text string) (token, int, bool) {
	end := len(text)
	for i, r := range text {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && !strings.ContainsRune("_@#$", r) {
			end = i
			break
		}
	}
	value := text[:end]
	if upper := strings.ToUpper(value); slices.Contains(keywords, upper) {
		return token{kind: keywordToken, value: upper}, end, true
	}
	return token{kind: identifierToken, value: value}, end, true
}

// symbol reads an operator or punctuation. A comment start is refused.
func symbol(text string) (token, int, bool) {
	for _, pair := range []string{"<=", ">=", "<>", "!=", "!<", "!>"} {
		if strings.HasPrefix(text, pair) {
			return token{kind: symbolToken, value: pair}, 2, true
		}
	}
	if strings.HasPrefix(text, "--") || strings.HasPrefix(text, "/*") {
		return token{}, 0, false
	}
	if strings.ContainsRune("+-*/%&|^~=<>,.()", rune(text[0])) {
		return token{kind: symbolToken, value: text[:1]}, 1, true
	}
	return token{}, 0, false
}

// SimpleArgument reports whether an argument is one identifier or one
// literal, so it reads at most a column of its predicate's table and calls
// nothing. An expression may call functions or name types of its own.
func SimpleArgument(argument string) bool {
	tokens, ok := argumentTokens(argument)
	return ok && len(tokens) == 1 && tokens[0].kind != keywordToken
}

// ArgumentColumn returns the column an argument names when it is one
// identifier, without brackets or quotes, and false for a literal or an
// expression.
func ArgumentColumn(argument string) (string, bool) {
	tokens, ok := argumentTokens(argument)
	if !ok || len(tokens) != 1 || tokens[0].kind != identifierToken {
		return "", false
	}
	return tokens[0].value, true
}
