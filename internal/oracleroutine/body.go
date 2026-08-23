package oracleroutine

import "strings"

// Body takes the body out of the text ALL_SOURCE stored.
//
// ALL_SOURCE keeps the statement WITHOUT its `CREATE OR REPLACE` prefix, so
// the text starts at the header:
//
//	FUNCTION fn_double(p IN NUMBER) RETURN NUMBER IS
//	BEGIN
//	  RETURN p * 2;
//	END;
//
// The header ends at the first IS or AS standing at parenthesis depth zero --
// PL/SQL treats the two words as synonyms there -- and everything after it is
// the body. Depth is what keeps a parameter's own mode out of the way: `IN`
// never matches, but a default expression may contain anything, and it lives
// inside the parentheses.
//
// Taking the FIRST match rather than the last is the decision that matters. A
// body's own `AS` -- in `SELECT 1 AS ok` -- stands at depth zero too, and the
// last-match rule would hand back the tail of the body as the whole routine.
// The header has exactly one such word before the body starts, so first is
// exact.
//
// This is exact for a routine Ptah wrote, because the text the catalog hands
// back is Ptah's own rendering. For one written by someone else it is
// best-effort, and the comparator's answer for a miss is a body difference
// rather than a wrong statement.
func Body(source string) string {
	upper := strings.ToUpper(source)
	depth := 0
	for i := 0; i < len(source); i++ {
		switch source[i] {
		case '(':
			depth++
			continue
		case ')':
			depth--
			continue
		case '\'':
			i = skipQuoted(source, i, '\'')
			continue
		case '"':
			i = skipQuoted(source, i, '"')
			continue
		case '-':
			if strings.HasPrefix(source[i:], "--") {
				i = skipLineComment(source, i)
			}
			continue
		case '/':
			if strings.HasPrefix(source[i:], "/*") {
				i = skipBlockComment(source, i)
			}
			continue
		}
		if depth != 0 {
			continue
		}
		if i+2 > len(upper) {
			continue
		}
		word := upper[i : i+2]
		if word != "IS" && word != "AS" {
			continue
		}
		if !standaloneWord(source, i, i+2) {
			continue
		}
		return strings.TrimSpace(source[i+2:])
	}
	return strings.TrimSpace(source)
}

// skipQuoted returns the index of the closing quote of the literal or quoted
// identifier that starts at open, or the last index when it is unterminated.
// A doubled quote is an escape and does not close it.
func skipQuoted(source string, open int, quote byte) int {
	for i := open + 1; i < len(source); i++ {
		if source[i] != quote {
			continue
		}
		if i+1 < len(source) && source[i+1] == quote {
			i++
			continue
		}
		return i
	}
	return len(source) - 1
}

// skipLineComment returns the index of the newline that ends the comment, or
// the last index when the comment runs to the end of the text.
func skipLineComment(source string, start int) int {
	if end := strings.IndexByte(source[start:], '\n'); end >= 0 {
		return start + end
	}
	return len(source) - 1
}

// skipBlockComment returns the index of the last byte of the closing marker, or
// the last index when the comment is unterminated.
func skipBlockComment(source string, start int) int {
	if end := strings.Index(source[start+2:], "*/"); end >= 0 {
		return start + 2 + end + 1
	}
	return len(source) - 1
}

// standaloneWord reports whether the slice [start, end) is a whole word rather
// than part of a longer identifier.
func standaloneWord(source string, start, end int) bool {
	if start > 0 && isIdentifierByte(source[start-1]) {
		return false
	}
	if end < len(source) && isIdentifierByte(source[end]) {
		return false
	}
	return true
}

func isIdentifierByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9':
		return true
	case b == '_', b == '$', b == '#':
		return true
	default:
		return false
	}
}

// ParameterCarriesDefault reports whether a parameter list declares a default
// value.
//
// PL/SQL spells one two ways, `p IN NUMBER DEFAULT 1` and `p IN NUMBER := 1`,
// and the catalog reports neither: ALL_ARGUMENTS.DEFAULTED says 'Y' and the
// value appears nowhere. A routine created with one therefore reads back
// without it and is replanned on every run, which is why the renderer names the
// shape and creates nothing instead.
//
// Quoted text is skipped so that a default that is itself a string containing
// the word cannot be missed, and so a body is not searched -- this is given the
// parameter list only.
func ParameterCarriesDefault(parameters string) bool {
	upper := strings.ToUpper(parameters)
	for i := 0; i < len(parameters); i++ {
		switch parameters[i] {
		case '\'':
			i = skipQuoted(parameters, i, '\'')
			continue
		case '"':
			i = skipQuoted(parameters, i, '"')
			continue
		case ':':
			if strings.HasPrefix(parameters[i:], ":=") {
				return true
			}
			continue
		}
		if i+7 <= len(upper) && upper[i:i+7] == "DEFAULT" && standaloneWord(parameters, i, i+7) {
			return true
		}
	}
	return false
}
