package defaultlit

import (
	"slices"
	"strings"
)

// RelativeTimeLiteral reports whether value, a default written as SQL on a
// column of columnType, is a string that literals names: a date and time
// input the engine reads as a moment relative to when it reads it, such as
// PostgreSQL's 'now'. literals is in upper case, as the dialect descriptor
// states it, and the string is matched without case and surrounding spaces.
//
// The string may carry a cast and parentheses, in either order: `'now'`,
// `'now'::timestamptz`, `('now')` and `('now')::timestamptz` are each the
// literal, because the engine folds each of them when it creates the table.
// Only a date or time column holds such a moment; on any other column, and
// for any value that is not one quoted string, the answer is false.
func RelativeTimeLiteral(columnType, value string, literals []string) bool {
	typeName := strings.ToLower(columnType)
	if len(literals) == 0 || (!strings.Contains(typeName, "date") && !strings.Contains(typeName, "time")) {
		return false
	}
	text := strings.TrimSpace(value)
	for {
		if enclosed(text) {
			text = strings.TrimSpace(text[1 : len(text)-1])
			continue
		}
		if cast := strings.LastIndex(text, "::"); cast > 0 && (text[cast-1] == '\'' || text[cast-1] == ')') && isTypeName(text[cast+2:]) {
			text = strings.TrimSpace(text[:cast])
			continue
		}
		break
	}
	if len(text) < 2 || text[0] != '\'' || text[len(text)-1] != '\'' || strings.Contains(text[1:len(text)-1], "'") {
		return false
	}
	return slices.Contains(literals, strings.ToUpper(strings.TrimSpace(text[1:len(text)-1])))
}

// enclosed reports whether text is one pair of parentheses around the rest,
// reading a quoted string as one token.
func enclosed(text string) bool {
	if len(text) < 2 || text[0] != '(' || text[len(text)-1] != ')' {
		return false
	}
	depth, quoted := 0, false
	for i := range len(text) {
		switch {
		case text[i] == '\'':
			quoted = !quoted
		case quoted:
		case text[i] == '(':
			depth++
		case text[i] == ')':
			depth--
			if depth == 0 && i < len(text)-1 {
				return false
			}
		}
	}
	return depth == 0 && !quoted
}

// isTypeName reports whether text can be the type a cast names, such as
// timestamptz, timestamp with time zone or numeric(10, 2), rather than more
// SQL after one.
func isTypeName(text string) bool {
	if strings.TrimSpace(text) == "" {
		return false
	}
	for _, ch := range []byte(text) {
		letter := ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9'
		if !letter && !strings.ContainsRune(" _(),[]\".", rune(ch)) {
			return false
		}
	}
	return true
}
