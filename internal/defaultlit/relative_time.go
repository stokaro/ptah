package defaultlit

import (
	"slices"
	"strings"
)

// RelativeTimeLiteral reports whether value, a default written as SQL, is a
// string that literals names: a date and time input the engine reads as a
// moment relative to when it reads it, such as PostgreSQL's 'now'. literals is
// in upper case, as the dialect descriptor states it, and the string is
// matched without case and surrounding spaces.
//
// The string may carry a cast and parentheses, in either order: `'now'`,
// `'now'::timestamptz`, `('now')` and `('now')::timestamptz` are each the
// literal, because the engine folds each of them when it creates the table.
// For any value that is not one quoted string the answer is false. Only a
// column that holds a moment reads the string as one, so a caller asks
// [DateTimeType] about the column first.
func RelativeTimeLiteral(value string, literals []string) bool {
	if len(literals) == 0 {
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

// dateTimeTypes are the built-in date and time types of the PostgreSQL
// family, the engines whose descriptors name relative time literals, as a
// column spells them without a precision.
var dateTimeTypes = map[string]bool{
	"date": true, "time": true, "timetz": true, "time without time zone": true, "time with time zone": true,
	"timestamp": true, "timestamptz": true, "timestamp without time zone": true, "timestamp with time zone": true,
}

// DateTimeType reports whether spelling names a built-in date or time type of
// the PostgreSQL family, without regard to case, a pg_catalog qualifier or a
// precision: `TIMESTAMPTZ`, `timestamp(3) with time zone` and
// `pg_catalog.date` each do. Any other name answers false, an array and a type
// somebody created included: resolving a created type, such as a domain, is
// the caller's. An enum named update_kind holds no moment, whatever its name
// says.
func DateTimeType(spelling string) bool {
	name := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(spelling)), "pg_catalog.")
	if open := strings.IndexByte(name, '('); open >= 0 {
		end := strings.IndexByte(name[open:], ')')
		if end < 0 {
			return false
		}
		name = name[:open] + " " + name[open+end+1:]
	}
	return dateTimeTypes[strings.Join(strings.Fields(name), " ")]
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
