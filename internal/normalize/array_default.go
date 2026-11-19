package normalize

import (
	"strings"
)

// arrayItem is one element of a one-dimensional array default: its text, or
// NULL.
type arrayItem struct {
	text string
	null bool
}

// arrayDefault is value written as PostgreSQL prints an array, as in
// `{a,"b c",NULL}`, where columnType is an array type and value is a quoted
// array literal or an ARRAY constructor whose elements are numbers, strings
// or NULL. CockroachDB stores `DEFAULT '{}'` and `DEFAULT ARRAY[]::text[]` as
// `ARRAY[]`, and `DEFAULT '{a,b}'` as `ARRAY['a', 'b']`, where PostgreSQL
// keeps the literal, so the two spellings of one array read as one default.
//
// ok is false for anything else, which is compared as written: a column that
// holds no array, a nested array, an element that is an expression, and a
// string cast to a type other than the element's.
func arrayDefault(value, columnType string) (canonical string, ok bool) {
	element, isArray := ArrayElement(columnType)
	if !isArray {
		return "", false
	}
	value = strings.TrimSpace(value)
	var items []arrayItem
	switch {
	case isPlainString(value):
		items, ok = arrayTextItems(plainStringValue(value))
	case len(value) > len("ARRAY") && strings.EqualFold(value[:len("ARRAY")], "ARRAY"):
		items, ok = arrayConstructorItems(strings.TrimSpace(value[len("ARRAY"):]), element)
	}
	if !ok {
		return "", false
	}
	return arrayText(items), true
}

// isPlainString reports that value is one single-quoted string with no
// prefix.
func isPlainString(value string) bool {
	return strings.HasPrefix(value, "'") && isQuotedDefaultLiteral(value)
}

// plainStringValue is the characters a plain single-quoted string holds: the
// text between its quotes, with each doubled quote read as one.
func plainStringValue(value string) string {
	return strings.ReplaceAll(value[1:len(value)-1], "''", "'")
}

// arrayTextItems reads the text of a one-dimensional array, as `{a,"b c"}`.
// An element in double quotes keeps its spaces and takes a backslash escape;
// an element without quotes loses the spaces around it, and NULL there is no
// value. A nested array, an empty element and a missing brace are not read.
func arrayTextItems(text string) ([]arrayItem, bool) {
	text = strings.TrimSpace(text)
	if len(text) < 2 || text[0] != '{' || text[len(text)-1] != '}' {
		return nil, false
	}
	body := strings.TrimSpace(text[1 : len(text)-1])
	if body == "" {
		return nil, true
	}
	var items []arrayItem
	for at := 0; ; {
		item, next, ok := arrayTextItem(body, at)
		if !ok {
			return nil, false
		}
		items = append(items, item)
		next = skipSpaces(body, next)
		if next == len(body) {
			return items, true
		}
		if body[next] != ',' {
			return nil, false
		}
		at = next + 1
	}
}

// arrayTextItem reads the element of an array's text that begins at or after
// at, and returns where it ends.
func arrayTextItem(body string, at int) (arrayItem, int, bool) {
	at = skipSpaces(body, at)
	if at < len(body) && body[at] == '"' {
		var text strings.Builder
		for i := at + 1; i < len(body); i++ {
			switch body[i] {
			case '\\':
				if i+1 == len(body) {
					return arrayItem{}, 0, false
				}
				i++
				text.WriteByte(body[i])
			case '"':
				return arrayItem{text: text.String()}, i + 1, true
			default:
				text.WriteByte(body[i])
			}
		}
		return arrayItem{}, 0, false
	}
	end := at
	for end < len(body) && body[end] != ',' {
		if strings.IndexByte(`{}"\`, body[end]) >= 0 {
			return arrayItem{}, 0, false
		}
		end++
	}
	text := strings.TrimSpace(body[at:end])
	if text == "" {
		return arrayItem{}, 0, false
	}
	return arrayItem{text: text, null: strings.EqualFold(text, "NULL")}, end, true
}

// arrayConstructorItems reads the elements of an ARRAY constructor, given the
// text after the keyword, as `['a', 'b']`. An element is a number, NULL, or a
// plain string, alone or cast to element, the array's element type.
func arrayConstructorItems(text, element string) ([]arrayItem, bool) {
	if len(text) < 2 || text[0] != '[' || text[len(text)-1] != ']' {
		return nil, false
	}
	body := strings.TrimSpace(text[1 : len(text)-1])
	if body == "" {
		return nil, true
	}
	var items []arrayItem
	for _, part := range splitOutsideQuotes(body) {
		item, ok := arrayConstructorItem(withoutGroupingParentheses(strings.TrimSpace(part)), element)
		if !ok {
			return nil, false
		}
		items = append(items, item)
	}
	return items, true
}

// arrayConstructorItem reads one element of an ARRAY constructor.
func arrayConstructorItem(part, element string) (arrayItem, bool) {
	switch {
	case strings.EqualFold(part, "NULL"):
		return arrayItem{null: true}, true
	case isIntegerLiteral(part), isDecimalLiteral(part):
		return arrayItem{text: part}, true
	case isPlainString(part):
		return arrayItem{text: plainStringValue(part)}, true
	}
	at := strings.LastIndex(part, "::")
	if at < 0 {
		return arrayItem{}, false
	}
	operand := strings.TrimSpace(part[:at])
	cast, isCast := readCastType(part[at+2:])
	if !isCast || cast.modifier != "" || !isPlainString(operand) || !cast.isColumnType(element) {
		return arrayItem{}, false
	}
	return arrayItem{text: plainStringValue(operand)}, true
}

// splitOutsideQuotes splits body at each comma outside single quotes.
func splitOutsideQuotes(body string) []string {
	var parts []string
	start, quoted := 0, false
	for i := range len(body) {
		switch {
		case body[i] == '\'':
			quoted = !quoted
		case body[i] == ',' && !quoted:
			parts = append(parts, body[start:i])
			start = i + 1
		}
	}
	return append(parts, body[start:])
}

// skipSpaces is the index of the first byte of text at or after at that is
// not white space.
func skipSpaces(text string, at int) int {
	for at < len(text) && isArraySpace(text[at]) {
		at++
	}
	return at
}

// isArraySpace reports that b is white space in an array's text.
func isArraySpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\v' || b == '\f'
}

// arrayText writes items as PostgreSQL prints an array: an element in double
// quotes where it is empty, is the word NULL, or holds a brace, a comma, a
// double quote, a backslash or white space, with each double quote and
// backslash escaped.
func arrayText(items []arrayItem) string {
	var text strings.Builder
	text.WriteByte('{')
	for i, item := range items {
		if i > 0 {
			text.WriteByte(',')
		}
		switch {
		case item.null:
			text.WriteString("NULL")
		case arrayTextQuotes(item.text):
			text.WriteByte('"')
			text.WriteString(strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(item.text))
			text.WriteByte('"')
		default:
			text.WriteString(item.text)
		}
	}
	text.WriteByte('}')
	return text.String()
}

// arrayTextQuotes reports that PostgreSQL prints text in double quotes as an
// element of an array.
func arrayTextQuotes(text string) bool {
	if text == "" || strings.EqualFold(text, "NULL") {
		return true
	}
	for i := range len(text) {
		if strings.IndexByte(`{}",\`, text[i]) >= 0 || isArraySpace(text[i]) {
			return true
		}
	}
	return false
}
