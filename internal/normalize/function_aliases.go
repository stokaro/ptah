package normalize

import "strings"

// FunctionAliases writes each call in expression to a function that aliases
// names, matched without case, with the name aliases maps it to, and keeps the
// rest of expression as written. A word is a call when an opening parenthesis
// follows it directly. Text inside a single-quoted string is never a call; a
// doubled quote stays inside the string, and so does a quote behind a
// backslash where backslashEscapes says a backslash escapes the next byte.
//
// A dialect descriptor states the aliases (Columns.FunctionAliases): MySQL and
// MariaDB take ucase for upper, so `ucase('abc')` and `upper('abc')` are one
// default there.
func FunctionAliases(expression string, aliases map[string]string, backslashEscapes bool) string {
	if len(aliases) == 0 {
		return expression
	}
	var b strings.Builder
	for i := 0; i < len(expression); i++ {
		ch := expression[i]
		if ch == '\'' {
			end := stringEnd(expression, i, backslashEscapes)
			b.WriteString(expression[i:end])
			i = end - 1
			continue
		}
		if isWordByte(ch) && (i == 0 || !isWordByte(expression[i-1])) {
			end := i
			for end < len(expression) && isWordByte(expression[end]) {
				end++
			}
			word := expression[i:end]
			if name, ok := aliases[strings.ToLower(word)]; ok && end < len(expression) && expression[end] == '(' {
				word = name
			}
			b.WriteString(word)
			i = end - 1
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

// stringEnd returns the position after the quote that closes the string
// opening at start in text, or the length of text when nothing closes it.
func stringEnd(text string, start int, backslashEscapes bool) int {
	for i := start + 1; i < len(text); i++ {
		switch {
		case backslashEscapes && text[i] == '\\':
			i++
		case text[i] == '\'' && i+1 < len(text) && text[i+1] == '\'':
			i++
		case text[i] == '\'':
			return i + 1
		}
	}
	return len(text)
}

// isWordByte reports whether ch can be part of an unquoted name.
func isWordByte(ch byte) bool {
	return ch == '_' || ch == '$' || (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}
