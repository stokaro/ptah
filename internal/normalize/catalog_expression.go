package normalize

import "strings"

// CatalogExpression folds an expression into the spelling a catalog that
// reprints expressions shares with the declaration it was written from:
// identifier quotes (backticks and double quotes) and whitespace dropped,
// ASCII letters lowered, and a parenthesized name reduced to the name. Text
// between the quotes stringQuotes lists, such as `'` alone or `'"` where a
// double-quoted token is a string rather than a name, is copied as written. A
// backslash inside such text escapes the byte after it, and a doubled quote is
// the quote itself.
func CatalogExpression(expression, stringQuotes string) string {
	var b strings.Builder
	for i := 0; i < len(expression); i++ {
		ch := expression[i]
		if strings.IndexByte(stringQuotes, ch) >= 0 {
			i = copyQuoted(&b, expression, i)
			continue
		}
		switch ch {
		case '`', '"', ' ', '\t', '\n', '\r':
			continue
		default:
			if ch >= 'A' && ch <= 'Z' {
				ch += 'a' - 'A'
			}
			b.WriteByte(ch)
		}
	}
	return collapseParenthesizedNames(b.String())
}

// RenameFunction replaces each call of the function from in expression with a
// call of to, outside quoted text: from followed by an opening parenthesis,
// where no identifier character comes before it. The names are matched as
// written, with no case folded.
func RenameFunction(expression, from, to string) string {
	from, to = from+"(", to+"("
	var b strings.Builder
	for i := 0; i < len(expression); i++ {
		if expression[i] == '\'' || expression[i] == '"' {
			i = copyQuoted(&b, expression, i)
			continue
		}
		if strings.HasPrefix(expression[i:], from) && (i == 0 || !isIdentifierByte(expression[i-1])) {
			b.WriteString(to)
			i += len(from) - 1
			continue
		}
		b.WriteByte(expression[i])
	}
	return b.String()
}

// copyQuoted copies the quoted text that opens at value[start] and returns the
// position of its closing quote, or of the last byte when it is not closed.
func copyQuoted(b *strings.Builder, value string, start int) int {
	quote := value[start]
	b.WriteByte(value[start])
	for i := start + 1; i < len(value); i++ {
		b.WriteByte(value[i])
		if value[i] == '\\' && i+1 < len(value) {
			i++
			b.WriteByte(value[i])
			continue
		}
		if value[i] != quote {
			continue
		}
		if i+1 < len(value) && value[i+1] == quote {
			i++
			b.WriteByte(value[i])
			continue
		}
		return i
	}
	return len(value) - 1
}

// collapseParenthesizedNames removes the parentheses around a bare name, as in
// `(a)+1`, until none is left. Parentheses after a name are a call's and stay.
func collapseParenthesizedNames(expression string) string {
	for {
		next, changed := collapseOneParenthesizedName(expression)
		if !changed {
			return expression
		}
		expression = next
	}
}

func collapseOneParenthesizedName(expression string) (string, bool) {
	for start := 0; start < len(expression); start++ {
		if expression[start] != '(' {
			continue
		}
		if start > 0 && isNameExpression(expression[start-1:start]) {
			continue
		}
		end := strings.IndexByte(expression[start+1:], ')')
		if end < 0 {
			return expression, false
		}
		end += start + 1
		inner := expression[start+1 : end]
		if !isNameExpression(inner) {
			continue
		}
		return expression[:start] + inner + expression[end+1:], true
	}
	return expression, false
}

// isNameExpression reports whether expression is a name, qualified or not:
// letters, digits, underscores and dots only.
func isNameExpression(expression string) bool {
	if expression == "" {
		return false
	}
	for i := 0; i < len(expression); i++ {
		ch := expression[i]
		if ch != '_' && ch != '.' &&
			(ch < '0' || ch > '9') &&
			(ch < 'a' || ch > 'z') &&
			(ch < 'A' || ch > 'Z') {
			return false
		}
	}
	return true
}

// isIdentifierByte reports whether ch can be part of an unquoted identifier.
func isIdentifierByte(ch byte) bool {
	return ch == '_' ||
		(ch >= '0' && ch <= '9') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= 'a' && ch <= 'z')
}
