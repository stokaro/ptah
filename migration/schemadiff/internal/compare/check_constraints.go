package compare

import (
	"strings"
	"unicode"
)

func normalizeCheckExpression(expr string) string {
	expr = trimBalancedCheckParens(strings.TrimSpace(expr))

	var b strings.Builder
	inString := false
	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if ch == '\'' {
			b.WriteByte(ch)
			if inString && i+1 < len(expr) && expr[i+1] == '\'' {
				i++
				b.WriteByte('\'')
				continue
			}
			inString = !inString
			continue
		}
		if !inString && ch == ':' && i+1 < len(expr) && expr[i+1] == ':' && checkCastFollowsLiteral(expr, i) {
			i += 2
			for i < len(expr) && isCheckCastChar(rune(expr[i])) {
				i++
			}
			i--
			continue
		}
		if !inString && unicode.IsSpace(rune(ch)) {
			continue
		}
		if !inString && ch == '`' {
			continue
		}
		if !inString && ch >= 'A' && ch <= 'Z' {
			ch += 'a' - 'A'
		}
		b.WriteByte(ch)
	}
	return b.String()
}

func trimBalancedCheckParens(expr string) string {
	for {
		trimmed := strings.TrimSpace(expr)
		if len(trimmed) < 2 || trimmed[0] != '(' || trimmed[len(trimmed)-1] != ')' {
			return trimmed
		}
		if !checkOuterParensWrap(trimmed) {
			return trimmed
		}
		expr = trimmed[1 : len(trimmed)-1]
	}
}

func checkOuterParensWrap(expr string) bool {
	depth := 0
	inString := false
	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if ch == '\'' {
			if inString && i+1 < len(expr) && expr[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}

		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 || (depth == 0 && i != len(expr)-1) {
				return false
			}
		}
	}
	return depth == 0
}

func isCheckCastChar(ch rune) bool {
	return unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_' || ch == '[' || ch == ']' || ch == '.'
}

func checkCastFollowsLiteral(expr string, castIndex int) bool {
	for i := castIndex - 1; i >= 0; i-- {
		ch := expr[i]
		if unicode.IsSpace(rune(ch)) {
			continue
		}
		return ch == '\'' || (ch >= '0' && ch <= '9')
	}
	return false
}

func checkExpressionHasUnsupportedRewrite(generated, database string) bool {
	db := strings.ToLower(database)
	return checkExpressionContainsInOperator(generated) && strings.Contains(db, "any") && strings.Contains(db, "array[")
}

func checkExpressionContainsInOperator(expr string) bool {
	inString := false
	for i := 0; i < len(expr)-1; i++ {
		ch := expr[i]
		if ch == '\'' {
			if inString && i+1 < len(expr) && expr[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString || (ch != 'i' && ch != 'I') || (expr[i+1] != 'n' && expr[i+1] != 'N') {
			continue
		}
		prevIdent := i > 0 && isCheckIdentChar(rune(expr[i-1]))
		nextIdent := i+2 < len(expr) && isCheckIdentChar(rune(expr[i+2]))
		if !prevIdent && !nextIdent {
			return true
		}
	}
	return false
}

func isCheckIdentChar(ch rune) bool {
	return unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_'
}
