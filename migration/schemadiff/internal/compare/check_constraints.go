package compare

import (
	"strings"
	"unicode"
)

func normalizeCheckExpression(expr string) string {
	expr = trimBalancedCheckParens(strings.TrimSpace(expr))

	normalizer := checkExpressionNormalizer{expr: expr}
	return normalizer.normalize()
}

type checkExpressionNormalizer struct {
	expr     string
	builder  strings.Builder
	inString bool
	pos      int
}

func (n *checkExpressionNormalizer) normalize() string {
	for n.pos = 0; n.pos < len(n.expr); n.pos++ {
		n.writeNext()
	}
	return n.builder.String()
}

func (n *checkExpressionNormalizer) writeNext() {
	ch := n.expr[n.pos]
	if n.writeEscapedQuote(ch) || n.writeStringQuote(ch) || n.skipCharsetIntroducer(ch) || n.skipLiteralCast(ch) {
		return
	}
	if !n.inString && unicode.IsSpace(rune(ch)) {
		return
	}
	if !n.inString && ch == '`' {
		return
	}
	if !n.inString && ch >= 'A' && ch <= 'Z' {
		ch += 'a' - 'A'
	}
	n.builder.WriteByte(ch)
}

func (n *checkExpressionNormalizer) writeEscapedQuote(ch byte) bool {
	if ch != '\\' || n.pos+1 >= len(n.expr) || n.expr[n.pos+1] != '\'' {
		return false
	}
	if !n.inString || checkEscapedQuoteClosesString(n.expr, n.pos+1) {
		return true
	}
	n.builder.WriteByte('\'')
	n.pos++
	return true
}

func (n *checkExpressionNormalizer) writeStringQuote(ch byte) bool {
	if ch != '\'' {
		return false
	}
	n.builder.WriteByte(ch)
	if n.inString && n.pos+1 < len(n.expr) && n.expr[n.pos+1] == '\'' {
		n.pos++
		return true
	}
	n.inString = !n.inString
	return true
}

func (n *checkExpressionNormalizer) skipCharsetIntroducer(ch byte) bool {
	if n.inString || ch != '_' || !checkCharsetIntroducerFollows(n.expr, n.pos) {
		return false
	}
	n.pos = skipCheckCharsetIntroducer(n.expr, n.pos)
	if n.pos < len(n.expr) && n.expr[n.pos] == '\\' {
		return true
	}
	n.pos--
	return true
}

func (n *checkExpressionNormalizer) skipLiteralCast(ch byte) bool {
	if n.inString || ch != ':' || n.pos+1 >= len(n.expr) || n.expr[n.pos+1] != ':' || !checkCastFollowsLiteral(n.expr, n.pos) {
		return false
	}
	n.pos += 2
	for n.pos < len(n.expr) && isCheckCastChar(rune(n.expr[n.pos])) {
		n.pos++
	}
	n.pos--
	return true
}

func checkEscapedQuoteClosesString(expr string, quoteIndex int) bool {
	next := quoteIndex + 1
	if next >= len(expr) {
		return true
	}
	return !isCheckStringBodyChar(rune(expr[next]))
}

func checkCharsetIntroducerFollows(expr string, start int) bool {
	i := start + 1
	if i >= len(expr) || !isCheckCharsetNameStart(rune(expr[i])) {
		return false
	}
	for i < len(expr) && isCheckCharsetNameChar(rune(expr[i])) {
		i++
	}
	if i < len(expr) && expr[i] == '\'' {
		return true
	}
	return i+1 < len(expr) && expr[i] == '\\' && expr[i+1] == '\''
}

func skipCheckCharsetIntroducer(expr string, start int) int {
	i := start + 1
	for i < len(expr) && isCheckCharsetNameChar(rune(expr[i])) {
		i++
	}
	return i
}

func isCheckCharsetNameStart(ch rune) bool {
	return unicode.IsLetter(ch)
}

func isCheckCharsetNameChar(ch rune) bool {
	return unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_'
}

func isCheckStringBodyChar(ch rune) bool {
	return unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_'
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
		if ch == '\\' && i+1 < len(expr) && expr[i+1] == '\'' {
			if inString && !checkEscapedQuoteClosesString(expr, i+1) {
				i++
			}
			continue
		}
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
