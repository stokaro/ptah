package compare

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"ptah.run/internal/lexer"
	"ptah.run/internal/normalize"
)

// normalizeCheckExpression folds a condition for comparison: whitespace and
// backticks outside strings go, letters outside strings are lowered, and a
// cast or a character set introducer on a literal is dropped.
//
// Where a string ends is quoting's question, and a string is kept whole. On a
// dialect whose strings read backslash escapes, a string is compared by the
// value it stands for, written between single quotes with each quote doubled.
// MySQL escapes a quote with a backslash where a declaration doubles it, and
// prints a tab where a declaration writes `\t`. MariaDB prints `'x\\%y'` for
// `'x\%y'`. Each pair is one value.
// Elsewhere a string is compared as written. PostgreSQL stores `t <> 'p\'` as a
// comparison with the two-byte string p\, outside an E'...' string.
func normalizeCheckExpression(expr string, quoting normalize.Quoting) string {
	expr = trimBalancedCheckParens(strings.TrimSpace(expr), quoting)

	normalizer := checkExpressionNormalizer{expr: expr, quoting: quoting}
	return canonicalizeCheckSetMembership(normalizer.normalize())
}

// normalizeCheckCondition is [normalizeCheckExpression] for the CHECK
// comparison on dialect, which keeps the case of a name where the dialect's
// names keep theirs ([checkWordFoldOf]).
func normalizeCheckCondition(expr, dialect string) string {
	quoting := normalize.QuotingOf(dialect)
	expr = trimBalancedCheckParens(strings.TrimSpace(expr), quoting)

	normalizer := checkExpressionNormalizer{expr: expr, quoting: quoting, foldWord: checkWordFoldOf(dialect)}
	return canonicalizeCheckSetMembership(normalizer.normalize())
}

type checkExpressionNormalizer struct {
	expr    string
	quoting normalize.Quoting
	builder strings.Builder
	pos     int
	// foldWord, when set, is the one word test: a word it accepts is lowered
	// and every other word keeps its case. Nil lowers every letter.
	foldWord func(word string) bool
}

func (n *checkExpressionNormalizer) normalize() string {
	for n.pos = 0; n.pos < len(n.expr); n.pos++ {
		n.writeNext()
	}
	return n.builder.String()
}

func (n *checkExpressionNormalizer) writeNext() {
	ch := n.expr[n.pos]
	if n.writeString() || n.skipCharsetIntroducer(ch) || n.skipLiteralCast(ch) {
		return
	}
	if unicode.IsSpace(rune(ch)) || ch == '`' {
		return
	}
	if n.writeWord() {
		return
	}
	if n.foldWord == nil && ch >= 'A' && ch <= 'Z' {
		ch += 'a' - 'A'
	}
	n.builder.WriteByte(ch)
}

// writeWord writes the word that starts at pos, lowered where foldWord
// accepts it and as written where it does not, and leaves pos on its last
// byte. It writes nothing when foldWord is nil or no word starts at pos.
func (n *checkExpressionNormalizer) writeWord() bool {
	if n.foldWord == nil || !isCheckWordStart(n.expr[n.pos]) || (n.pos > 0 && isCheckWordByte(n.expr[n.pos-1])) {
		return false
	}
	end := n.pos
	for end < len(n.expr) && isCheckWordByte(n.expr[end]) {
		end++
	}
	word := n.expr[n.pos:end]
	if n.foldWord(word) {
		word = strings.ToLower(word)
	}
	n.builder.WriteString(word)
	n.pos = end - 1
	return true
}

// isCheckWordStart reports a byte a name or a keyword may start with: an
// ASCII letter, an underscore, or a byte of a multi-byte character.
func isCheckWordStart(ch byte) bool {
	return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == '_' || ch >= utf8.RuneSelf
}

// isCheckWordByte reports a byte of a name or a keyword after its first.
func isCheckWordByte(ch byte) bool {
	return isCheckWordStart(ch) || ch >= '0' && ch <= '9'
}

// backslashStrings reads a string literal with backslash escapes, as the
// shared lexer reads one on every dialect whose descriptor states them.
var backslashStrings = lexer.Options{BackslashEscapes: true}

// writeString writes the string that opens at pos, as normalizeCheckExpression
// says, and leaves pos on its closing quote.
func (n *checkExpressionNormalizer) writeString() bool {
	if !n.quoting.OpensString(n.expr, n.pos) {
		return false
	}
	end := n.quoting.QuotedEnd(n.expr, n.pos)
	literal := n.expr[n.pos : end+1]
	n.pos = end
	if n.quoting.BackslashEscapes {
		if value, read := lexer.StringValue(literal, backslashStrings); read {
			n.builder.WriteByte('\'')
			n.builder.WriteString(strings.ReplaceAll(value, "'", "''"))
			n.builder.WriteByte('\'')
			return true
		}
	}
	n.builder.WriteString(literal)
	return true
}

func (n *checkExpressionNormalizer) skipCharsetIntroducer(ch byte) bool {
	if ch != '_' || !checkCharsetIntroducerFollows(n.expr, n.pos) {
		return false
	}
	n.pos = skipCheckCharsetIntroducer(n.expr, n.pos) - 1
	return true
}

func (n *checkExpressionNormalizer) skipLiteralCast(ch byte) bool {
	if ch != ':' || n.pos+1 >= len(n.expr) || n.expr[n.pos+1] != ':' || !checkCastFollowsLiteral(n.expr, n.pos) {
		return false
	}
	n.pos += 2
	for n.pos < len(n.expr) && isCheckCastChar(rune(n.expr[n.pos])) {
		n.pos++
	}
	n.pos--
	return true
}

// checkCharsetIntroducerFollows reports whether expr holds a character set
// introducer at start: an underscore, a character set name and a string, with
// or without whitespace before the string. MySQL 26.10 and MariaDB 12.3 read
// `_binary 'ab'` as `_binary'ab'`, and store the CHECK `b <> _binary 'ab'` on a
// varbinary column as `b <> _binary'ab'`.
func checkCharsetIntroducerFollows(expr string, start int) bool {
	i := start + 1
	if i >= len(expr) || !isCheckCharsetNameStart(rune(expr[i])) {
		return false
	}
	for i < len(expr) && isCheckCharsetNameChar(rune(expr[i])) {
		i++
	}
	for i < len(expr) && unicode.IsSpace(rune(expr[i])) {
		i++
	}
	return i < len(expr) && expr[i] == '\''
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

func trimBalancedCheckParens(expr string, quoting normalize.Quoting) string {
	for {
		trimmed := strings.TrimSpace(expr)
		if len(trimmed) < 2 || trimmed[0] != '(' || trimmed[len(trimmed)-1] != ')' {
			return trimmed
		}
		if !checkOuterParensWrap(trimmed, quoting) {
			return trimmed
		}
		expr = trimmed[1 : len(trimmed)-1]
	}
}

// checkOuterParensWrap reports whether the parenthesis that opens expr closes
// at its end, reading a parenthesis inside a string as text.
func checkOuterParensWrap(expr string, quoting normalize.Quoting) bool {
	depth := 0
	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if quoting.OpensString(expr, i) {
			i = quoting.QuotedEnd(expr, i)
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

func checkExpressionHasUnsupportedRewrite(desired, database string) bool {
	db := strings.ToLower(database)
	return checkExpressionContainsInOperator(desired) && strings.Contains(db, "any") && strings.Contains(db, "array[")
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
