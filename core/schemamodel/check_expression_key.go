package schemamodel

import (
	"strings"
	"unicode"
)

// checkExpressionKey is the key two spellings of one CHECK condition share, and
// no two conditions that any dialect reads differently share.
//
// No dialect is known where constraints are deduplicated, so only what every
// dialect reads alike is folded: whitespace that stands beside punctuation, and
// parentheses that wrap the whole condition. Letters keep their case, since an
// identifier is case-sensitive on some servers and a string is on all of them.
// Whitespace between two word characters stays, as one space, because `NOT a`
// and `nota` are different conditions.
//
// An expression that holds a quote, a square bracket, a backslash, a dollar
// sign, a type cast, a comment opener or whitespace outside ASCII is kept as
// written, trimmed. Its strings, quoted and bracketed identifiers and
// introducers read differently by dialect: SQL Server and SQLite read `[a b]`
// and `[a  b]` as two columns, and MySQL takes a no-break space inside an
// unquoted identifier. A key that merged two of them would drop the second
// condition without a word.
func checkExpressionKey(expr string) string {
	expr = strings.TrimSpace(expr)
	if strings.ContainsAny(expr, "'\"`\\$[]") || strings.Contains(expr, "::") ||
		strings.Contains(expr, "--") || strings.Contains(expr, "/*") || strings.Contains(expr, "#") ||
		strings.ContainsFunc(expr, isNonASCIISpace) {
		return expr
	}
	return foldPunctuationSpacing(trimWrappingParens(expr))
}

// isNonASCIISpace reports whitespace outside ASCII, such as a no-break space,
// which some servers read as part of an identifier.
func isNonASCIISpace(r rune) bool {
	return r > unicode.MaxASCII && unicode.IsSpace(r)
}

// trimWrappingParens removes parentheses that enclose the whole of expr.
func trimWrappingParens(expr string) string {
	for len(expr) >= 2 && expr[0] == '(' && expr[len(expr)-1] == ')' && wrapsWhole(expr) {
		expr = strings.TrimSpace(expr[1 : len(expr)-1])
	}
	return expr
}

// wrapsWhole reports whether the parenthesis that opens expr closes at its end.
func wrapsWhole(expr string) bool {
	depth := 0
	for i := 0; i < len(expr); i++ {
		switch expr[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(expr)-1 {
				return false
			}
		}
	}
	return depth == 0
}

// foldPunctuationSpacing drops each whitespace run that stands beside
// punctuation (see standsBesidePunctuation) or borders an end of expr, and
// writes any other run as one space.
func foldPunctuationSpacing(expr string) string {
	runes := []rune(expr)
	var out strings.Builder
	for i := 0; i < len(runes); i++ {
		if !unicode.IsSpace(runes[i]) {
			out.WriteRune(runes[i])
			continue
		}
		j := i
		for j < len(runes) && unicode.IsSpace(runes[j]) {
			j++
		}
		if i > 0 && j < len(runes) && !standsBesidePunctuation(runes[i-1], runes[j]) {
			out.WriteByte(' ')
		}
		i = j - 1
	}
	return out.String()
}

// standsBesidePunctuation reports whether whitespace between before and after
// can go: a word beside punctuation, or two punctuation characters of which one
// is a parenthesis or a comma. Two operator characters keep their space, since
// joined they could read as one operator or a comment.
func standsBesidePunctuation(before, after rune) bool {
	if isWordRune(before) != isWordRune(after) {
		return true
	}
	return !isWordRune(before) && (strings.ContainsRune("(),", before) || strings.ContainsRune("(),", after))
}

func isWordRune(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
