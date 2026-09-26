package lexer

import (
	"strings"
	"unicode/utf8"
)

// This file holds the parts of PostgreSQL's string constant grammar that both
// ends of a string token need: the lexer, to know where the token ends, and
// [StringValue], to split the token it made back into its segments. One
// answer serves both, so the two cannot disagree about where a constant
// stops.

// unicodeEscapeKeyword is the keyword that names a Unicode escape string's
// escape character.
const unicodeEscapeKeyword = "UESCAPE"

// continuationAt reports whether the text at pos, just after a closing quote,
// continues the string, and answers the position of the next segment's
// opening quote.
//
// It is the rule of section 4.1.2.2 of the PostgreSQL manual, as PostgreSQL
// 18's scanner applies it: only whitespace between the quotes, and at least
// one newline in it. A -- comment is whitespace here, as it is anywhere; a
// block comment is not, and ends the constant.
func continuationAt(text string, pos int) (int, bool) {
	newline := false
	for pos < len(text) {
		switch text[pos] {
		case '\n', '\r':
			newline = true
			pos++
		case ' ', '\t', '\f', '\v':
			pos++
		case '-':
			if !strings.HasPrefix(text[pos:], "--") {
				return 0, false
			}
			pos = lineEnd(text, pos)
		case '\'':
			if !newline {
				return 0, false
			}
			return pos, true
		default:
			return 0, false
		}
	}
	return 0, false
}

// unicodeEscapeClauseAt reports whether a UESCAPE clause follows the Unicode
// escape string that ends at pos, and answers where the clause's string
// constant starts. Whitespace and comments of either kind may surround the
// keyword, as they may around any token.
func unicodeEscapeClauseAt(text string, pos int) (int, bool) {
	keyword := skipGap(text, pos)
	end := keyword + len(unicodeEscapeKeyword)
	if end > len(text) || !strings.EqualFold(text[keyword:end], unicodeEscapeKeyword) {
		return 0, false
	}
	if next, _ := utf8.DecodeRuneInString(text[end:]); end < len(text) && isIdentifierPart(next) {
		return 0, false
	}
	return skipGap(text, end), true
}

// skipGap answers the position after the whitespace and comments at pos.
func skipGap(text string, pos int) int {
	for pos < len(text) {
		switch {
		case isGapSpace(text[pos]):
			pos++
		case strings.HasPrefix(text[pos:], "--"):
			pos = lineEnd(text, pos)
		case strings.HasPrefix(text[pos:], "/*"):
			end := strings.Index(text[pos+2:], "*/")
			if end < 0 {
				return len(text)
			}
			pos += 2 + end + 2
		default:
			return pos
		}
	}
	return pos
}

// lineEnd answers the position of the newline that ends the line pos is on,
// or the end of text.
func lineEnd(text string, pos int) int {
	if end := strings.IndexAny(text[pos:], "\n\r"); end >= 0 {
		return pos + end
	}
	return len(text)
}

// isGapSpace reports the characters PostgreSQL's scanner reads as whitespace.
func isGapSpace(character byte) bool {
	switch character {
	case ' ', '\t', '\n', '\r', '\f', '\v':
		return true
	default:
		return false
	}
}

// hasUnicodeEscapePrefix reports a token that starts as a Unicode escape
// string does.
func hasUnicodeEscapePrefix(token string) bool {
	return len(token) > 2 && (token[0] == 'U' || token[0] == 'u') && token[1] == '&' && token[2] == '\''
}
