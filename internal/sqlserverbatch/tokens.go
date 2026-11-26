package sqlserverbatch

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// newParser reads text into the words the server reads, with SQL Server's
// lexing: bracketed names, # as the start of a temporary name and block
// comments that nest. Where the lexer and the server could read the text
// differently, the reader refuses it rather than guess, since a word the
// server reads and the reader does not is a statement nobody judges:
//
//   - a token the lexer cannot close, such as a bracketed name with no ];
//   - quoting T-SQL does not have, a backtick or a dollar quote;
//   - a character outside the T-SQL operators, which is where the server
//     reads whitespace the lexer does not (U+200B, NUL and the other ASCII
//     control characters), or a currency sign that opens a money constant;
//   - a number written against a word, as in 0x1INSERT, which the server
//     reads as a number and a word, where the lexer reads one name.
//
// A variable is one token, as the server reads it: @ and the identifier
// characters that touch it, so that @a#b and @1 are one name each. A bare @
// is a variable too: DECLARE @ int = 5 SELECT @ SELECT 42 is three
// statements, so the word after an @ that does not touch it is not part of a
// name.
func newParser(text string) (*parser, error) {
	raw := rawTokens(text)
	p := &parser{text: text}
	for index := 0; index < len(raw); index++ {
		tok := raw[index]
		if tok.Type == lexer.TokenOperator && tok.Value == "@" {
			variable, next, err := readVariable(text, raw, index)
			if err != nil {
				return nil, err
			}
			p.tokens = append(p.tokens, variable)
			index = next - 1
			continue
		}
		switch {
		case tok.Type == lexer.TokenUnknown:
			return nil, unreadableAt(tok.Start, "text the lexer cannot read")
		case tok.Type == lexer.TokenString && strings.HasPrefix(tok.Value, "$"),
			tok.Type == lexer.TokenIdentifier && strings.HasPrefix(tok.Value, "`"):
			return nil, unreadableAt(tok.Start, "quoting T-SQL does not have")
		case tok.Type == lexer.TokenOperator && !strings.Contains(operators, tok.Value):
			return nil, unreadableAt(tok.Start, "a character that is not a T-SQL operator")
		}
		end, number := numberEnd(text, tok)
		if !number {
			p.tokens = append(p.tokens, tok)
			continue
		}
		if end < len(text) && touchesNumber(text[end:]) {
			return nil, unreadableAt(tok.Start, "a number written against a word")
		}
		for index+1 < len(raw) && raw[index+1].Start < end {
			index++
		}
		if raw[index].End != end {
			return nil, unreadableAt(tok.Start, "a number the lexer reads differently")
		}
		p.tokens = append(p.tokens, lexer.Token{Type: lexer.TokenIdentifier, Value: text[tok.Start:end], Start: tok.Start, End: end})
	}
	return p, nil
}

// readVariable reads the variable whose @ is raw[index]: the @ and every
// identifier character that touches it, as the server reads it. It returns the
// variable as one token and the index of the raw token after it. A raw token
// that runs past the variable's end is refused rather than cut: in @1.5 the
// lexer reads the number 1.5, where the server reads the variable @1 and the
// number .5, and fails the batch (Msg 102 on SQL Server 2025).
func readVariable(text string, raw []lexer.Token, index int) (lexer.Token, int, error) {
	start := raw[index].Start
	end := start + 1
	for end < len(text) {
		r, size := utf8.DecodeRuneInString(text[end:])
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && !strings.ContainsRune("_@#$", r) {
			break
		}
		end += size
	}
	next := index + 1
	for next < len(raw) && raw[next].Start < end {
		if raw[next].End > end {
			return lexer.Token{}, 0, unreadableAt(start, "a variable name the lexer reads differently")
		}
		next++
	}
	return lexer.Token{Type: lexer.TokenIdentifier, Value: text[start:end], Start: start, End: end}, next, nil
}

// operators are the characters T-SQL reads as operators and punctuation. The
// lexer reads every other character it has no rule for as an operator of one
// character.
const operators = "()<>=!+-*/%&|^~:@{},."

// rawTokens returns the tokens of text, without whitespace and comments.
func rawTokens(text string) []lexer.Token {
	var tokens []lexer.Token
	lex := lexer.NewLexerWithOptions(text, dialectlexer.Options(platform.SQLServer))
	for tok := lex.NextToken(); tok.Type != lexer.TokenEOF; tok = lex.NextToken() {
		if tok.Type != lexer.TokenWhitespace && tok.Type != lexer.TokenComment {
			tokens = append(tokens, tok)
		}
	}
	return tokens
}

// numberEnd reports whether a numeric constant starts at tok, and the offset
// where the server ends it: 0x and hexadecimal digits, or digits with a
// fraction and an exponent, each optional, as in 1e and 1.; a dollar sign
// before a number writes a money constant. The lexer ends these elsewhere: it
// reads 0x1F as 0 and x1F, and 1e5 as 1 and e5.
func numberEnd(text string, tok lexer.Token) (int, bool) {
	start := tok.Start
	switch {
	case tok.Type == lexer.TokenIdentifier && strings.HasPrefix(tok.Value, "$"):
		start++
	case tok.Type == lexer.TokenIdentifier, tok.Value == ".":
	default:
		return 0, false
	}
	digits := start
	if digits < len(text) && text[digits] == '.' {
		digits++
	}
	if digits >= len(text) || !isDigit(text[digits]) {
		return 0, false
	}
	if strings.HasPrefix(text[start:], "0x") || strings.HasPrefix(text[start:], "0X") {
		return skip(text, start+2, isHexDigit), true
	}
	i := skip(text, start, isDigit)
	if i < len(text) && text[i] == '.' {
		i = skip(text, i+1, isDigit)
	}
	if i < len(text) && (text[i] == 'e' || text[i] == 'E') {
		i++
		if i < len(text) && (text[i] == '+' || text[i] == '-') {
			i++
		}
		i = skip(text, i, isDigit)
	}
	return i, true
}

// skip returns the offset of the first byte of text from i on that is not
// one of, or len(text).
func skip(text string, i int, of func(byte) bool) int {
	for i < len(text) && of(text[i]) {
		i++
	}
	return i
}

// touchesNumber reports whether rest, the text after a number, starts with a
// character the server reads as the start of a word.
func touchesNumber(rest string) bool {
	r, _ := utf8.DecodeRuneInString(rest)
	return unicode.IsLetter(r) || strings.ContainsRune("_$#@", r)
}

func isDigit(b byte) bool { return '0' <= b && b <= '9' }

func isHexDigit(b byte) bool {
	return isDigit(b) || 'a' <= b && b <= 'f' || 'A' <= b && b <= 'F'
}
