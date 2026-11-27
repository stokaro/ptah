package sqlserverbatch

import (
	"strings"

	"ptah.run/internal/lexer"
)

// ExecText returns the text an EXEC statement runs when the statement writes
// that text as one string literal, with or without the N in front of it:
//
//	EXEC ('...')
//	EXECUTE (N'...')
//	EXEC sp_executesql N'...'
//	EXEC sys.sp_executesql @stmt = N'...', N'@p int', @p = 1
//	EXEC [sp_executesql] N'...'
//
// The text is the literal's value, each doubled quote read as one. ok is false
// for every other statement, and for an EXEC whose text the server computes
// when it runs it: a variable, a concatenation, a function call, or an EXEC
// that sends the text to a linked server with AT. Nothing that reads the
// migration file sees that text, so no judge of what the file runs reads it.
func ExecText(statement string) (text string, ok bool) {
	p, err := newParser(strings.TrimSpace(statement))
	if err != nil {
		return "", false
	}
	end := len(p.tokens)
	if end > 0 && p.tokens[end-1].Type == lexer.TokenSemicolon {
		end--
	}
	if word := p.keyword(0); word != "EXEC" && word != "EXECUTE" {
		return "", false
	}
	if p.op(1, "(") {
		text, next, ok := p.literal(2)
		if !ok || !p.op(next, ")") || next+1 != end {
			return "", false
		}
		return text, true
	}
	name := p.afterName(1)
	if name == 1 || !strings.EqualFold(strings.TrimSuffix(strings.TrimPrefix(p.tokens[name-1].Value, "["), "]"), "sp_executesql") {
		return "", false
	}
	first := name
	if first < end && strings.EqualFold(p.tokens[first].Value, "@stmt") && p.op(first+1, "=") {
		first += 2
	}
	text, next, ok := p.literal(first)
	if !ok || next != end && !p.op(next, ",") {
		return "", false
	}
	return text, true
}

// literal reads the string literal at index, with the N that may stand
// against its opening quote, and returns its value and the index after it.
func (p *parser) literal(index int) (string, int, bool) {
	if index < len(p.tokens) && strings.EqualFold(p.tokens[index].Value, "N") && index+1 < len(p.tokens) &&
		p.tokens[index+1].Start == p.tokens[index].End {
		index++
	}
	if index >= len(p.tokens) {
		return "", 0, false
	}
	tok := p.tokens[index]
	if tok.Type != lexer.TokenString || len(tok.Value) < 2 || tok.Value[0] != '\'' || tok.Value[len(tok.Value)-1] != '\'' {
		return "", 0, false
	}
	return strings.ReplaceAll(tok.Value[1:len(tok.Value)-1], "''", "'"), index + 1, true
}
