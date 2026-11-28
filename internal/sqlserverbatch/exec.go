package sqlserverbatch

import (
	"strings"
	"unicode"

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
	literal, ok := ExecLiteral(statement)
	return literal.Text, ok
}

// Literal is the string literal an EXEC runs ([ExecLiteral]): its value, and
// where the value stands in the statement.
type Literal struct {
	// Text is the literal's value, each doubled quote read as one.
	Text string
	// Start and End are the byte range of the value in the statement, inside
	// the quotes, doubled quotes included.
	Start, End int
}

// Offset returns the byte offset in the statement of offset i of l.Text.
// Each doubled quote before i is two bytes of the statement and one of the
// text, so the two offsets part where the literal holds one.
func (l Literal) Offset(i int) int {
	at := l.Start
	for read := 0; read < i && at < l.End; read++ {
		if l.Text[read] == '\'' {
			at++
		}
		at++
	}
	return at
}

// ExecLiteral is [ExecText] that also says where the literal stands in
// statement, so a reader that reports positions can report the statements
// the literal holds at their place in the file.
func ExecLiteral(statement string) (Literal, bool) {
	trimmed := strings.TrimSpace(statement)
	lead := len(statement) - len(strings.TrimLeftFunc(statement, unicode.IsSpace))
	p, err := newParser(trimmed)
	if err != nil {
		return Literal{}, false
	}
	end := len(p.tokens)
	if end > 0 && p.tokens[end-1].Type == lexer.TokenSemicolon {
		end--
	}
	if word := p.keyword(0); word != "EXEC" && word != "EXECUTE" {
		return Literal{}, false
	}
	if p.op(1, "(") {
		literal, next, ok := p.literal(2)
		if !ok || !p.op(next, ")") || next+1 != end {
			return Literal{}, false
		}
		return literal.shift(lead), true
	}
	name := p.afterName(1)
	if name == 1 || !strings.EqualFold(strings.TrimSuffix(strings.TrimPrefix(p.tokens[name-1].Value, "["), "]"), "sp_executesql") {
		return Literal{}, false
	}
	first := name
	if first < end && strings.EqualFold(p.tokens[first].Value, "@stmt") && p.op(first+1, "=") {
		first += 2
	}
	literal, next, ok := p.literal(first)
	if !ok || next != end && !p.op(next, ",") {
		return Literal{}, false
	}
	return literal.shift(lead), true
}

// shift moves l's range by offset bytes, from the trimmed statement the
// parser read to the statement as given.
func (l Literal) shift(offset int) Literal {
	l.Start += offset
	l.End += offset
	return l
}

// literal reads the string literal at index, with the N that may stand
// against its opening quote, and returns it and the index after it.
func (p *parser) literal(index int) (Literal, int, bool) {
	if index < len(p.tokens) && strings.EqualFold(p.tokens[index].Value, "N") && index+1 < len(p.tokens) &&
		p.tokens[index+1].Start == p.tokens[index].End {
		index++
	}
	if index >= len(p.tokens) {
		return Literal{}, 0, false
	}
	tok := p.tokens[index]
	if tok.Type != lexer.TokenString || len(tok.Value) < 2 || tok.Value[0] != '\'' || tok.Value[len(tok.Value)-1] != '\'' {
		return Literal{}, 0, false
	}
	return Literal{
		Text:  strings.ReplaceAll(tok.Value[1:len(tok.Value)-1], "''", "'"),
		Start: tok.Start + 1,
		End:   tok.End - 1,
	}, index + 1, true
}
