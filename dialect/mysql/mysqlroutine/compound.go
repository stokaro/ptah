package mysqlroutine

import (
	"strings"

	"ptah.run/internal/lexer"
)

// kind is what a statement of a MySQL compound statement is, by the word it
// begins with.
type kind string

const (
	kindRaw         kind = "raw"
	kindDeclaration kind = "declaration"
	kindHandler     kind = "handler"
	kindCursor      kind = "cursor"
	kindIf          kind = "if"
	kindCase        kind = "case"
	kindBlock       kind = "block"
	kindLoop        kind = "loop"
	kindWhile       kind = "while"
	kindRepeat      kind = "repeat"
	kindLeave       kind = "leave"
	kindIterate     kind = "iterate"
	kindReturn      kind = "return"
	kindSet         kind = "set"
	kindSelect      kind = "select"
	kindInsert      kind = "insert"
	kindUpdate      kind = "update"
	kindDelete      kind = "delete"
	kindOpen        kind = "open"
	kindFetch       kind = "fetch"
	kindClose       kind = "close"
	kindLabel       kind = "label"
	kindPrepare     kind = "prepare"
	kindExecute     kind = "execute"
	kindDeallocate  kind = "deallocate"
)

// statement is one top-level statement of a MySQL routine body. A
// control-flow statement carries its whole text, and the statements it runs
// are not split out of it.
type statement struct {
	kind kind
	sql  string
}

// parseBody splits a MySQL routine body into its top-level statements: the
// statements of the outer BEGIN ... END, or the body itself when it is a
// single statement such as a function's RETURN.
//
// The tokens are read from the text the statements are cut from. A body with
// whitespace in front of it otherwise cuts every statement that many bytes
// early.
func parseBody(bodySQL string) []statement {
	input := strings.TrimSpace(bodySQL)
	parser := bodyParser{
		input:  input,
		tokens: tokenize(input),
	}
	return parser.parseStatements()
}

type bodyParser struct {
	input  string
	tokens []lexer.Token
}

func (p bodyParser) parseStatements() []statement {
	beginIdx, endIdx := p.outerBlockRange()
	if beginIdx == -1 || endIdx == -1 || beginIdx >= endIdx {
		return p.singleStatement()
	}

	statements := make([]statement, 0)
	statementStartIdx := -1
	depth := 0
	caseDepth := 0
	pendingEndTrailer := false

	for i := beginIdx + 1; i < endIdx; i++ {
		tok := p.tokens[i]
		if statementStartIdx == -1 {
			if isTrivia(tok) {
				continue
			}
			statementStartIdx = i
		}

		if tok.Type == lexer.TokenIdentifier {
			keyword := strings.ToUpper(tok.Value)
			if keyword != "IF" || !p.isScalarIF(i) {
				trackKeyword(keyword, &depth, &caseDepth, &pendingEndTrailer)
			}
		}

		if tok.Type == lexer.TokenSemicolon && depth == 0 && caseDepth == 0 && statementStartIdx != -1 {
			statements = append(statements, p.statement(statementStartIdx, i))
			statementStartIdx = -1
			pendingEndTrailer = false
			continue
		}

		if tok.Type == lexer.TokenSemicolon {
			pendingEndTrailer = false
		}
	}

	if statementStartIdx != -1 {
		statements = append(statements, p.statement(statementStartIdx, endIdx))
	}
	return statements
}

func (p bodyParser) singleStatement() []statement {
	startIdx := p.nextSignificant(0)
	if startIdx == -1 {
		return nil
	}
	endIdx := len(p.tokens) - 1
	if endIdx < startIdx {
		return nil
	}
	return []statement{p.statement(startIdx, endIdx)}
}

func (p bodyParser) statement(startIdx, endIdx int) statement {
	if startIdx < 0 || endIdx >= len(p.tokens) || startIdx > endIdx {
		return statement{}
	}
	end := p.tokens[endIdx].End
	if p.tokens[endIdx].Type == lexer.TokenEOF {
		end = p.tokens[endIdx].Start
	}
	sql := strings.TrimSpace(p.rawFragment(p.tokens[startIdx].Start, end))
	statement := statement{
		kind: p.classifyStatement(startIdx),
		sql:  sql,
	}
	return statement
}

func (p bodyParser) classifyStatement(startIdx int) kind {
	if p.isLabelStatement(startIdx) {
		return kindLabel
	}

	tok := p.tokens[startIdx]
	if tok.Type != lexer.TokenIdentifier {
		return kindRaw
	}

	keyword := strings.ToUpper(tok.Value)
	switch keyword {
	case "DECLARE":
		return p.classifyDeclareStatement(startIdx)
	case "IF":
		if p.isScalarIF(startIdx) {
			return kindRaw
		}
		return kindIf
	case "DROP":
		// DROP PREPARE is the other spelling of DEALLOCATE PREPARE.
		if next := p.nextSignificant(startIdx + 1); next != -1 && p.tokens[next].MatchIdentifierValue("PREPARE") {
			return kindDeallocate
		}
		return kindRaw
	}

	if kind, ok := kindsByKeyword[keyword]; ok {
		return kind
	}
	return kindRaw
}

var kindsByKeyword = map[string]kind{
	"CASE":    kindCase,
	"BEGIN":   kindBlock,
	"LOOP":    kindLoop,
	"WHILE":   kindWhile,
	"REPEAT":  kindRepeat,
	"LEAVE":   kindLeave,
	"ITERATE": kindIterate,
	"RETURN":  kindReturn,
	"SET":     kindSet,
	"SELECT":  kindSelect,
	"INSERT":  kindInsert,
	"UPDATE":  kindUpdate,
	"DELETE":  kindDelete,
	"OPEN":    kindOpen,
	"FETCH":   kindFetch,
	"CLOSE":   kindClose,
	// PREPARE builds a statement from text, EXECUTE runs one PREPARE built, and
	// DEALLOCATE releases one.
	"PREPARE":    kindPrepare,
	"EXECUTE":    kindExecute,
	"DEALLOCATE": kindDeallocate,
}

func (p bodyParser) classifyDeclareStatement(startIdx int) kind {
	for i := startIdx + 1; i < len(p.tokens); i++ {
		if p.tokens[i].Type == lexer.TokenSemicolon || p.tokens[i].Type == lexer.TokenEOF {
			return kindDeclaration
		}
		if p.tokens[i].MatchIdentifierValue("HANDLER") {
			return kindHandler
		}
		if p.tokens[i].MatchIdentifierValue("CURSOR") {
			return kindCursor
		}
	}
	return kindDeclaration
}

func (p bodyParser) outerBlockRange() (beginIdx, endIdx int) {
	beginIdx = p.nextSignificant(0)
	if beginIdx != -1 && p.isLabelStatement(beginIdx) {
		colonIdx := p.nextSignificant(beginIdx + 1)
		beginIdx = p.nextSignificant(colonIdx + 1)
	}
	if beginIdx == -1 || !p.tokens[beginIdx].MatchIdentifierValue("BEGIN") {
		return -1, -1
	}

	depth := 0
	caseDepth := 0
	pendingEndTrailer := false
	for i := beginIdx; i < len(p.tokens); i++ {
		tok := p.tokens[i]
		if tok.Type == lexer.TokenSemicolon {
			pendingEndTrailer = false
			continue
		}
		if tok.Type != lexer.TokenIdentifier {
			continue
		}
		keyword := strings.ToUpper(tok.Value)
		if keyword == "IF" && p.isScalarIF(i) {
			continue
		}
		trackKeyword(keyword, &depth, &caseDepth, &pendingEndTrailer)
		if depth == 0 && keyword == "END" {
			return beginIdx, i
		}
	}
	return -1, -1
}

func (p bodyParser) isLabelStatement(startIdx int) bool {
	nextIdx := p.nextSignificant(startIdx + 1)
	return nextIdx != -1 && p.tokens[nextIdx].MatchOperatorValue(":")
}

func (p bodyParser) isScalarIF(idx int) bool {
	nextIdx := p.nextSignificant(idx + 1)
	return scalarIFAt(p.tokens, nextIdx)
}

func (p bodyParser) nextSignificant(startIdx int) int {
	return nextToken(p.tokens, startIdx)
}

func (p bodyParser) rawFragment(start, end int) string {
	return fragment(p.input, start, end)
}

// tokenize reads a body with the lexer the MySQL routine parser reads a
// routine with.
func tokenize(sql string) []lexer.Token {
	l := lexer.NewLexer(sql)
	tokens := make([]lexer.Token, 0)
	for {
		tok := l.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == lexer.TokenEOF {
			return tokens
		}
	}
}

// scalarIFAt reports whether the IF before the token at openParenIdx is the
// IF() function rather than an IF statement: a parenthesis follows, and no
// THEN follows its closing one.
func scalarIFAt(tokens []lexer.Token, openParenIdx int) bool {
	if openParenIdx == -1 || openParenIdx >= len(tokens) || !tokens[openParenIdx].MatchOperatorValue("(") {
		return false
	}
	closeParenIdx := matchingParen(tokens, openParenIdx)
	if closeParenIdx == -1 {
		return true
	}
	nextIdx := nextToken(tokens, closeParenIdx+1)
	return nextIdx == -1 || !tokens[nextIdx].MatchIdentifierValue("THEN")
}

func matchingParen(tokens []lexer.Token, openParenIdx int) int {
	depth := 0
	for i := openParenIdx; i < len(tokens); i++ {
		switch {
		case tokens[i].MatchOperatorValue("("):
			depth++
		case tokens[i].MatchOperatorValue(")"):
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// nextToken returns the index of the first token from startIdx on that is
// neither whitespace, a comment nor the end of the body, or -1.
func nextToken(tokens []lexer.Token, startIdx int) int {
	for i := max(startIdx, 0); i < len(tokens); i++ {
		if !isTrivia(tokens[i]) && tokens[i].Type != lexer.TokenEOF {
			return i
		}
	}
	return -1
}

func isTrivia(tok lexer.Token) bool {
	return tok.Type == lexer.TokenWhitespace || tok.Type == lexer.TokenComment
}

func fragment(input string, start, end int) string {
	if start < 0 || start > end || end > len(input) {
		return ""
	}
	return input[start:end]
}

// trackKeyword follows the nesting a keyword opens or closes: BEGIN, IF,
// LOOP, REPEAT and WHILE open a block and END closes one, CASE opens a CASE,
// and the keyword after END that names what it closes is not an opener.
func trackKeyword(keyword string, blockDepth, caseDepth *int, pendingEndTrailer *bool) {
	if *pendingEndTrailer {
		*pendingEndTrailer = false
		if isEndTrailer(keyword) {
			return
		}
	}
	switch keyword {
	case "BEGIN", "IF", "LOOP", "REPEAT", "WHILE":
		(*blockDepth)++
	case "CASE":
		(*caseDepth)++
	case "END":
		if *caseDepth > 0 {
			(*caseDepth)--
		} else if *blockDepth > 0 {
			(*blockDepth)--
		}
		*pendingEndTrailer = true
	}
}

func isEndTrailer(keyword string) bool {
	switch keyword {
	case "CASE", "IF", "LOOP", "REPEAT", "WHILE":
		return true
	default:
		return false
	}
}
