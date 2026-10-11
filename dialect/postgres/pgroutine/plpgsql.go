package pgroutine

import (
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// plKind is what a PL/pgSQL statement is, by the word it begins with.
type plKind string

const (
	plRaw         plKind = "raw"
	plDeclaration plKind = "declaration"
	plBlock       plKind = "block"
	plException   plKind = "exception"
	plReturn      plKind = "return"
	plPerform     plKind = "perform"
	plExecute     plKind = "execute"
	plRaise       plKind = "raise"
	plIf          plKind = "if"
	plCase        plKind = "case"
	plLoop        plKind = "loop"
)

// plStatement is one statement of a PL/pgSQL body: its kind, its text, and the
// statements a control-flow statement carries. The text of a control-flow
// statement is all of it, its own END included.
type plStatement struct {
	kind       plKind
	sql        string
	statements []plStatement
}

// parsePLpgSQL splits a PL/pgSQL body into its statements: an optional
// DECLARE section, then the statements of the outer BEGIN ... END block, each
// control-flow statement carrying the statements it runs.
func parsePLpgSQL(sql string) []plStatement {
	parser := bodyParser{
		input:  sql,
		tokens: newTokenizer(sql).tokens(),
	}
	return parser.parseStatements()
}

type bodyParser struct {
	input  string
	tokens []lexer.Token
}

func (p bodyParser) parseStatements() []plStatement {
	startIdx := p.nextSignificant(0)
	if startIdx == -1 {
		return nil
	}

	statements := make([]plStatement, 0)
	if p.tokens[startIdx].MatchIdentifierValue("DECLARE") {
		declareEndIdx := p.findTopLevelKeyword(startIdx+1, "BEGIN")
		if declareEndIdx == -1 {
			return []plStatement{p.statement(startIdx, len(p.tokens)-1)}
		}
		statements = append(statements, plStatement{
			kind: plDeclaration,
			sql:  p.rawTokenRange(startIdx, p.tokens[declareEndIdx].Start),
		})
		startIdx = declareEndIdx
	}

	if p.tokens[startIdx].MatchIdentifierValue("BEGIN") {
		return append(statements, p.parseOuterBlockStatements(startIdx)...)
	}
	return append(statements, p.statement(startIdx, len(p.tokens)-1))
}

func (p bodyParser) parseOuterBlockStatements(beginIdx int) []plStatement {
	endIdx := p.findMatchingBlockEnd(beginIdx)
	if endIdx == -1 {
		return []plStatement{p.statement(beginIdx, len(p.tokens)-1)}
	}
	return p.parseStatementRange(beginIdx+1, endIdx)
}

// parseStatementRange splits the tokens in [fromIdx, toIdx) into statements.
//
// It is the block loop, taken out of [parseOuterBlockStatements] so that the
// statements a control-flow statement carries are split by the same code that
// splits the body they sit in. A second splitter would answer the same question
// differently on the first edit (stokaro/ptah#2393).
func (p bodyParser) parseStatementRange(fromIdx, toIdx int) []plStatement {
	statements := make([]plStatement, 0)
	statementStartIdx := -1
	depth := 0
	caseDepth := 0
	pendingEndTrailer := false

	for i := fromIdx; i < toIdx; i++ {
		tok := p.tokens[i]
		if statementStartIdx == -1 {
			if isTrivia(tok) {
				continue
			}
			// A branch keyword separates statements rather than starting one.
			// Without this, `ELSE EXECUTE '...'` is a single raw statement and
			// what runs in the branch is invisible -- the same blindness one
			// level down that nesting exists to remove (stokaro/ptah#2393).
			if resume, branch := p.skipBranchKeyword(i, toIdx); branch {
				i = resume
				continue
			}
			statementStartIdx = i
		}

		if tok.MatchIdentifierValue("EXCEPTION") && depth == 0 && caseDepth == 0 {
			if statementStartIdx != -1 && statementStartIdx != i {
				statements = append(statements, p.statement(statementStartIdx, i))
			}
			statements = append(statements, plStatement{
				kind: plException,
				sql:  p.rawTokenRange(i, p.tokens[toIdx].Start),
			})
			return statements
		}

		if tok.Type == lexer.TokenIdentifier {
			trackKeyword(tok.Value, &depth, &caseDepth, &pendingEndTrailer)
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
		statements = append(statements, p.statement(statementStartIdx, toIdx))
	}
	return statements
}

func (p bodyParser) statement(startIdx, endIdx int) plStatement {
	if startIdx < 0 || endIdx >= len(p.tokens) || startIdx > endIdx {
		return plStatement{}
	}
	end := p.tokens[endIdx].End
	if p.tokens[endIdx].Type == lexer.TokenEOF {
		end = p.tokens[endIdx].Start
	}
	statement := plStatement{
		kind: p.classifyStatement(startIdx),
		sql:  strings.TrimSpace(p.rawFragment(p.tokens[startIdx].Start, end)),
	}
	if from, to, ok := p.nestedRange(startIdx, endIdx); ok {
		statement.statements = p.parseStatementRange(from, to)
	}
	return statement
}

// skipBranchKeyword reports whether the token at idx opens a branch of the
// statement being split, and the index to resume from when it does.
//
// ELSE opens its branch by itself. ELSIF and WHEN carry a condition first, so
// their branch opens at the THEN that follows.
func (p bodyParser) skipBranchKeyword(idx, toIdx int) (resume int, branch bool) {
	tok := p.tokens[idx]
	if tok.Type != lexer.TokenIdentifier {
		return idx, false
	}
	switch strings.ToUpper(tok.Value) {
	case "ELSE":
		return idx, true
	case "ELSIF", "ELSEIF", "WHEN":
		for i := idx + 1; i < toIdx; i++ {
			if p.tokens[i].MatchIdentifierValue("THEN") {
				return i, true
			}
		}
		// A branch whose THEN is missing is not one this can split; leaving it
		// as a statement keeps the text rather than dropping it.
		return idx, false
	default:
		return idx, false
	}
}

// nestedRange locates the tokens a control-flow statement carries: from just
// after the keyword that opens its body, to the END that closes it.
//
// The opener differs by shape and the tracker already knows the shapes. IF and
// CASE open their body at THEN; LOOP, FOR, WHILE and FOREACH at LOOP itself;
// a nested block at BEGIN. What closes all of them is the depth returning to
// where it started (stokaro/ptah#2393).
func (p bodyParser) nestedRange(startIdx, endIdx int) (from, to int, ok bool) {
	if endIdx >= len(p.tokens) {
		endIdx = len(p.tokens) - 1
	}
	depth, caseDepth := 0, 0
	pendingEndTrailer := false
	from = -1
	for i := startIdx; i <= endIdx; i++ {
		tok := p.tokens[i]
		if tok.Type != lexer.TokenIdentifier {
			continue
		}
		trackKeyword(tok.Value, &depth, &caseDepth, &pendingEndTrailer)
		if from == -1 {
			if opensBody(tok.Value, depth, caseDepth) {
				from = i + 1
			}
			continue
		}
		if depth == 0 && caseDepth == 0 {
			return from, i, from < i
		}
	}
	return 0, 0, false
}

// opensBody reports whether a keyword opens the body of the control-flow
// statement that has just been entered, at the depth it opens it.
func opensBody(value string, depth, caseDepth int) bool {
	switch strings.ToUpper(value) {
	case "THEN":
		// IF ... THEN, and CASE ... WHEN ... THEN.
		return depth == 1 || caseDepth == 1
	case "LOOP", "BEGIN":
		// The keyword that opens these IS the one that raised the depth.
		return depth == 1
	default:
		return false
	}
}

// classifyStatement names a statement by the word it begins with, the label
// in front of a block or a loop skipped.
func (p bodyParser) classifyStatement(startIdx int) plKind {
	startIdx = p.skipLabel(startIdx)
	if startIdx < 0 || startIdx >= len(p.tokens) || p.tokens[startIdx].Type != lexer.TokenIdentifier {
		return plRaw
	}
	switch strings.ToUpper(p.tokens[startIdx].Value) {
	case "BEGIN":
		return plBlock
	case "RETURN", "RETURNING":
		return plReturn
	case "PERFORM":
		return plPerform
	case "EXECUTE":
		return plExecute
	case "RAISE":
		return plRaise
	case "IF":
		return plIf
	case "CASE":
		return plCase
	case "LOOP", "FOR", "WHILE", "FOREACH":
		return plLoop
	default:
		return plRaw
	}
}

// skipLabel returns the index of the token after a label `<<name>>` that
// begins at idx, or idx when none does.
//
// PL/pgSQL writes a label in front of a block or a loop so that EXIT and
// CONTINUE can name it. Read by its first word, a labeled block was a plain
// statement beginning `<`: nothing reported the EXECUTE inside it, and the
// statements it ran were read as one statement nobody recognized.
func (p bodyParser) skipLabel(idx int) int {
	i := idx
	for _, step := range []struct {
		types []lexer.TokenType
		value string
	}{
		{types: []lexer.TokenType{lexer.TokenOperator}, value: "<"},
		{types: []lexer.TokenType{lexer.TokenOperator}, value: "<"},
		{types: []lexer.TokenType{lexer.TokenIdentifier, lexer.TokenString}},
		{types: []lexer.TokenType{lexer.TokenOperator}, value: ">"},
		{types: []lexer.TokenType{lexer.TokenOperator}, value: ">"},
	} {
		i = p.nextSignificant(i)
		if i == -1 || !slices.Contains(step.types, p.tokens[i].Type) || (step.value != "" && p.tokens[i].Value != step.value) {
			return idx
		}
		i++
	}
	if next := p.nextSignificant(i); next != -1 {
		return next
	}
	return idx
}

func (p bodyParser) findMatchingBlockEnd(beginIdx int) int {
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
		trackKeyword(tok.Value, &depth, &caseDepth, &pendingEndTrailer)
		if depth == 0 && tok.MatchIdentifierValue("END") {
			return i
		}
	}
	return -1
}

func (p bodyParser) findTopLevelKeyword(startIdx int, keyword string) int {
	depth := 0
	caseDepth := 0
	pendingEndTrailer := false
	for i := startIdx; i < len(p.tokens); i++ {
		tok := p.tokens[i]
		if tok.Type == lexer.TokenSemicolon {
			pendingEndTrailer = false
			continue
		}
		if tok.Type != lexer.TokenIdentifier {
			continue
		}
		if depth == 0 && caseDepth == 0 && tok.MatchIdentifierValue(keyword) {
			return i
		}
		trackKeyword(tok.Value, &depth, &caseDepth, &pendingEndTrailer)
	}
	return -1
}

func trackKeyword(value string, blockDepth, caseDepth *int, pendingEndTrailer *bool) {
	keyword := strings.ToUpper(value)
	if *pendingEndTrailer {
		*pendingEndTrailer = false
		if isEndTrailerKeyword(keyword) {
			return
		}
	}

	switch keyword {
	case "BEGIN", "IF", "LOOP":
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

func isEndTrailerKeyword(keyword string) bool {
	switch keyword {
	case "CASE", "IF", "LOOP":
		return true
	default:
		return false
	}
}

func (p bodyParser) nextSignificant(startIdx int) int {
	return nextToken(p.tokens, startIdx)
}

func (p bodyParser) rawTokenRange(startIdx, end int) string {
	if startIdx < 0 || startIdx >= len(p.tokens) {
		return ""
	}
	return strings.TrimSpace(p.rawFragment(p.tokens[startIdx].Start, end))
}

func (p bodyParser) rawFragment(start, end int) string {
	if start < 0 || start > end || end > len(p.input) {
		return ""
	}
	return p.input[start:end]
}

// tokenizer reads a body with PostgreSQL's string rules, which a routine and
// its body follow whatever dialect of the family the document names. The
// dialect-blind lexer reads `E'...'` as an identifier and a string, and a
// backslash in a standard string as an escape (stokaro/ptah#3691).
type tokenizer struct {
	lexer *lexer.Lexer
}

func newTokenizer(sql string) tokenizer {
	return tokenizer{lexer: lexer.NewLexerWithOptions(sql, dialectlexer.Options(platform.Postgres))}
}

// tokens returns every token of the body, the closing EOF included.
func (t tokenizer) tokens() []lexer.Token {
	tokens := make([]lexer.Token, 0)
	for {
		tok := t.lexer.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == lexer.TokenEOF {
			return tokens
		}
	}
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
