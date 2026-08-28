package parser

import (
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/lexer"
)

type postgresRoutineParser struct{}

func (postgresRoutineParser) parseCreateRoutine(p *Parser, target string, statementStart int) (ast.Node, error) {
	if target == "PROCEDURE" {
		sql, err := p.collectPostgresRoutineStatement(statementStart)
		if err != nil {
			return nil, err
		}
		return parsePostgresRoutineSQL(sql, p.dialect, ast.RoutineKindProcedure), nil
	}
	if target == "PROC" {
		return compatibilityRoutineParser{}.parseCreateRoutine(p, target, statementStart)
	}
	return p.parseCreateFunction(statementStart)
}

func (postgresRoutineParser) parseCreateDefinerRoutine(p *Parser, statementStart int) (ast.Node, error) {
	return compatibilityRoutineParser{}.parseCreateDefinerRoutine(p, statementStart)
}

func (p *Parser) isPostgresRoutineDialect() bool {
	return platform.IsPostgresFamily(p.dialect)
}

func (p *Parser) parsePostgresDoStatement(statementStart int) (ast.Node, error) {
	p.advance()
	sql, err := p.collectRawStatement(statementStart, "DO statement")
	if err != nil {
		return nil, err
	}

	block := ast.NewPostgresDoBlock(sql)
	body, language := parsePostgresDoBlockSQL(sql)
	block.Language = language
	block.Body = body
	return block, nil
}

func (p *Parser) collectPostgresRoutineStatement(statementStart int) (string, error) {
	blockDepth := 0
	for !p.isAtEnd() {
		if err := p.checkTimeout(); err != nil {
			return "", err
		}
		if p.current.Type == lexer.TokenSemicolon && blockDepth == 0 {
			sql := p.rawStatement(statementStart)
			p.advance()
			return sql, nil
		}
		if p.current.Type == lexer.TokenIdentifier {
			trackPostgresSQLBodyKeyword(p.current.Value, &blockDepth)
		}
		p.advance()
	}

	if blockDepth > 0 {
		return "", fmt.Errorf("unterminated CREATE PROCEDURE body at position %d", p.current.Start)
	}
	return p.rawStatementFragment(statementStart, p.previous.End), nil
}

func (p *Parser) attachPostgresFunctionBody(function *ast.CreateFunctionNode) {
	if !p.isPostgresRoutineDialect() || function == nil {
		return
	}
	delimiter := ""
	if function.RoutineBody != nil {
		delimiter = function.RoutineBody.Delimiter
	}
	body := parsePostgresRoutineBody(function.Body, function.Language, delimiter)
	function.RoutineBody = &body
}

func parsePostgresDoBlockSQL(sql string) (ast.PostgresRoutineBody, string) {
	tokens := tokenizePostgresRoutineSQL(sql)
	bodyIdx := -1
	language := ""
	for i, tok := range tokens {
		if tok.Type == lexer.TokenString && bodyIdx == -1 {
			bodyIdx = i
			continue
		}
		if tok.MatchIdentifierValue("LANGUAGE") {
			if langIdx := nextPostgresRoutineToken(tokens, i+1); langIdx != -1 {
				language = tokens[langIdx].Value
			}
		}
	}
	if language == "" {
		language = "plpgsql"
	}
	language = strings.ToLower(language)
	if bodyIdx == -1 {
		return ast.PostgresRoutineBody{Language: language}, language
	}

	bodyToken := tokens[bodyIdx].Value
	return parsePostgresRoutineBody(stripSQLStringDelimiters(bodyToken), language, dollarQuoteDelimiter(bodyToken)), language
}

func parsePostgresRoutineSQL(sql, dialect string, kind ast.RoutineKind) *ast.PostgresRoutineNode {
	tokens := tokenizePostgresRoutineSQL(sql)
	routine := ast.NewPostgresRoutine(sql, dialect, kind)
	routine.Name, routine.Parameters = parsePostgresRoutineHeader(sql, tokens, strings.ToUpper(string(kind)))

	bodyToken, language := parsePostgresRoutineBodyClause(tokens)
	routine.Language = strings.ToLower(language)
	if bodyToken != "" {
		routine.Body = parsePostgresRoutineBody(stripSQLStringDelimiters(bodyToken), routine.Language, dollarQuoteDelimiter(bodyToken))
	} else if bodySQL := parsePostgresSQLBody(sql, tokens); bodySQL != "" {
		routine.Body = parsePostgresRoutineBody(bodySQL, routine.Language, "")
	}
	return routine
}

func parsePostgresRoutineHeader(sql string, tokens []lexer.Token, keyword string) (name, parameters string) {
	for i, tok := range tokens {
		if !tok.MatchIdentifierValue(keyword) {
			continue
		}
		nameIdx := nextPostgresRoutineToken(tokens, i+1)
		if nameIdx == -1 {
			return "", ""
		}
		for j := nameIdx; j < len(tokens); j++ {
			if tokens[j].MatchOperatorValue("(") {
				name := strings.TrimSpace(sql[tokens[nameIdx].Start:tokens[j].Start])
				params := parsePostgresRoutineParameters(sql, tokens, j)
				return name, params
			}
		}
		return strings.TrimSpace(sql[tokens[nameIdx].Start:tokens[len(tokens)-1].Start]), ""
	}
	return "", ""
}

func parsePostgresRoutineParameters(sql string, tokens []lexer.Token, openIdx int) string {
	depth := 0
	paramsStart := tokens[openIdx].End
	for i := openIdx; i < len(tokens); i++ {
		switch {
		case tokens[i].MatchOperatorValue("("):
			depth++
		case tokens[i].MatchOperatorValue(")"):
			depth--
			if depth == 0 {
				return strings.TrimSpace(sql[paramsStart:tokens[i].Start])
			}
		}
	}
	return ""
}

func parsePostgresRoutineBodyClause(tokens []lexer.Token) (bodyToken, language string) {
	for i, tok := range tokens {
		if tok.MatchIdentifierValue("LANGUAGE") {
			if langIdx := nextPostgresRoutineToken(tokens, i+1); langIdx != -1 {
				language = tokens[langIdx].Value
			}
			continue
		}
		if tok.MatchIdentifierValue("AS") && bodyToken == "" {
			if bodyIdx := nextPostgresRoutineToken(tokens, i+1); bodyIdx != -1 && tokens[bodyIdx].Type == lexer.TokenString {
				bodyToken = tokens[bodyIdx].Value
			}
		}
	}
	return bodyToken, language
}

func parsePostgresSQLBody(sql string, tokens []lexer.Token) string {
	for _, tok := range tokens {
		if tok.MatchIdentifierValue("BEGIN") {
			return strings.TrimSpace(sql[tok.Start:postgresRoutineStatementEnd(sql, tokens)])
		}
	}
	return ""
}

func postgresRoutineStatementEnd(sql string, tokens []lexer.Token) int {
	for _, tok := range slices.Backward(tokens) {
		switch tok.Type {
		case lexer.TokenEOF, lexer.TokenWhitespace, lexer.TokenComment:
			continue
		case lexer.TokenSemicolon:
			return tok.Start
		default:
			return tok.End
		}
	}
	return len(sql)
}

func parsePostgresRoutineBody(sql, language, delimiter string) ast.PostgresRoutineBody {
	body := ast.PostgresRoutineBody{
		SQL:       sql,
		Delimiter: delimiter,
		Language:  strings.ToLower(language),
	}
	if strings.EqualFold(language, "plpgsql") {
		body.Statements = parsePostgresPLpgSQLStatements(sql)
		return body
	}
	if strings.TrimSpace(sql) != "" {
		body.Statements = []ast.PostgresRoutineStatement{{
			Kind: ast.PostgresRoutineStatementRaw,
			SQL:  strings.TrimSpace(sql),
		}}
	}
	return body
}

func parsePostgresPLpgSQLStatements(sql string) []ast.PostgresRoutineStatement {
	parser := postgresRoutineBodyParser{
		input:  sql,
		tokens: newPostgresRoutineTokenizer(sql).tokens(),
	}
	return parser.parseStatements()
}

// postgresRoutineTokenizer is the PostgreSQL routine sub-language tokenization
// boundary. It intentionally reuses the shared SQL token primitives for string,
// comment, and identifier handling, while keeping routine-body parsing separate
// from the generic SQL statement parser.
type postgresRoutineTokenizer struct {
	lexer *lexer.Lexer
}

func newPostgresRoutineTokenizer(sql string) postgresRoutineTokenizer {
	return postgresRoutineTokenizer{lexer: lexer.NewLexer(sql)}
}

func (t postgresRoutineTokenizer) tokens() []lexer.Token {
	tokens := make([]lexer.Token, 0)
	for {
		tok := t.lexer.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == lexer.TokenEOF {
			return tokens
		}
	}
}

type postgresRoutineBodyParser struct {
	input  string
	tokens []lexer.Token
}

func (p postgresRoutineBodyParser) parseStatements() []ast.PostgresRoutineStatement {
	startIdx := p.nextSignificant(0)
	if startIdx == -1 {
		return nil
	}

	statements := make([]ast.PostgresRoutineStatement, 0)
	if p.tokens[startIdx].MatchIdentifierValue("DECLARE") {
		declareEndIdx := p.findTopLevelKeyword(startIdx+1, "BEGIN")
		if declareEndIdx == -1 {
			return []ast.PostgresRoutineStatement{p.statement(startIdx, len(p.tokens)-1)}
		}
		statements = append(statements, ast.PostgresRoutineStatement{
			Kind: ast.PostgresRoutineStatementDeclaration,
			SQL:  p.rawTokenRange(startIdx, p.tokens[declareEndIdx].Start),
		})
		startIdx = declareEndIdx
	}

	if p.tokens[startIdx].MatchIdentifierValue("BEGIN") {
		return append(statements, p.parseOuterBlockStatements(startIdx)...)
	}
	return append(statements, p.statement(startIdx, len(p.tokens)-1))
}

func (p postgresRoutineBodyParser) parseOuterBlockStatements(beginIdx int) []ast.PostgresRoutineStatement {
	endIdx := p.findMatchingBlockEnd(beginIdx)
	if endIdx == -1 {
		return []ast.PostgresRoutineStatement{p.statement(beginIdx, len(p.tokens)-1)}
	}
	return p.parseStatementRange(beginIdx+1, endIdx)
}

// parseStatementRange splits the tokens in [fromIdx, toIdx) into statements.
//
// It is the block loop, taken out of [parseOuterBlockStatements] so that the
// statements a control-flow statement carries are split by the same code that
// splits the body they sit in. A second splitter would answer the same question
// differently on the first edit (stokaro/ptah#2393).
func (p postgresRoutineBodyParser) parseStatementRange(fromIdx, toIdx int) []ast.PostgresRoutineStatement {
	statements := make([]ast.PostgresRoutineStatement, 0)
	statementStartIdx := -1
	depth := 0
	caseDepth := 0
	pendingEndTrailer := false

	for i := fromIdx; i < toIdx; i++ {
		tok := p.tokens[i]
		if statementStartIdx == -1 {
			if isPostgresRoutineTrivia(tok) {
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
			statements = append(statements, ast.PostgresRoutineStatement{
				Kind: ast.PostgresRoutineStatementException,
				SQL:  p.rawTokenRange(i, p.tokens[toIdx].Start),
			})
			return statements
		}

		if tok.Type == lexer.TokenIdentifier {
			trackPostgresRoutineKeyword(tok.Value, &depth, &caseDepth, &pendingEndTrailer)
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

func (p postgresRoutineBodyParser) statement(startIdx, endIdx int) ast.PostgresRoutineStatement {
	if startIdx < 0 || endIdx >= len(p.tokens) || startIdx > endIdx {
		return ast.PostgresRoutineStatement{}
	}
	end := p.tokens[endIdx].End
	if p.tokens[endIdx].Type == lexer.TokenEOF {
		end = p.tokens[endIdx].Start
	}
	statement := ast.PostgresRoutineStatement{
		Kind: p.classifyStatement(startIdx),
		SQL:  strings.TrimSpace(p.rawFragment(p.tokens[startIdx].Start, end)),
	}
	if from, to, ok := p.nestedRange(startIdx, endIdx); ok {
		statement.Statements = p.parseStatementRange(from, to)
	}
	return statement
}

// skipBranchKeyword reports whether the token at idx opens a branch of the
// statement being split, and the index to resume from when it does.
//
// ELSE opens its branch by itself. ELSIF and WHEN carry a condition first, so
// their branch opens at the THEN that follows.
func (p postgresRoutineBodyParser) skipBranchKeyword(idx, toIdx int) (resume int, branch bool) {
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
func (p postgresRoutineBodyParser) nestedRange(startIdx, endIdx int) (from, to int, ok bool) {
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
		trackPostgresRoutineKeyword(tok.Value, &depth, &caseDepth, &pendingEndTrailer)
		if from == -1 {
			if opensRoutineBody(tok.Value, depth, caseDepth) {
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

// opensRoutineBody reports whether a keyword opens the body of the control-flow
// statement that has just been entered, at the depth it opens it.
func opensRoutineBody(value string, depth, caseDepth int) bool {
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

func (p postgresRoutineBodyParser) classifyStatement(startIdx int) ast.PostgresRoutineStatementKind {
	if startIdx < 0 || startIdx >= len(p.tokens) || p.tokens[startIdx].Type != lexer.TokenIdentifier {
		return ast.PostgresRoutineStatementRaw
	}
	switch strings.ToUpper(p.tokens[startIdx].Value) {
	case "BEGIN":
		return ast.PostgresRoutineStatementBlock
	case "RETURN", "RETURNING":
		return ast.PostgresRoutineStatementReturn
	case "PERFORM":
		return ast.PostgresRoutineStatementPerform
	case "EXECUTE":
		return ast.PostgresRoutineStatementExecute
	case "RAISE":
		return ast.PostgresRoutineStatementRaise
	case "IF":
		return ast.PostgresRoutineStatementIf
	case "CASE":
		return ast.PostgresRoutineStatementCase
	case "LOOP", "FOR", "WHILE", "FOREACH":
		return ast.PostgresRoutineStatementLoop
	default:
		return ast.PostgresRoutineStatementRaw
	}
}

func (p postgresRoutineBodyParser) findMatchingBlockEnd(beginIdx int) int {
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
		trackPostgresRoutineKeyword(tok.Value, &depth, &caseDepth, &pendingEndTrailer)
		if depth == 0 && tok.MatchIdentifierValue("END") {
			return i
		}
	}
	return -1
}

func (p postgresRoutineBodyParser) findTopLevelKeyword(startIdx int, keyword string) int {
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
		trackPostgresRoutineKeyword(tok.Value, &depth, &caseDepth, &pendingEndTrailer)
	}
	return -1
}

func trackPostgresRoutineKeyword(value string, blockDepth, caseDepth *int, pendingEndTrailer *bool) {
	keyword := strings.ToUpper(value)
	if *pendingEndTrailer {
		*pendingEndTrailer = false
		if isPostgresRoutineEndTrailerKeyword(keyword) {
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

func trackPostgresSQLBodyKeyword(value string, blockDepth *int) {
	switch strings.ToUpper(value) {
	case "BEGIN":
		(*blockDepth)++
	case "END":
		if *blockDepth > 0 {
			(*blockDepth)--
		}
	}
}

func isPostgresRoutineEndTrailerKeyword(keyword string) bool {
	switch keyword {
	case "CASE", "IF", "LOOP":
		return true
	default:
		return false
	}
}

func tokenizePostgresRoutineSQL(sql string) []lexer.Token {
	return newPostgresRoutineTokenizer(sql).tokens()
}

func nextPostgresRoutineToken(tokens []lexer.Token, startIdx int) int {
	for i := max(startIdx, 0); i < len(tokens); i++ {
		if !isPostgresRoutineTrivia(tokens[i]) && tokens[i].Type != lexer.TokenEOF {
			return i
		}
	}
	return -1
}

func (p postgresRoutineBodyParser) nextSignificant(startIdx int) int {
	return nextPostgresRoutineToken(p.tokens, startIdx)
}

func isPostgresRoutineTrivia(tok lexer.Token) bool {
	return tok.Type == lexer.TokenWhitespace || tok.Type == lexer.TokenComment
}

func (p postgresRoutineBodyParser) rawTokenRange(startIdx, end int) string {
	if startIdx < 0 || startIdx >= len(p.tokens) {
		return ""
	}
	return strings.TrimSpace(p.rawFragment(p.tokens[startIdx].Start, end))
}

func (p postgresRoutineBodyParser) rawFragment(start, end int) string {
	if start < 0 || start > end || end > len(p.input) {
		return ""
	}
	return p.input[start:end]
}

func dollarQuoteDelimiter(value string) string {
	if !strings.HasPrefix(value, "$") {
		return ""
	}
	end := strings.Index(value[1:], "$")
	if end < 0 {
		return ""
	}
	return value[:end+2]
}
