package parser

import (
	"fmt"
	"slices"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/lexer"
	"github.com/stokaro/ptah/core/platform"
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

func parsePostgresRoutineHeader(sql string, tokens []lexer.Token, keyword string) (name string, parameters string) {
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

func parsePostgresRoutineBodyClause(tokens []lexer.Token) (bodyToken string, language string) {
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

	statements := make([]ast.PostgresRoutineStatement, 0)
	statementStartIdx := -1
	depth := 0
	caseDepth := 0
	pendingEndTrailer := false

	for i := beginIdx + 1; i < endIdx; i++ {
		tok := p.tokens[i]
		if statementStartIdx == -1 {
			if isPostgresRoutineTrivia(tok) {
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
				SQL:  p.rawTokenRange(i, p.tokens[endIdx].Start),
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
		statements = append(statements, p.statement(statementStartIdx, endIdx))
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
	return ast.PostgresRoutineStatement{
		Kind: p.classifyStatement(startIdx),
		SQL:  strings.TrimSpace(p.rawFragment(p.tokens[startIdx].Start, end)),
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

func (p postgresRoutineBodyParser) rawTokenRange(startIdx int, end int) string {
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
