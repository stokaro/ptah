package parser

import (
	"slices"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/lexer"
)

func (p *Parser) parseCreateSQLServerRoutineStatement(statementStart int, kind ast.RoutineKind) (ast.Node, error) {
	mode := sqlServerRoutineKeepTopLevelSemicolon
	if kind == ast.RoutineKindFunction {
		mode = sqlServerRoutineStopAtTopLevelSemicolon
	}
	sql, err := p.collectRawSQLServerRoutine(statementStart, mode)
	if err != nil {
		return nil, err
	}
	return parseSQLServerRoutineSQL(sql, p.dialect, kind), nil
}

func parseSQLServerRoutineSQL(sql, dialect string, kind ast.RoutineKind) *ast.SQLServerRoutineNode {
	tokens := newSQLServerRoutineTokenizer(sql).tokens()
	routine := ast.NewSQLServerRoutine(sql, dialect, kind)
	asIdx := findSQLServerRoutineBodyAS(tokens, kind)
	routine.Name, routine.Parameters, routine.Returns = parseSQLServerRoutineHeader(sql, tokens, kind, asIdx)
	routine.Form = classifySQLServerRoutineForm(routine)
	routine.Body = parseSQLServerRoutineBody(sql, tokens, asIdx)
	return routine
}

type sqlServerRoutineTokenizer struct {
	lexer *lexer.Lexer
}

func newSQLServerRoutineTokenizer(sql string) sqlServerRoutineTokenizer {
	return sqlServerRoutineTokenizer{lexer: lexer.NewLexer(sql)}
}

func (t sqlServerRoutineTokenizer) tokens() []lexer.Token {
	tokens := make([]lexer.Token, 0)
	for {
		tok := t.lexer.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == lexer.TokenEOF {
			return tokens
		}
	}
}

func parseSQLServerRoutineHeader(
	sql string,
	tokens []lexer.Token,
	kind ast.RoutineKind,
	asIdx int,
) (name string, parameters string, returns string) {
	keywordIdx := findSQLServerRoutineKeyword(tokens, strings.ToUpper(string(kind)), 0)
	if keywordIdx == -1 && kind == ast.RoutineKindProcedure {
		keywordIdx = findSQLServerRoutineKeyword(tokens, "PROC", 0)
	}
	if keywordIdx == -1 {
		return "", "", ""
	}

	nameIdx := nextSQLServerRoutineToken(tokens, keywordIdx+1)
	if nameIdx == -1 {
		return "", "", ""
	}
	openParamsIdx := findSQLServerRoutineOperator(tokens, "(", nameIdx+1, asIdx)
	if kind == ast.RoutineKindProcedure && asIdx != -1 {
		if paramsIdx := findSQLServerRoutineOperator(tokens, "@", nameIdx+1, asIdx); paramsIdx != -1 &&
			(openParamsIdx == -1 || paramsIdx < openParamsIdx) {
			name = strings.TrimSpace(sql[tokens[nameIdx].Start:tokens[paramsIdx].Start])
			parameters = strings.TrimSpace(sql[tokens[paramsIdx].Start:tokens[asIdx].Start])
			return name, parameters, ""
		}
	}
	if openParamsIdx == -1 {
		if asIdx == -1 {
			return strings.TrimSpace(sql[tokens[nameIdx].Start:tokens[len(tokens)-1].Start]), "", ""
		}
		return strings.TrimSpace(sql[tokens[nameIdx].Start:tokens[asIdx].Start]), "", ""
	}

	name = strings.TrimSpace(sql[tokens[nameIdx].Start:tokens[openParamsIdx].Start])
	parameters = parseSQLServerRoutineParameters(sql, tokens, openParamsIdx)
	if kind == ast.RoutineKindFunction {
		returns = parseSQLServerRoutineReturns(sql, tokens, findSQLServerRoutineClosingParen(tokens, openParamsIdx), asIdx)
	}
	return name, parameters, returns
}

func findSQLServerRoutineBodyAS(tokens []lexer.Token, kind ast.RoutineKind) int {
	startIdx := 0
	if kind == ast.RoutineKindFunction {
		if returnsIdx := findSQLServerRoutineKeyword(tokens, "RETURNS", 0); returnsIdx != -1 {
			return findSQLServerRoutineBodyASAfterReturns(tokens, returnsIdx+1)
		}
	}
	return findSQLServerRoutineKeyword(tokens, "AS", startIdx)
}

func findSQLServerRoutineBodyASAfterReturns(tokens []lexer.Token, startIdx int) int {
	depth := 0
	for i := max(startIdx, 0); i < len(tokens); i++ {
		switch {
		case tokens[i].MatchOperatorValue("["):
			i = sqlServerRoutineBracketEnd(tokens, i)
		case tokens[i].MatchOperatorValue("("):
			depth++
		case tokens[i].MatchOperatorValue(")") && depth > 0:
			depth--
		case depth == 0 && tokens[i].MatchIdentifierValue("AS"):
			return i
		}
	}
	return -1
}

func parseSQLServerRoutineParameters(sql string, tokens []lexer.Token, openIdx int) string {
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

func findSQLServerRoutineClosingParen(tokens []lexer.Token, openIdx int) int {
	depth := 0
	for i := openIdx; i < len(tokens); i++ {
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
	return openIdx
}

func parseSQLServerRoutineReturns(sql string, tokens []lexer.Token, afterIdx int, asIdx int) string {
	returnsIdx := findSQLServerRoutineKeyword(tokens, "RETURNS", afterIdx+1)
	if returnsIdx == -1 || returnsIdx >= asIdx {
		return ""
	}
	return strings.TrimSpace(sql[tokens[returnsIdx].End:tokens[asIdx].Start])
}

func classifySQLServerRoutineForm(routine *ast.SQLServerRoutineNode) ast.SQLServerRoutineForm {
	if routine.Kind == ast.RoutineKindProcedure {
		return ast.SQLServerRoutineFormProcedure
	}
	returns := strings.ToUpper(routine.Returns)
	switch {
	case strings.HasPrefix(strings.TrimSpace(returns), "TABLE"):
		return ast.SQLServerRoutineFormInlineTableValuedFunction
	case strings.Contains(returns, " TABLE"):
		return ast.SQLServerRoutineFormMultiStatementTableFunction
	default:
		return ast.SQLServerRoutineFormScalarFunction
	}
}

func parseSQLServerRoutineBody(sql string, tokens []lexer.Token, asIdx int) ast.SQLServerRoutineBody {
	if asIdx == -1 {
		return ast.SQLServerRoutineBody{}
	}
	bodyStartIdx := nextSQLServerRoutineToken(tokens, asIdx+1)
	if bodyStartIdx == -1 {
		return ast.SQLServerRoutineBody{}
	}
	bodySQL := strings.TrimSpace(sql[tokens[bodyStartIdx].Start:sqlServerRoutineStatementEnd(sql, tokens)])
	return ast.SQLServerRoutineBody{
		SQL:        bodySQL,
		Statements: parseSQLServerRoutineBodyStatements(bodySQL),
	}
}

func parseSQLServerRoutineBodyStatements(sql string) []ast.SQLServerRoutineStatement {
	parser := sqlServerRoutineBodyParser{
		input:  sql,
		tokens: newSQLServerRoutineTokenizer(sql).tokens(),
	}
	return parser.parseStatements()
}

type sqlServerRoutineBodyParser struct {
	input  string
	tokens []lexer.Token
}

func (p sqlServerRoutineBodyParser) parseStatements() []ast.SQLServerRoutineStatement {
	startIdx := p.nextSignificant(0)
	if startIdx == -1 {
		return nil
	}
	if p.tokens[startIdx].MatchIdentifierValue("BEGIN") && !p.isNonBlockBegin(startIdx) {
		return p.parseOuterBlockStatements(startIdx)
	}
	return p.parseStatementRange(startIdx, len(p.tokens)-1)
}

func (p sqlServerRoutineBodyParser) parseOuterBlockStatements(beginIdx int) []ast.SQLServerRoutineStatement {
	endIdx := p.findMatchingBlockEnd(beginIdx)
	if endIdx == -1 {
		return []ast.SQLServerRoutineStatement{p.statement(beginIdx, len(p.tokens)-1)}
	}

	return p.parseStatementRange(beginIdx+1, endIdx)
}

func (p sqlServerRoutineBodyParser) parseStatementRange(startIdx, endIdx int) []ast.SQLServerRoutineStatement {
	statements := make([]ast.SQLServerRoutineStatement, 0)
	statementStartIdx := -1
	blockDepth := 0
	caseDepth := 0
	for i := startIdx; i < endIdx; i++ {
		tok := p.tokens[i]
		if tok.MatchOperatorValue("[") {
			i = sqlServerRoutineBracketEnd(p.tokens, i)
			continue
		}
		if statementStartIdx == -1 {
			if isSQLServerRoutineTrivia(tok) {
				continue
			}
			statementStartIdx = i
		}

		if tok.Type == lexer.TokenIdentifier {
			nextValue := p.nextSignificantValue(i + 1)
			trackSQLServerRoutineBodyKeyword(tok.Value, nextValue, &blockDepth, &caseDepth)
		}
		if p.startsRecoverableStatement(i) &&
			!p.keywordBelongsToCurrentStatement(statementStartIdx, i) &&
			blockDepth == 0 &&
			caseDepth == 0 &&
			statementStartIdx != i {
			statements = append(statements, p.statementUntil(statementStartIdx, tok.Start))
			statementStartIdx = i
		}
		if tok.Type == lexer.TokenSemicolon && blockDepth == 0 && caseDepth == 0 && statementStartIdx != -1 {
			statements = append(statements, p.statement(statementStartIdx, i))
			statementStartIdx = -1
		}
	}
	if statementStartIdx != -1 {
		statements = append(statements, p.statementUntil(statementStartIdx, p.tokens[endIdx].Start))
	}
	return statements
}

func (p sqlServerRoutineBodyParser) statement(startIdx, endIdx int) ast.SQLServerRoutineStatement {
	if startIdx < 0 || endIdx >= len(p.tokens) || startIdx > endIdx {
		return ast.SQLServerRoutineStatement{}
	}
	end := p.tokens[endIdx].End
	if p.tokens[endIdx].Type == lexer.TokenEOF {
		end = p.tokens[endIdx].Start
	}
	return p.statementUntil(startIdx, end)
}

func (p sqlServerRoutineBodyParser) statementUntil(startIdx int, end int) ast.SQLServerRoutineStatement {
	return ast.SQLServerRoutineStatement{
		Kind: p.classifyStatement(startIdx),
		SQL:  strings.TrimSpace(p.rawFragment(p.tokens[startIdx].Start, end)),
	}
}

func (p sqlServerRoutineBodyParser) classifyStatement(startIdx int) ast.SQLServerRoutineStatementKind {
	if startIdx < 0 || startIdx >= len(p.tokens) {
		return ast.SQLServerRoutineStatementRaw
	}
	if p.tokens[startIdx].MatchOperatorValue("@") {
		return ast.SQLServerRoutineStatementAssignment
	}
	if p.tokens[startIdx].Type != lexer.TokenIdentifier {
		return ast.SQLServerRoutineStatementRaw
	}
	switch strings.ToUpper(p.tokens[startIdx].Value) {
	case "BEGIN":
		return ast.SQLServerRoutineStatementBlock
	case "DECLARE":
		return ast.SQLServerRoutineStatementDeclaration
	case "IF":
		return ast.SQLServerRoutineStatementIf
	case "WHILE":
		return ast.SQLServerRoutineStatementWhile
	case "RETURN":
		return ast.SQLServerRoutineStatementReturn
	case "SELECT":
		return ast.SQLServerRoutineStatementSelect
	case "INSERT":
		return ast.SQLServerRoutineStatementInsert
	case "SET":
		return ast.SQLServerRoutineStatementAssignment
	default:
		return ast.SQLServerRoutineStatementRaw
	}
}

func (p sqlServerRoutineBodyParser) findMatchingBlockEnd(beginIdx int) int {
	blockDepth := 0
	caseDepth := 0
	for i := beginIdx; i < len(p.tokens); i++ {
		tok := p.tokens[i]
		if tok.MatchOperatorValue("[") {
			i = sqlServerRoutineBracketEnd(p.tokens, i)
			continue
		}
		if tok.Type != lexer.TokenIdentifier {
			continue
		}
		nextValue := p.nextSignificantValue(i + 1)
		trackSQLServerRoutineBodyKeyword(tok.Value, nextValue, &blockDepth, &caseDepth)
		if blockDepth == 0 && tok.MatchIdentifierValue("END") {
			return i
		}
	}
	return -1
}

func trackSQLServerRoutineBodyKeyword(value string, nextValue string, blockDepth, caseDepth *int) {
	switch strings.ToUpper(value) {
	case "BEGIN":
		if sqlServerRawRoutineNonBlockBeginFollower(nextValue) {
			return
		}
		(*blockDepth)++
	case "CASE":
		(*caseDepth)++
	case "END":
		if strings.EqualFold(nextValue, "CONVERSATION") {
			return
		}
		if *caseDepth > 0 {
			(*caseDepth)--
		} else if *blockDepth > 0 {
			(*blockDepth)--
		}
	}
}

func findSQLServerRoutineKeyword(tokens []lexer.Token, keyword string, startIdx int) int {
	for i := max(startIdx, 0); i < len(tokens); i++ {
		if tokens[i].MatchOperatorValue("[") {
			i = sqlServerRoutineBracketEnd(tokens, i)
			continue
		}
		if sqlServerRoutinePreviousSignificantIsOperator(tokens, i, "@") {
			continue
		}
		if tokens[i].MatchIdentifierValue(keyword) {
			return i
		}
	}
	return -1
}

func findSQLServerRoutineOperator(tokens []lexer.Token, operator string, startIdx int, endIdx int) int {
	if endIdx == -1 {
		endIdx = len(tokens)
	}
	for i := max(startIdx, 0); i < endIdx; i++ {
		if tokens[i].MatchOperatorValue("[") {
			i = sqlServerRoutineBracketEnd(tokens, i)
			continue
		}
		if tokens[i].MatchOperatorValue(operator) || tokens[i].Value == operator {
			return i
		}
	}
	return -1
}

func sqlServerRoutineStatementEnd(sql string, tokens []lexer.Token) int {
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

func nextSQLServerRoutineToken(tokens []lexer.Token, startIdx int) int {
	for i := max(startIdx, 0); i < len(tokens); i++ {
		if !isSQLServerRoutineTrivia(tokens[i]) && tokens[i].Type != lexer.TokenEOF {
			return i
		}
	}
	return -1
}

func sqlServerRoutineBracketEnd(tokens []lexer.Token, openIdx int) int {
	for i := openIdx + 1; i < len(tokens); i++ {
		if !tokens[i].MatchOperatorValue("]") {
			continue
		}
		if i+1 < len(tokens) && tokens[i+1].MatchOperatorValue("]") {
			i++
			continue
		}
		return i
	}
	return openIdx
}

func (p sqlServerRoutineBodyParser) nextSignificant(startIdx int) int {
	return nextSQLServerRoutineToken(p.tokens, startIdx)
}

func (p sqlServerRoutineBodyParser) nextSignificantValue(startIdx int) string {
	idx := p.nextSignificant(startIdx)
	if idx == -1 {
		return ""
	}
	return strings.ToUpper(p.tokens[idx].Value)
}

func (p sqlServerRoutineBodyParser) isNonBlockBegin(idx int) bool {
	return sqlServerRawRoutineNonBlockBeginFollower(p.nextSignificantValue(idx + 1))
}

func (p sqlServerRoutineBodyParser) startsRecoverableStatement(idx int) bool {
	if idx < 0 || idx >= len(p.tokens) || p.tokens[idx].Type != lexer.TokenIdentifier {
		return false
	}
	if sqlServerRoutinePreviousSignificantIsOperator(p.tokens, idx, "@") {
		return false
	}
	switch strings.ToUpper(p.tokens[idx].Value) {
	case "DECLARE", "IF", "INSERT", "RETURN", "SELECT", "SET", "WHILE":
		return true
	default:
		return false
	}
}

func (p sqlServerRoutineBodyParser) keywordBelongsToCurrentStatement(statementStartIdx, keywordIdx int) bool {
	if statementStartIdx < 0 || keywordIdx < 0 || keywordIdx >= len(p.tokens) {
		return false
	}
	currentKind := p.classifyStatement(statementStartIdx)
	switch {
	case currentKind == ast.SQLServerRoutineStatementInsert && p.tokens[keywordIdx].MatchIdentifierValue("SELECT"):
		return true
	case currentKind == ast.SQLServerRoutineStatementReturn && p.tokens[keywordIdx].MatchIdentifierValue("SELECT"):
		return true
	case currentKind == ast.SQLServerRoutineStatementIf && !p.tokens[keywordIdx].MatchIdentifierValue("RETURN"):
		return true
	case currentKind == ast.SQLServerRoutineStatementWhile && !p.tokens[keywordIdx].MatchIdentifierValue("RETURN"):
		return true
	default:
		return false
	}
}

func sqlServerRoutinePreviousSignificantIsOperator(tokens []lexer.Token, idx int, operator string) bool {
	for i := idx - 1; i >= 0; i-- {
		if isSQLServerRoutineTrivia(tokens[i]) {
			continue
		}
		return tokens[i].MatchOperatorValue(operator)
	}
	return false
}

func isSQLServerRoutineTrivia(tok lexer.Token) bool {
	return tok.Type == lexer.TokenWhitespace || tok.Type == lexer.TokenComment
}

func (p sqlServerRoutineBodyParser) rawFragment(start, end int) string {
	if start < 0 || start > end || end > len(p.input) {
		return ""
	}
	return p.input[start:end]
}
