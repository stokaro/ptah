package parser

import (
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/lexer"
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
	return sqlServerRoutineTokenizer{lexer: lexer.NewLexerWithOptions(sql, lexer.Options{
		BracketIdentifiers:  true,
		DisableHashComments: true,
	})}
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
) (name, parameters, returns string) {
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

func parseSQLServerRoutineReturns(sql string, tokens []lexer.Token, afterIdx, asIdx int) string {
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
	return ast.SQLServerRoutineBody{SQL: strings.TrimSpace(sql[tokens[bodyStartIdx].Start:sqlServerRoutineStatementEnd(sql, tokens)])}
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

func findSQLServerRoutineOperator(tokens []lexer.Token, operator string, startIdx, endIdx int) int {
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
