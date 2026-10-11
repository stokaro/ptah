package parser

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/lexer"
)

func (p *Parser) parseCreateMySQLRoutineStatement(statementStart int, kind ast.RoutineKind) (ast.Node, error) {
	sql, err := p.collectRawRoutineStatement(statementStart)
	if err != nil {
		return nil, err
	}
	routine, err := parseMySQLRoutineSQL(sql, p.dialect, kind)
	if err != nil {
		return ast.NewOpaqueRoutine(sql, p.dialect, kind), nil
	}
	return routine, nil
}

func parseMySQLRoutineSQL(sql, dialect string, kind ast.RoutineKind) (*ast.MySQLRoutineNode, error) {
	parser := newMySQLRoutineParser(sql, dialect, kind)
	return parser.parse()
}

type mysqlRoutineParser struct {
	input  string
	tokens []lexer.Token

	dialect string
	kind    ast.RoutineKind
}

func newMySQLRoutineParser(sql, dialect string, kind ast.RoutineKind) mysqlRoutineParser {
	trimmed := strings.TrimSpace(sql)
	return mysqlRoutineParser{
		input:   trimmed,
		tokens:  tokenizeMySQLRoutineSQL(trimmed),
		dialect: dialect,
		kind:    kind,
	}
}

func tokenizeMySQLRoutineSQL(sql string) []lexer.Token {
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

func (p mysqlRoutineParser) parse() (*ast.MySQLRoutineNode, error) {
	routine := ast.NewMySQLRoutine(p.input, p.dialect, p.kind)
	targetIdx, err := p.findRoutineTarget()
	if err != nil {
		return nil, err
	}

	routine.Definer = p.collectDefiner(targetIdx)
	afterNameIdx, err := p.parseRoutineName(targetIdx, routine)
	if err != nil {
		return nil, err
	}

	afterParamsIdx, err := p.parseRoutineParameters(afterNameIdx, routine)
	if err != nil {
		return nil, err
	}

	bodyIdx := p.findRoutineBodyStart(afterParamsIdx)
	if bodyIdx == -1 {
		return nil, fmt.Errorf("unsupported MySQL routine body at position %d", p.tokens[afterParamsIdx].Start)
	}

	if routine.Kind == ast.RoutineKindFunction {
		routine.Returns = p.collectFunctionReturns(afterParamsIdx, bodyIdx)
	}
	routine.SetCharacteristics(p.collectRoutineCharacteristics(afterParamsIdx, bodyIdx))

	body, err := p.parseRoutineBody(bodyIdx)
	if err != nil {
		return nil, err
	}
	routine.Body = body

	return routine, nil
}

func (p mysqlRoutineParser) findRoutineTarget() (int, error) {
	for i, tok := range p.tokens {
		if tok.Type != lexer.TokenIdentifier {
			continue
		}
		if tok.MatchIdentifierValue("FUNCTION") && p.kind == ast.RoutineKindFunction {
			return i, nil
		}
		if tok.MatchIdentifierValue("PROCEDURE") && p.kind == ast.RoutineKindProcedure {
			return i, nil
		}
	}
	return -1, fmt.Errorf("expected CREATE %s target", strings.ToUpper(string(p.kind)))
}

func (p mysqlRoutineParser) collectDefiner(targetIdx int) string {
	createIdx := p.findFirstIdentifier("CREATE")
	if createIdx == -1 || createIdx >= targetIdx {
		return ""
	}

	for i := createIdx + 1; i < targetIdx; i++ {
		if !p.tokens[i].MatchIdentifierValue("DEFINER") {
			continue
		}
		return strings.TrimSpace(p.rawFragment(p.tokens[i].Start, p.tokens[targetIdx].Start))
	}
	return ""
}

func (p mysqlRoutineParser) parseRoutineName(targetIdx int, routine *ast.MySQLRoutineNode) (int, error) {
	nameStartIdx := p.nextSignificant(targetIdx + 1)
	if nameStartIdx == -1 {
		return -1, fmt.Errorf("expected MySQL routine name")
	}

	for i := nameStartIdx; i < len(p.tokens); i++ {
		if p.tokens[i].MatchOperatorValue("(") {
			routine.Name = strings.TrimSpace(p.rawFragment(p.tokens[nameStartIdx].Start, p.tokens[i].Start))
			return i, nil
		}
	}
	return -1, fmt.Errorf("expected parameter list for MySQL routine %s", p.rawToken(nameStartIdx))
}

func (p mysqlRoutineParser) parseRoutineParameters(openParenIdx int, routine *ast.MySQLRoutineNode) (int, error) {
	closeParenIdx, err := p.matchingParen(openParenIdx)
	if err != nil {
		return -1, err
	}
	routine.Parameters = strings.TrimSpace(p.rawFragment(p.tokens[openParenIdx].End, p.tokens[closeParenIdx].Start))
	return closeParenIdx + 1, nil
}

func (p mysqlRoutineParser) matchingParen(openParenIdx int) (int, error) {
	depth := 0
	for i := openParenIdx; i < len(p.tokens); i++ {
		switch {
		case p.tokens[i].MatchOperatorValue("("):
			depth++
		case p.tokens[i].MatchOperatorValue(")"):
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}
	return -1, fmt.Errorf("unterminated MySQL routine parameter list")
}

func (p mysqlRoutineParser) findRoutineBodyStart(startIdx int) int {
	for i := startIdx; i < len(p.tokens); i++ {
		if p.tokens[i].Type != lexer.TokenIdentifier {
			continue
		}
		if p.isOuterBodyLabel(i) {
			return i
		}
		if p.tokens[i].MatchIdentifierValue("BEGIN") || p.tokens[i].MatchIdentifierValue("RETURN") {
			return i
		}
	}
	return -1
}

func (p mysqlRoutineParser) isOuterBodyLabel(idx int) bool {
	colonIdx := p.nextSignificant(idx + 1)
	if colonIdx == -1 || !p.tokens[colonIdx].MatchOperatorValue(":") {
		return false
	}
	beginIdx := p.nextSignificant(colonIdx + 1)
	return beginIdx != -1 && p.tokens[beginIdx].MatchIdentifierValue("BEGIN")
}

func (p mysqlRoutineParser) collectFunctionReturns(startIdx, bodyIdx int) string {
	returnsIdx := p.findIdentifierBetween("RETURNS", startIdx, bodyIdx)
	if returnsIdx == -1 {
		return ""
	}

	valueStartIdx := p.nextSignificant(returnsIdx + 1)
	if valueStartIdx == -1 || valueStartIdx >= bodyIdx {
		return ""
	}

	valueEnd := p.tokens[bodyIdx].Start
	for i := valueStartIdx; i < bodyIdx; i++ {
		if isMySQLRoutineCharacteristicStart(p.tokens[i]) {
			valueEnd = p.tokens[i].Start
			break
		}
	}
	return strings.TrimSpace(p.rawFragment(p.tokens[valueStartIdx].Start, valueEnd))
}

func (p mysqlRoutineParser) collectRoutineCharacteristics(startIdx, bodyIdx int) []string {
	characteristics := make([]string, 0)
	for i := startIdx; i < bodyIdx; i++ {
		if isMySQLRoutineCharacteristicStart(p.tokens[i]) {
			characteristic, nextIdx := p.collectRoutineCharacteristic(i, bodyIdx)
			if characteristic != "" {
				characteristics = append(characteristics, characteristic)
			}
			i = nextIdx - 1
		}
	}
	return characteristics
}

func (p mysqlRoutineParser) collectRoutineCharacteristic(startIdx, bodyIdx int) (string, int) {
	endIdx := p.routineCharacteristicEnd(startIdx, bodyIdx)
	end := p.tokens[endIdx].Start
	if endIdx >= bodyIdx {
		end = p.tokens[bodyIdx].Start
	}
	return strings.TrimSpace(p.rawFragment(p.tokens[startIdx].Start, end)), endIdx
}

func (p mysqlRoutineParser) routineCharacteristicEnd(startIdx, bodyIdx int) int {
	switch strings.ToUpper(p.tokens[startIdx].Value) {
	case "COMMENT", "LANGUAGE":
		return p.afterSignificantTokens(startIdx, bodyIdx, 1)
	case "NOT":
		return p.afterOptionalKeyword(startIdx, bodyIdx, "DETERMINISTIC")
	case "NO", "CONTAINS":
		return p.afterOptionalKeyword(startIdx, bodyIdx, "SQL")
	case "READS", "MODIFIES":
		return p.afterOptionalKeywords(startIdx, bodyIdx, "SQL", "DATA")
	case "SQL":
		if nextIdx := p.nextSignificant(startIdx + 1); nextIdx != -1 && nextIdx < bodyIdx && p.tokens[nextIdx].MatchIdentifierValue("SECURITY") {
			return p.afterSignificantTokens(startIdx, bodyIdx, 2)
		}
	}
	return min(startIdx+1, bodyIdx)
}

func (p mysqlRoutineParser) afterOptionalKeyword(startIdx, bodyIdx int, keyword string) int {
	return p.afterOptionalKeywords(startIdx, bodyIdx, keyword)
}

func (p mysqlRoutineParser) afterOptionalKeywords(startIdx, bodyIdx int, keywords ...string) int {
	endIdx := startIdx + 1
	for _, keyword := range keywords {
		nextIdx := p.nextSignificant(endIdx)
		if nextIdx == -1 || nextIdx >= bodyIdx || !p.tokens[nextIdx].MatchIdentifierValue(keyword) {
			return endIdx
		}
		endIdx = nextIdx + 1
	}
	return min(endIdx, bodyIdx)
}

func (p mysqlRoutineParser) afterSignificantTokens(startIdx, bodyIdx, count int) int {
	endIdx := startIdx + 1
	for range count {
		nextIdx := p.nextSignificant(endIdx)
		if nextIdx == -1 || nextIdx >= bodyIdx {
			return min(endIdx, bodyIdx)
		}
		endIdx = nextIdx + 1
	}
	return min(endIdx, bodyIdx)
}

func (p mysqlRoutineParser) parseRoutineBody(bodyIdx int) (ast.MySQLRoutineBody, error) {
	switch {
	case p.isOuterBodyLabel(bodyIdx):
		bodySQL, err := p.collectCompoundBodySQL(bodyIdx)
		if err != nil {
			return ast.MySQLRoutineBody{}, err
		}
		return ast.MySQLRoutineBody{SQL: bodySQL}, nil
	case p.tokens[bodyIdx].MatchIdentifierValue("BEGIN"):
		bodySQL, err := p.collectCompoundBodySQL(bodyIdx)
		if err != nil {
			return ast.MySQLRoutineBody{}, err
		}
		return ast.MySQLRoutineBody{SQL: bodySQL}, nil
	case p.tokens[bodyIdx].MatchIdentifierValue("RETURN"):
		bodySQL := strings.TrimSpace(p.rawFragment(p.tokens[bodyIdx].Start, p.statementEndFrom(bodyIdx)))
		return ast.MySQLRoutineBody{SQL: bodySQL}, nil
	default:
		return ast.MySQLRoutineBody{}, fmt.Errorf("unsupported MySQL routine body at position %d", p.tokens[bodyIdx].Start)
	}
}

func (p mysqlRoutineParser) collectCompoundBodySQL(beginIdx int) (string, error) {
	bodyStartIdx := beginIdx
	bodyLabel := ""
	if p.isOuterBodyLabel(beginIdx) {
		bodyLabel = p.tokens[beginIdx].Value
		colonIdx := p.nextSignificant(beginIdx + 1)
		beginIdx = p.nextSignificant(colonIdx + 1)
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
		trackRoutineCompoundKeyword(keyword, &depth, &caseDepth, &pendingEndTrailer)
		if depth == 0 && keyword == "END" {
			end := p.compoundBodyEnd(i, bodyLabel)
			return strings.TrimSpace(p.rawFragment(p.tokens[bodyStartIdx].Start, end)), nil
		}
	}

	return "", fmt.Errorf("unterminated MySQL routine body at position %d", p.tokens[beginIdx].Start)
}

func (p mysqlRoutineParser) compoundBodyEnd(endIdx int, bodyLabel string) int {
	nextIdx := p.nextSignificant(endIdx + 1)
	if bodyLabel != "" && nextIdx != -1 && strings.EqualFold(p.tokens[nextIdx].Value, bodyLabel) {
		return p.tokens[nextIdx].End
	}
	return p.tokens[endIdx].End
}

func (p mysqlRoutineParser) statementEndFrom(startIdx int) int {
	for i := startIdx; i < len(p.tokens); i++ {
		if p.tokens[i].Type == lexer.TokenSemicolon || p.tokens[i].Type == lexer.TokenEOF {
			return p.tokens[i].Start
		}
	}
	return len(p.input)
}

func (p mysqlRoutineParser) isScalarIF(idx int) bool {
	nextIdx := p.nextSignificant(idx + 1)
	return isScalarMySQLRoutineIF(p.tokens, nextIdx)
}

func isScalarMySQLRoutineIF(tokens []lexer.Token, openParenIdx int) bool {
	if openParenIdx == -1 || openParenIdx >= len(tokens) || !tokens[openParenIdx].MatchOperatorValue("(") {
		return false
	}

	closeParenIdx := matchingMySQLRoutineParen(tokens, openParenIdx)
	if closeParenIdx == -1 {
		return true
	}

	nextIdx := nextSignificantMySQLRoutineToken(tokens, closeParenIdx+1)
	return nextIdx == -1 || !tokens[nextIdx].MatchIdentifierValue("THEN")
}

func matchingMySQLRoutineParen(tokens []lexer.Token, openParenIdx int) int {
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

func (p mysqlRoutineParser) findFirstIdentifier(value string) int {
	for i, tok := range p.tokens {
		if tok.MatchIdentifierValue(value) {
			return i
		}
	}
	return -1
}

func (p mysqlRoutineParser) findIdentifierBetween(value string, startIdx, endIdx int) int {
	for i := startIdx; i < endIdx && i < len(p.tokens); i++ {
		if p.tokens[i].MatchIdentifierValue(value) {
			return i
		}
	}
	return -1
}

func (p mysqlRoutineParser) nextSignificant(startIdx int) int {
	return nextSignificantMySQLRoutineToken(p.tokens, startIdx)
}

func nextSignificantMySQLRoutineToken(tokens []lexer.Token, startIdx int) int {
	for i := max(startIdx, 0); i < len(tokens); i++ {
		if !isMySQLRoutineTrivia(tokens[i]) && tokens[i].Type != lexer.TokenEOF {
			return i
		}
	}
	return -1
}

func isMySQLRoutineTrivia(tok lexer.Token) bool {
	return tok.Type == lexer.TokenWhitespace || tok.Type == lexer.TokenComment
}

func (p mysqlRoutineParser) rawToken(idx int) string {
	if idx < 0 || idx >= len(p.tokens) {
		return ""
	}
	return p.tokens[idx].Value
}

func (p mysqlRoutineParser) rawFragment(start, end int) string {
	return rawMySQLRoutineFragment(p.input, start, end)
}

func rawMySQLRoutineFragment(input string, start, end int) string {
	if start < 0 || start > end || end > len(input) {
		return ""
	}
	return input[start:end]
}

func isMySQLRoutineCharacteristicStart(tok lexer.Token) bool {
	if tok.Type != lexer.TokenIdentifier {
		return false
	}
	return slices.Contains(mysqlRoutineCharacteristicKeywords, strings.ToUpper(tok.Value))
}

var mysqlRoutineCharacteristicKeywords = []string{
	"COMMENT",
	"CONTAINS",
	"DETERMINISTIC",
	"LANGUAGE",
	"MODIFIES",
	"NO",
	"NOT",
	"READS",
	"SQL",
}
