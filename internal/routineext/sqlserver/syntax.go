package sqlserver

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/lexer"
	"ptah.run/internal/routineext"
	"ptah.run/internal/sqlcompound"
)

func parseRoutine(p routineext.Cursor, statementStart int, kind ast.RoutineKind) (ast.Node, error) {
	mode := sqlServerRoutineKeepTopLevelSemicolon
	if kind == ast.RoutineKindFunction {
		mode = sqlServerRoutineStopAtTopLevelSemicolon
	}
	sql, err := collect(p, statementStart, mode)
	if err != nil {
		return nil, err
	}
	routine, options, err := parseSQLServerRoutineSQL(sql, p.Dialect(), kind)
	if err != nil {
		return nil, err
	}
	p.ClaimOptions(routine, options, statementStart+leadingSpace(p.Input()[statementStart:]))
	return routine, nil
}

// leadingSpace is the length of the white space text opens with, which a
// statement's collected text leaves out.
func leadingSpace(text string) int {
	return len(text) - len(strings.TrimLeft(text, " \t\r\n"))
}

func parseSQLServerRoutineSQL(sql, dialect string, kind ast.RoutineKind) (*ast.RoutineNode, []routineext.OptionSpan, error) {
	tokens := newSQLServerRoutineTokenizer(sql).tokens()
	routine := ast.NewRoutine(sql, dialect, kind)
	asIdx := findSQLServerRoutineBodyBoundary(tokens, kind)
	headerEnd, options, err := parseSQLServerRoutineOptions(sql, tokens, kind, asIdx)
	if err != nil {
		return nil, nil, err
	}
	routine.Name, routine.Parameters, routine.Returns = parseSQLServerRoutineHeader(sql, tokens, kind, headerEnd)
	for _, option := range options {
		routine.Options = append(routine.Options, ast.RoutineOption{Text: option.Text})
	}
	routine.Body = parseSQLServerRoutineBody(sql, tokens, asIdx)
	return routine, options, nil
}

// parseSQLServerRoutineOptions reads what a routine header writes between its
// signature and the AS that opens its body: a WITH list, such as `WITH
// RECOMPILE, EXECUTE AS OWNER`, and FOR REPLICATION. It returns where the
// signature ends and each option. A header with neither ends at asIdx.
func parseSQLServerRoutineOptions(sql string, tokens []lexer.Token, kind ast.RoutineKind, asIdx int) (headerEnd int, options []routineext.OptionSpan, err error) {
	if asIdx == -1 {
		return -1, nil, nil
	}
	start := sqlServerRoutineSignatureStart(tokens, kind)
	withIdx, forIdx := -1, -1
	depth := 0
	for i := start; i < asIdx; i++ {
		switch {
		case tokens[i].MatchOperatorValue("["):
			i = sqlServerRoutineBracketEnd(tokens, i)
		case tokens[i].MatchOperatorValue("("):
			depth++
		case tokens[i].MatchOperatorValue(")"):
			depth--
		case depth != 0, sqlServerRoutinePreviousSignificantIsOperator(tokens, i, "@"):
		case withIdx == -1 && tokens[i].MatchIdentifierValue("WITH"):
			withIdx = i
		case tokens[i].MatchIdentifierValue("FOR"):
			if next := nextSQLServerRoutineToken(tokens, i+1); next != -1 && tokens[next].MatchIdentifierValue("REPLICATION") {
				forIdx = i
			}
		}
	}
	headerEnd = asIdx
	if forIdx != -1 {
		headerEnd = forIdx
	}
	if withIdx != -1 {
		headerEnd = withIdx
		end := asIdx
		if forIdx != -1 {
			end = forIdx
		}
		options, err = sqlServerRoutineOptionList(sql, tokens, withIdx+1, end)
		if err != nil {
			return 0, nil, err
		}
	}
	if forIdx != -1 {
		options = append(options, routineext.OptionSpan{Text: "FOR REPLICATION", Source: "FOR REPLICATION",
			Words: []string{"FOR", "REPLICATION"}, Start: tokens[forIdx].Start})
	}
	return headerEnd, options, nil
}

// sqlServerRoutineSignatureStart returns the index of the routine's name, the
// first token a header option can follow.
func sqlServerRoutineSignatureStart(tokens []lexer.Token, kind ast.RoutineKind) int {
	keywordIdx := findSQLServerRoutineKeyword(tokens, strings.ToUpper(string(kind)), 0)
	if keywordIdx == -1 && kind == ast.RoutineKindProcedure {
		keywordIdx = findSQLServerRoutineKeyword(tokens, "PROC", 0)
	}
	return keywordIdx + 1
}

// sqlServerRoutineOptionList splits the WITH list between start and end at its
// commas.
func sqlServerRoutineOptionList(sql string, tokens []lexer.Token, start, end int) ([]routineext.OptionSpan, error) {
	var options []routineext.OptionSpan
	var current routineext.OptionSpan
	first, last := -1, -1
	flush := func() error {
		if len(current.Words) == 0 {
			return fmt.Errorf("SQL Server routine WITH list contains an empty option")
		}
		current.Source = sql[tokens[first].Start:tokens[last].End]
		current.Start = tokens[first].Start
		options = append(options, current)
		current, first, last = routineext.OptionSpan{}, -1, -1
		return nil
	}
	for i := start; i < end; i++ {
		tok := tokens[i]
		switch {
		case isSQLServerRoutineTrivia(tok), tok.Type == lexer.TokenEOF:
			continue
		case tok.MatchOperatorValue(","):
			if err := flush(); err != nil {
				return nil, err
			}
			continue
		}
		if first == -1 {
			first = i
		}
		last = i
		current.Words = append(current.Words, tok.Value)
		word := strings.ToUpper(tok.Value)
		if tok.Type == lexer.TokenString {
			word = tok.Value
		}
		current.Text = strings.TrimSpace(current.Text + " " + word)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return options, nil
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

// findSQLServerRoutineBodyBoundary finds the AS that opens the body, or a
// function's BEGIN or RETURN when its optional AS is absent. It skips the AS
// in EXECUTE AS and a parameter's optional AS, as in @a AS INT.
func findSQLServerRoutineBodyBoundary(tokens []lexer.Token, kind ast.RoutineKind) int {
	if kind == ast.RoutineKindFunction {
		if returnsIdx := findSQLServerRoutineKeyword(tokens, "RETURNS", 0); returnsIdx != -1 {
			return findSQLServerRoutineBodyBoundaryFrom(tokens, returnsIdx+1, true)
		}
	}
	return findSQLServerRoutineBodyBoundaryFrom(tokens, sqlServerRoutineSignatureStart(tokens, kind), false)
}

func findSQLServerRoutineBodyBoundaryFrom(tokens []lexer.Token, startIdx int, optionalAS bool) int {
	depth := 0
	for i := max(startIdx, 0); i < len(tokens); i++ {
		switch {
		case tokens[i].MatchOperatorValue("["):
			i = sqlServerRoutineBracketEnd(tokens, i)
		case tokens[i].MatchOperatorValue("("):
			depth++
		case tokens[i].MatchOperatorValue(")") && depth > 0:
			depth--
		case depth == 0 && tokens[i].MatchIdentifierValue("AS") && opensSQLServerRoutineBody(tokens, i):
			return i
		case depth == 0 && optionalAS && !sqlServerRoutinePreviousSignificantIsOperator(tokens, i, "@") &&
			(tokens[i].MatchIdentifierValue("BEGIN") || tokens[i].MatchIdentifierValue("RETURN")):
			return i
		}
	}
	return -1
}

// opensSQLServerRoutineBody reports whether the AS at idx opens the body rather
// than belonging to EXECUTE AS, to a parameter's type, or being a variable
// named as.
func opensSQLServerRoutineBody(tokens []lexer.Token, idx int) bool {
	prev := previousSQLServerRoutineToken(tokens, idx)
	if prev == -1 {
		return true
	}
	if tokens[prev].MatchOperatorValue("@") || tokens[prev].MatchIdentifierValue("EXECUTE") {
		return false
	}
	return !sqlServerRoutinePreviousSignificantIsOperator(tokens, prev, "@")
}

func previousSQLServerRoutineToken(tokens []lexer.Token, idx int) int {
	for i := idx - 1; i >= 0; i-- {
		if !isSQLServerRoutineTrivia(tokens[i]) {
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

func parseSQLServerRoutineBody(sql string, tokens []lexer.Token, asIdx int) ast.RoutineBody {
	if asIdx == -1 {
		return ast.RoutineBody{}
	}
	bodyStartIdx := asIdx
	if tokens[asIdx].MatchIdentifierValue("AS") {
		bodyStartIdx = nextSQLServerRoutineToken(tokens, asIdx+1)
	}
	if bodyStartIdx == -1 {
		return ast.RoutineBody{}
	}
	return ast.RoutineBody{SQL: strings.TrimSpace(sql[tokens[bodyStartIdx].Start:sqlServerRoutineStatementEnd(sql, tokens)])}
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

func skipSQLServerBracketedIdentifier(p routineext.Cursor) {
	p.Advance()
	for !p.AtEnd() {
		if p.Current().MatchOperatorValue("]") {
			p.Advance()
			if p.Current().MatchOperatorValue("]") {
				p.Advance()
				continue
			}
			return
		}
		p.Advance()
	}
}

func collect(p routineext.Cursor, statementStart int, semicolonMode sqlServerRoutineSemicolonMode) (string, error) {
	blockDepth := 0
	caseDepth := 0
	for !p.AtEnd() {
		if err := p.CheckTimeout(); err != nil {
			return "", err
		}

		if p.Current().MatchOperatorValue("[") {
			skipSQLServerBracketedIdentifier(p)
			continue
		}

		if p.Current().Type == lexer.TokenSemicolon &&
			blockDepth == 0 &&
			semicolonMode == sqlServerRoutineStopAtTopLevelSemicolon {
			sql := p.Statement(statementStart)
			p.Advance()
			return sql, nil
		}
		if p.BatchSeparator() && blockDepth == 0 {
			sql := p.Fragment(statementStart, p.Current().Start)
			p.Advance()
			return sql, nil
		}

		if p.Current().Type == lexer.TokenIdentifier && !p.Previous().MatchOperatorValue("@") {
			nextValue := sqlServerRawRoutineNextSignificantValue(p.Input(), p.Current().End)
			if complete := trackSQLServerRawRoutineKeyword(strings.ToUpper(p.Current().Value), nextValue, &blockDepth, &caseDepth); complete {
				end := p.Current().End
				p.Advance()
				return p.Fragment(statementStart, end), nil
			}
		}
		p.Advance()
	}

	if blockDepth > 0 {
		return "", fmt.Errorf("unterminated CREATE routine body at position %d", p.Current().Start)
	}
	return p.Fragment(statementStart, p.Previous().End), nil
}

func trackSQLServerRawRoutineKeyword(keyword, nextValue string, blockDepth, caseDepth *int) bool {
	switch keyword {
	case "BEGIN":
		if !sqlcompound.SQLServerBeginOpensBlock(nextValue) {
			return false
		}
		(*blockDepth)++
	case "CASE":
		(*caseDepth)++
	case "END":
		if !sqlcompound.SQLServerEndClosesBlock(nextValue) {
			return false
		}
		if *caseDepth > 0 {
			(*caseDepth)--
		} else if *blockDepth > 0 {
			(*blockDepth)--
			return *blockDepth == 0
		}
	}
	return false
}

func sqlServerRawRoutineNextSignificantValue(input string, pos int) string {
	if pos >= len(input) {
		return ""
	}
	l := lexer.NewLexerWithOptions(input[pos:], lexer.Options{
		BracketIdentifiers:  true,
		DisableHashComments: true,
	})
	for {
		tok := l.NextToken()
		switch tok.Type {
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		case lexer.TokenEOF:
			return ""
		default:
			return strings.ToUpper(tok.Value)
		}
	}
}

type sqlServerRoutineSemicolonMode int

const (
	sqlServerRoutineStopAtTopLevelSemicolon sqlServerRoutineSemicolonMode = iota
	sqlServerRoutineKeepTopLevelSemicolon
)

// Raw collects the permissive PROC form, stopping at a top-level terminator.
func Raw(p routineext.Cursor, start int) (string, error) {
	return collect(p, start, sqlServerRoutineStopAtTopLevelSemicolon)
}

type syntax struct{}

func (syntax) Create(p routineext.Cursor, target string, statementStart int) (ast.Node, error) {
	switch target {
	case "FUNCTION":
		return parseRoutine(p, statementStart, ast.RoutineKindFunction)
	case "PROC", "PROCEDURE":
		return parseRoutine(p, statementStart, ast.RoutineKindProcedure)
	}
	return p.Fallback(target, statementStart)
}

func (syntax) Definer(p routineext.Cursor, statementStart int) (ast.Node, error) {
	return p.DefinerFallback(statementStart)
}
