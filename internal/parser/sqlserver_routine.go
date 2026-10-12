package parser

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/sqlext"
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
	routine, options, err := parseSQLServerRoutineSQL(sql, p.dialect, kind)
	if err != nil {
		return nil, err
	}
	p.claimOwnerRoutineOptions(routine, options, statementStart+leadingSpace(p.input[statementStart:]))
	return routine, nil
}

// leadingSpace is the length of the white space text opens with, which a
// statement's collected text leaves out.
func leadingSpace(text string) int {
	return len(text) - len(strings.TrimLeft(text, " \t\r\n"))
}

func parseSQLServerRoutineSQL(sql, dialect string, kind ast.RoutineKind) (*ast.SQLServerRoutineNode, []routineOptionSpan, error) {
	tokens := newSQLServerRoutineTokenizer(sql).tokens()
	routine := ast.NewSQLServerRoutine(sql, dialect, kind)
	asIdx := findSQLServerRoutineBodyBoundary(tokens, kind)
	headerEnd, options, err := parseSQLServerRoutineOptions(sql, tokens, kind, asIdx)
	if err != nil {
		return nil, nil, err
	}
	routine.Name, routine.Parameters, routine.Returns = parseSQLServerRoutineHeader(sql, tokens, kind, headerEnd)
	for _, option := range options {
		routine.Options = append(routine.Options, ast.SQLServerRoutineOption{Text: option.text})
	}
	routine.Form = classifySQLServerRoutineForm(routine)
	routine.Body = parseSQLServerRoutineBody(sql, tokens, asIdx)
	return routine, options, nil
}

// routineOptionSpan is one option of a routine header: its normalized text,
// the words an owner's pattern is matched against, its source text and where
// that starts in the statement.
type routineOptionSpan struct {
	text, source string
	words        []string
	start        int
}

// claimOwnerRoutineOptions offers each option of a routine header to the
// owners whose patterns stand at [sqlext.RoutineOption], and records the
// claims for the owners to read once the document is parsed. The claims of
// one routine share its number, so an owner combines them into one facet. An option an
// owner claims is marked read, and the facets its owner reads join the
// routine's.
func (p *Parser) claimOwnerRoutineOptions(routine *ast.SQLServerRoutineNode, options []routineOptionSpan, offset int) {
	candidates := p.owners.Candidates(p.dialect, sqlext.RoutineOption)
	p.definitions++
	for i, option := range options {
		for _, candidate := range candidates {
			if !candidate.Matches(option.words) {
				continue
			}
			p.ownerClaims = append(p.ownerClaims, ownerClaim{
				match: sqlext.Match{Candidate: candidate, Claim: sqlext.Claim{
					Pattern: candidate.Pattern.Name, Site: sqlext.RoutineOption, Subject: routine.Name,
					Text: option.source, Definition: p.definitions, Offset: offset + option.start,
				}},
				apply: func(value sqlext.Value) error {
					merged, err := routine.Facets.Merge(value.Facets)
					if err != nil {
						return fmt.Errorf("%s %s: %w", routine.Kind, routine.Name, err)
					}
					routine.Facets = merged
					routine.Options[i].Read = true
					return nil
				},
			})
			break
		}
	}
}

// parseSQLServerRoutineOptions reads what a routine header writes between its
// signature and the AS that opens its body: a WITH list, such as `WITH
// RECOMPILE, EXECUTE AS OWNER`, and FOR REPLICATION. It returns where the
// signature ends and each option. A header with neither ends at asIdx.
func parseSQLServerRoutineOptions(sql string, tokens []lexer.Token, kind ast.RoutineKind, asIdx int) (headerEnd int, options []routineOptionSpan, err error) {
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
		options = append(options, routineOptionSpan{text: "FOR REPLICATION", source: "FOR REPLICATION",
			words: []string{"FOR", "REPLICATION"}, start: tokens[forIdx].Start})
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
func sqlServerRoutineOptionList(sql string, tokens []lexer.Token, start, end int) ([]routineOptionSpan, error) {
	var options []routineOptionSpan
	var current routineOptionSpan
	first, last := -1, -1
	flush := func() error {
		if len(current.words) == 0 {
			return fmt.Errorf("SQL Server routine WITH list contains an empty option")
		}
		current.source = sql[tokens[first].Start:tokens[last].End]
		current.start = tokens[first].Start
		options = append(options, current)
		current, first, last = routineOptionSpan{}, -1, -1
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
		current.words = append(current.words, tok.Value)
		word := strings.ToUpper(tok.Value)
		if tok.Type == lexer.TokenString {
			word = tok.Value
		}
		current.text = strings.TrimSpace(current.text + " " + word)
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
	bodyStartIdx := asIdx
	if tokens[asIdx].MatchIdentifierValue("AS") {
		bodyStartIdx = nextSQLServerRoutineToken(tokens, asIdx+1)
	}
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
