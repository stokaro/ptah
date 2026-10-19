package parser

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

type postgresRoutineParser struct{}

func (postgresRoutineParser) parseCreateRoutine(p *Parser, target string, statementStart int) (ast.Node, error) {
	if target == "PROCEDURE" {
		sql, err := p.collectPostgresRoutineStatement(statementStart)
		if err != nil {
			return nil, err
		}
		return parsePostgresRoutineSQL(sql, p.dialect, p.postgresRoutineLexerOptions(), ast.RoutineKindProcedure)
	}
	if target == "PROC" {
		return compatibilityRoutineParser{}.parseCreateRoutine(p, target, statementStart)
	}
	return p.parseCreateFunction(statementStart)
}

func (postgresRoutineParser) parseCreateDefinerRoutine(p *Parser, statementStart int) (ast.Node, error) {
	return compatibilityRoutineParser{}.parseCreateDefinerRoutine(p, statementStart)
}

// isPostgresRoutineDialect reports whether the dialect writes PostgreSQL's
// routine grammar, whose body is a string constant in a LANGUAGE.
func (p *Parser) isPostgresRoutineDialect() bool {
	return p.target.grammar.Routines.Body == descriptor.RoutineQuotedBody
}

// postgresRoutineLexerOptions are the string rules of the head of the
// dialect's family, which a routine of PostgreSQL's grammar and its body
// follow whatever member of the family the document names: PostgreSQL's for
// every engine of its family. The dialect-blind lexer reads `E'...'` as an
// identifier and a string, which leaves a procedure written that way with no
// body at all, and it reads a backslash in a standard string as an escape
// (stokaro/ptah#3691).
func (p *Parser) postgresRoutineLexerOptions() lexer.Options {
	return dialectlexer.Options(p.target.family)
}

func (p *Parser) parsePostgresDoStatement(statementStart int) (ast.Node, error) {
	p.advance()
	sql, err := p.collectRawStatement(statementStart, "DO statement")
	if err != nil {
		return nil, err
	}

	block := ast.NewDoBlock(sql)
	body, language, err := parsePostgresDoBlockSQL(sql, p.postgresRoutineLexerOptions())
	if err != nil {
		return nil, err
	}
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

func parsePostgresDoBlockSQL(sql string, options lexer.Options) (ast.PostgresRoutineBody, string, error) {
	tokens := tokenizePostgresRoutineSQL(sql, options)
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
		return ast.PostgresRoutineBody{Language: language}, language, nil
	}

	bodyToken := tokens[bodyIdx].Value
	body, err := postgresRoutineBodyText(bodyToken, "DO", options)
	if err != nil {
		return ast.PostgresRoutineBody{}, "", err
	}
	return parsePostgresRoutineBody(body, language, dollarQuoteDelimiter(bodyToken)), language, nil
}

func parsePostgresRoutineSQL(sql, dialect string, options lexer.Options, kind ast.RoutineKind) (*ast.PostgresRoutineNode, error) {
	tokens := tokenizePostgresRoutineSQL(sql, options)
	routine := ast.NewPostgresRoutine(sql, dialect, kind)
	routine.Name, routine.Parameters = parsePostgresRoutineHeader(sql, tokens, strings.ToUpper(string(kind)))
	security, settings, err := parsePostgresRoutineClauses(tokens, strings.ToUpper(string(kind)))
	if err != nil {
		return nil, err
	}
	routine.Security, routine.Settings = security, settings

	bodyToken, language := parsePostgresRoutineBodyClause(tokens)
	routine.Language = strings.ToLower(language)
	if bodyToken != "" {
		body, err := postgresRoutineBodyText(bodyToken, "CREATE "+strings.ToUpper(string(kind)), options)
		if err != nil {
			return nil, err
		}
		routine.Body = parsePostgresRoutineBody(body, routine.Language, dollarQuoteDelimiter(bodyToken))
	} else if bodySQL := parsePostgresSQLBody(sql, tokens); bodySQL != "" {
		routine.Body = parsePostgresRoutineBody(bodySQL, routine.Language, "")
	}
	return routine, nil
}

// postgresRoutineBodyText answers the body a routine's string literal holds:
// the text between the quotes, with the doubled quotes of `'...'` and the
// escapes of `E'...'` undone. The body is written back between dollar quotes,
// where neither is an escape, so a body kept with its quoting is a statement
// the server refuses (stokaro/ptah#3691). A literal whose closing quote is
// missing is refused here rather than read as a body. options are the string
// rules the token was read with.
func postgresRoutineBodyText(token, statement string, options lexer.Options) (string, error) {
	body, ok := lexer.StringValue(token, options)
	if !ok {
		return "", fmt.Errorf("unsupported %s body: %s is not a complete string literal", statement, abbreviatedToken(token))
	}
	return body, nil
}

// abbreviatedToken shortens a token for an error message.
func abbreviatedToken(token string) string {
	const limit = 40
	if len(token) <= limit {
		return token
	}
	return token[:limit] + "..."
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

// parsePostgresRoutineClauses reads the SECURITY and SET clauses of a routine
// header, between its parameter list and its body, as the CREATE FUNCTION path
// reads them: the mode upper-cased, and each setting as `name=value` or `name
// FROM CURRENT` (see [Parser.parseFunctionSetting]). Read nowhere else, a
// procedure declared SECURITY DEFINER became one that runs with its caller's
// rights, and its search_path was dropped.
func parsePostgresRoutineClauses(tokens []lexer.Token, keyword string) (security string, settings []string, err error) {
	start := postgresRoutineParametersEnd(tokens, keyword)
	if start == -1 {
		return "", nil, nil
	}
	for i := nextPostgresRoutineToken(tokens, start); i != -1; i = nextPostgresRoutineToken(tokens, i+1) {
		switch {
		case tokens[i].MatchIdentifierValue("AS"), tokens[i].MatchIdentifierValue("BEGIN"):
			return security, settings, nil
		case tokens[i].MatchIdentifierValue("EXTERNAL"):
			// The SQL standard's spelling of SECURITY, read as the CREATE
			// FUNCTION path reads it.
			if next := nextPostgresRoutineToken(tokens, i+1); next == -1 || !tokens[next].MatchIdentifierValue("SECURITY") {
				return "", nil, fmt.Errorf("expected SECURITY after EXTERNAL at position %d", tokens[i].Start)
			}
		case tokens[i].MatchIdentifierValue("SECURITY"):
			modeIdx := nextPostgresRoutineToken(tokens, i+1)
			if modeIdx == -1 || tokens[modeIdx].Type != lexer.TokenIdentifier {
				return "", nil, fmt.Errorf("expected a security mode after SECURITY at position %d", tokens[i].Start)
			}
			security, i = strings.ToUpper(tokens[modeIdx].Value), modeIdx
		case tokens[i].MatchIdentifierValue("SET"):
			var setting string
			if setting, i, err = postgresRoutineSetting(tokens, i); err != nil {
				return "", nil, err
			}
			settings = append(settings, setting)
		}
	}
	return security, settings, nil
}

// postgresRoutineParametersEnd returns the index after the parameter list that
// follows keyword, or -1 when there is none.
func postgresRoutineParametersEnd(tokens []lexer.Token, keyword string) int {
	keywordIdx := slices.IndexFunc(tokens, func(tok lexer.Token) bool { return tok.MatchIdentifierValue(keyword) })
	if keywordIdx == -1 {
		return -1
	}
	depth := 0
	for i := keywordIdx + 1; i < len(tokens); i++ {
		switch {
		case tokens[i].MatchOperatorValue("("):
			depth++
		case tokens[i].MatchOperatorValue(")"):
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return -1
}

// postgresRoutineSetting reads the SET clause at setIdx and returns it with the
// index of its last token.
func postgresRoutineSetting(tokens []lexer.Token, setIdx int) (setting string, lastIdx int, err error) {
	nameIdx := nextPostgresRoutineToken(tokens, setIdx+1)
	if nameIdx == -1 || tokens[nameIdx].Type != lexer.TokenIdentifier {
		return "", -1, fmt.Errorf("expected a routine setting name at position %d", tokens[setIdx].Start)
	}
	name := tokens[nameIdx].Value
	opIdx := nextPostgresRoutineToken(tokens, nameIdx+1)
	switch {
	case opIdx != -1 && tokens[opIdx].MatchIdentifierValue("FROM"):
		currentIdx := nextPostgresRoutineToken(tokens, opIdx+1)
		if currentIdx == -1 || !tokens[currentIdx].MatchIdentifierValue("CURRENT") {
			return "", -1, fmt.Errorf("expected CURRENT after FROM in the %s routine setting", name)
		}
		return name + " FROM CURRENT", currentIdx, nil
	case opIdx != -1 && (tokens[opIdx].MatchOperatorValue("=") || tokens[opIdx].MatchIdentifierValue("TO")):
	default:
		return "", -1, fmt.Errorf("expected =, TO or FROM CURRENT in the %s routine setting", name)
	}
	var value strings.Builder
	lastIdx = opIdx
	for i := opIdx + 1; i < len(tokens); i++ {
		tok := tokens[i]
		if tok.Type == lexer.TokenEOF || tok.Type == lexer.TokenSemicolon ||
			tok.Type == lexer.TokenIdentifier && endsRoutineSettingValue(tok.Value) {
			break
		}
		if !isPostgresRoutineTrivia(tok) {
			value.WriteString(tok.Value)
			lastIdx = i
		}
	}
	if value.Len() == 0 {
		return "", -1, fmt.Errorf("expected a value for the %s routine setting", name)
	}
	return name + "=" + value.String(), lastIdx, nil
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

// parsePostgresRoutineBody records a body as the routine writes it. What its
// statements are is the PostgreSQL owner's to read (dialect/postgres/pgroutine),
// through the runtime's routine body reader.
func parsePostgresRoutineBody(sql, language, delimiter string) ast.PostgresRoutineBody {
	return ast.PostgresRoutineBody{
		SQL:       sql,
		Delimiter: delimiter,
		Language:  strings.ToLower(language),
	}
}

// postgresRoutineTokenizer is the PostgreSQL routine sub-language tokenization
// boundary. It intentionally reuses the shared SQL token primitives for string,
// comment, and identifier handling, while keeping routine-body parsing separate
// from the generic SQL statement parser.
type postgresRoutineTokenizer struct {
	lexer *lexer.Lexer
}

func newPostgresRoutineTokenizer(sql string, options lexer.Options) postgresRoutineTokenizer {
	return postgresRoutineTokenizer{lexer: lexer.NewLexerWithOptions(sql, options)}
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

func tokenizePostgresRoutineSQL(sql string, options lexer.Options) []lexer.Token {
	return newPostgresRoutineTokenizer(sql, options).tokens()
}

func nextPostgresRoutineToken(tokens []lexer.Token, startIdx int) int {
	for i := max(startIdx, 0); i < len(tokens); i++ {
		if !isPostgresRoutineTrivia(tokens[i]) && tokens[i].Type != lexer.TokenEOF {
			return i
		}
	}
	return -1
}

func isPostgresRoutineTrivia(tok lexer.Token) bool {
	return tok.Type == lexer.TokenWhitespace || tok.Type == lexer.TokenComment
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
