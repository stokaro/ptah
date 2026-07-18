// Package parser provides token-to-AST parsing logic.
package parser

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/lexer"
)

// Parser converts SQL tokens into AST nodes.
//
// The parser takes a stream of tokens from the lexer and builds an Abstract Syntax Tree
// representation of SQL DDL statements. It supports CREATE TABLE, ALTER TABLE, CREATE INDEX,
// and other DDL operations.
type Parser struct {
	lexer     *lexer.Lexer
	input     string
	current   lexer.Token
	previous  lexer.Token
	startTime time.Time
	timeout   time.Duration
}

// NewParser creates a new parser with the given SQL input.
//
// The parser initializes with a lexer and advances to the first token.
//
// Example:
//
//	parser := NewParser("CREATE TABLE users (id INTEGER PRIMARY KEY);")
func NewParser(input string) *Parser {
	normalized := normalizeClientDelimiters(input)
	l := lexer.NewLexer(normalized)
	p := &Parser{
		lexer:     l,
		input:     normalized,
		startTime: time.Now(),
		timeout:   30 * time.Second, // 30 second timeout to prevent infinite loops
	}
	p.advance() // Load the first token
	return p
}

// Parse parses the input SQL and returns a list of AST statements.
//
// This method parses multiple SQL statements separated by semicolons and returns
// them as a StatementList. Each statement is parsed according to its type
// (CREATE TABLE, ALTER TABLE, etc.).
//
// Returns an error if the SQL syntax is invalid or unsupported.
func (p *Parser) Parse() (*ast.StatementList, error) {
	statements := &ast.StatementList{
		Statements: make([]ast.Node, 0),
	}

	for !p.isAtEnd() {
		// Check for timeout to prevent infinite loops
		if err := p.checkTimeout(); err != nil {
			return nil, err
		}

		// Skip whitespace and comments
		if p.current.Type == lexer.TokenWhitespace || p.current.Type == lexer.TokenComment {
			p.advance()
			continue
		}

		// Skip empty statements (just semicolons)
		if p.current.Type == lexer.TokenSemicolon {
			p.advance()
			continue
		}

		stmt, err := p.parseStatement()
		if err != nil {
			return nil, err
		}

		if stmt != nil {
			statements.Statements = append(statements.Statements, stmt)
		}

		// Consume optional semicolon
		if p.current.Type == lexer.TokenSemicolon {
			p.advance()
		}
	}

	return statements, nil
}

// parseStatement parses a single SQL statement based on the current token.
func (p *Parser) parseStatement() (ast.Node, error) {
	if p.current.Type != lexer.TokenIdentifier {
		return nil, fmt.Errorf("expected SQL keyword, got %s at position %d", p.current.Type, p.current.Start)
	}

	keyword := strings.ToUpper(p.current.Value)
	switch keyword {
	case "CREATE":
		return p.parseCreateStatement()
	case "ALTER":
		return p.parseAlterStatement()
	case "COMMENT":
		return p.parseCommentStatement()
	case "DROP":
		return p.parseDropStatement()
	case "DO":
		return p.parseDoStatement()
	case "GO":
		// SQL Server tooling uses GO as a batch separator, not as schema DDL.
		p.skipGoBatchSeparator()
		return nil, nil
	case "ANALYZE", "BEGIN", "CALL", "COMMIT", "DELETE", "INSERT", "PRAGMA", "REINDEX", "ROLLBACK", "SELECT", "SET", "SHOW", "UPDATE", "USE", "VACUUM", "WITH":
		p.skipSchemaNeutralStatement()
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported SQL statement: %s at position %d", keyword, p.current.Start)
	}
}

func (p *Parser) parseDoStatement() (ast.Node, error) {
	statementStart := p.current.Start
	p.advance()
	sql, err := p.collectRawStatement(statementStart, "DO statement")
	if err != nil {
		return nil, err
	}
	return ast.NewRawSQL(sql), nil
}

func (p *Parser) collectRawStatement(statementStart int, label string) (string, error) {
	for !p.isAtEnd() {
		if err := p.checkTimeout(); err != nil {
			return "", err
		}
		if p.current.Type == lexer.TokenSemicolon {
			sql := p.rawStatement(statementStart)
			p.advance()
			return sql, nil
		}
		p.advance()
	}
	return "", fmt.Errorf("unterminated %s at position %d", label, p.current.Start)
}

// parseCreateStatement parses CREATE statements (TABLE, INDEX, TYPE).
func (p *Parser) parseCreateStatement() (ast.Node, error) {
	statementStart := p.current.Start
	if err := p.expect(lexer.TokenIdentifier, "CREATE"); err != nil {
		return nil, err
	}

	p.skipWhitespace()

	if p.current.Type != lexer.TokenIdentifier {
		return nil, fmt.Errorf("expected CREATE target (TABLE, VIEW, FUNCTION, TRIGGER, INDEX, TYPE, DOMAIN, SCHEMA, DATABASE, EXTENSION), got %s at position %d", p.current.Type, p.current.Start)
	}

	target := strings.ToUpper(p.current.Value)
	switch target {
	case "SCHEMA":
		return p.parseCreateSchema()
	case "DATABASE":
		return p.parseCreateDatabase()
	case "EXTENSION":
		return p.parseCreateExtension()
	case "TABLE":
		return p.parseCreateTable()
	case "VIEW":
		return p.parseCreateView()
	case "FUNCTION", "PROCEDURE":
		return p.parseCreateRoutine(target, statementStart)
	case "DEFINER":
		return p.parseCreateDefinerRoutine(statementStart)
	case "TRIGGER":
		return p.parseCreateTrigger(statementStart)
	case "INDEX":
		return p.parseCreateIndex()
	case "FULLTEXT", "SPATIAL":
		p.advance()
		p.skipWhitespace()
		if err := p.expect(lexer.TokenIdentifier, "INDEX"); err != nil {
			return nil, fmt.Errorf("expected INDEX after %s: %w", target, err)
		}
		return p.parseCreateIndexAfterKeyword(target)
	case "OR":
		return p.parseCreateOrReplaceStatement(statementStart)
	case "TEMP", "TEMPORARY":
		return p.parseCreateTemporary(target, statementStart)
	case "UNIQUE":
		// Handle CREATE UNIQUE INDEX
		p.advance()
		p.skipWhitespace()
		if err := p.expect(lexer.TokenIdentifier, "INDEX"); err != nil {
			return nil, err
		}
		return p.parseCreateUniqueIndex()
	case "TYPE":
		return p.parseCreateType()
	case "DOMAIN":
		return p.parseCreateDomain()
	default:
		return nil, fmt.Errorf("unsupported CREATE target: %s at position %d", target, p.current.Start)
	}
}

func (p *Parser) parseCreateRoutine(target string, statementStart int) (ast.Node, error) {
	if target == "PROCEDURE" {
		return p.parseCreateRawRoutineStatement(statementStart)
	}
	return p.parseCreateFunction(statementStart)
}

func (p *Parser) parseCreateDefinerRoutine(statementStart int) (ast.Node, error) {
	for !p.isAtEnd() && p.current.Type != lexer.TokenSemicolon {
		if err := p.checkTimeout(); err != nil {
			return nil, err
		}
		if p.current.MatchIdentifierValue("FUNCTION") || p.current.MatchIdentifierValue("PROCEDURE") {
			return p.parseCreateRawRoutineStatement(statementStart)
		}
		p.advance()
	}
	return nil, fmt.Errorf("unsupported CREATE DEFINER target at position %d", p.current.Start)
}

func (p *Parser) parseCreateTemporary(target string, statementStart int) (ast.Node, error) {
	p.advance()
	p.skipWhitespace()
	if p.current.MatchIdentifierValue("TRIGGER") {
		return p.parseCreateTrigger(statementStart)
	}
	return nil, fmt.Errorf("unsupported CREATE %s target: %s at position %d", target, p.current.Value, p.current.Start)
}

func (p *Parser) parseCreateSchema() (*ast.CreateSchemaNode, error) {
	if err := p.expect(lexer.TokenIdentifier, "SCHEMA"); err != nil {
		return nil, err
	}
	name, ifNotExists, err := p.parseCreateNamespaceName("schema name")
	if err != nil {
		return nil, err
	}
	return &ast.CreateSchemaNode{Name: name, IfNotExists: ifNotExists}, nil
}

func (p *Parser) parseCreateDatabase() (*ast.CreateDatabaseNode, error) {
	if err := p.expect(lexer.TokenIdentifier, "DATABASE"); err != nil {
		return nil, err
	}
	name, ifNotExists, err := p.parseCreateNamespaceName("database name")
	if err != nil {
		return nil, err
	}
	return &ast.CreateDatabaseNode{Name: name, IfNotExists: ifNotExists}, nil
}

func (p *Parser) parseCreateExtension() (*ast.ExtensionNode, error) {
	if err := p.expect(lexer.TokenIdentifier, "EXTENSION"); err != nil {
		return nil, err
	}

	p.skipWhitespace()
	ifNotExists, err := p.parseOptionalIfNotExists()
	if err != nil {
		return nil, err
	}

	p.skipWhitespace()
	name, err := p.expectIdentifier()
	if err != nil {
		return nil, fmt.Errorf("expected extension name: %w", err)
	}

	extension := ast.NewExtension(name)
	if ifNotExists {
		extension.SetIfNotExists()
	}

	p.skipWhitespace()
	if p.current.MatchIdentifierValue("VERSION") {
		version, err := p.parseCreateExtensionVersion()
		if err != nil {
			return nil, err
		}
		extension.SetVersion(version)
	}

	return extension, nil
}

func (p *Parser) parseCreateExtensionVersion() (string, error) {
	p.advance()
	p.skipWhitespace()

	if p.current.Type != lexer.TokenString && p.current.Type != lexer.TokenIdentifier {
		return "", fmt.Errorf("expected extension version, got %s at position %d", p.current.Type, p.current.Start)
	}

	version := strings.Trim(p.current.Value, "'\"")
	p.advance()
	return version, nil
}

func (p *Parser) parseCreateNamespaceName(label string) (string, bool, error) {
	p.skipWhitespace()
	ifNotExists, err := p.parseOptionalIfNotExists()
	if err != nil {
		return "", false, err
	}
	p.skipWhitespace()
	name, err := p.expectIdentifier()
	if err != nil {
		return "", false, fmt.Errorf("expected %s: %w", label, err)
	}
	return name, ifNotExists, nil
}

func (p *Parser) parseOptionalIfNotExists() (bool, error) {
	if !p.current.MatchIdentifierValue("IF") {
		return false, nil
	}
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "NOT"); err != nil {
		return false, fmt.Errorf("expected NOT after IF: %w", err)
	}
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "EXISTS"); err != nil {
		return false, fmt.Errorf("expected EXISTS after IF NOT: %w", err)
	}
	return true, nil
}

func (p *Parser) parseCreateOrReplaceStatement(statementStart int) (ast.Node, error) {
	if err := p.expect(lexer.TokenIdentifier, "OR"); err != nil {
		return nil, err
	}
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "REPLACE"); err != nil {
		return nil, fmt.Errorf("expected REPLACE after CREATE OR: %w", err)
	}
	p.skipWhitespace()
	if p.current.MatchIdentifierValue("VIEW") {
		return p.parseCreateOrReplaceView()
	}
	if p.current.MatchIdentifierValue("FUNCTION") {
		return p.parseCreateFunction(statementStart)
	}
	if p.current.MatchIdentifierValue("PROCEDURE") {
		return p.parseCreateRawRoutineStatement(statementStart)
	}
	if p.current.MatchIdentifierValue("TRIGGER") {
		node, err := p.parseCreateTrigger(statementStart)
		if err != nil {
			return nil, err
		}
		trigger, ok := node.(*ast.CreateTriggerNode)
		if !ok {
			return node, nil
		}
		return trigger.SetReplace(), nil
	}
	return nil, fmt.Errorf("unsupported CREATE OR REPLACE target: %s at position %d", p.current.Value, p.current.Start)
}

// advance moves to the next token.
func (p *Parser) advance() {
	p.previous = p.current
	p.current = p.lexer.NextToken()
}

// isAtEnd checks if we've reached the end of the input.
func (p *Parser) isAtEnd() bool {
	return p.current.Type == lexer.TokenEOF
}

// checkTimeout checks if parsing has exceeded the timeout limit.
func (p *Parser) checkTimeout() error {
	if time.Since(p.startTime) > p.timeout {
		return fmt.Errorf("parsing timeout exceeded (%v) - possible infinite loop at position %d", p.timeout, p.current.Start)
	}
	return nil
}

// expect consumes a token of the expected type and value, returning an error if it doesn't match.
func (p *Parser) expect(tokenType lexer.TokenType, value string) error {
	p.skipWhitespace()
	if p.current.Type != tokenType {
		return fmt.Errorf("expected %s, got %s at position %d", tokenType, p.current.Type, p.current.Start)
	}
	if value != "" && !strings.EqualFold(p.current.Value, value) {
		return fmt.Errorf("expected '%s', got '%s' at position %d", value, p.current.Value, p.current.Start)
	}
	p.advance()
	return nil
}

// expectIdentifier consumes an identifier token and returns its value.
func (p *Parser) expectIdentifier() (string, error) {
	if p.current.Type != lexer.TokenIdentifier && !isDoubleQuotedIdentifierToken(p.current) {
		return "", fmt.Errorf("expected identifier, got %s at position %d", p.current.Type, p.current.Start)
	}
	value := p.current.Value
	p.advance()
	return value, nil
}

func isDoubleQuotedIdentifierToken(tok lexer.Token) bool {
	return tok.Type == lexer.TokenString && strings.HasPrefix(tok.Value, `"`)
}

func (p *Parser) parseQualifiedIdentifier(label string) (string, error) {
	first, err := p.expectIdentifier()
	if err != nil {
		return "", fmt.Errorf("expected %s: %w", label, err)
	}

	var name strings.Builder
	name.WriteString(first)
	for p.current.Type == lexer.TokenOperator && p.current.Value == "." {
		name.WriteString(".")
		p.advance()
		p.skipWhitespace()
		part, err := p.expectIdentifier()
		if err != nil {
			return "", fmt.Errorf("expected identifier after '.' in %s: %w", label, err)
		}
		name.WriteString(part)
	}
	return name.String(), nil
}

// skipWhitespace skips whitespace and comment tokens.
func (p *Parser) skipWhitespace() {
	for p.current.Type == lexer.TokenWhitespace || p.current.Type == lexer.TokenComment {
		p.advance()
	}
}

func (p *Parser) skipSchemaNeutralStatement() {
	for !p.isAtEnd() {
		if p.current.Type == lexer.TokenSemicolon {
			p.advance()
			return
		}
		p.advance()
	}
}

func (p *Parser) skipGoBatchSeparator() {
	p.advance()
	p.skipWhitespace()
	if p.current.Type == lexer.TokenIdentifier && isNumeric(p.current.Value) {
		p.advance()
	}
}

// isNumeric checks if a string represents a numeric value.
func isNumeric(s string) bool {
	if s == "" {
		return false
	}

	for i, r := range s {
		if i == 0 && (r == '-' || r == '+') {
			continue
		}
		if r < '0' || r > '9' {
			if r == '.' {
				continue // Allow decimal points
			}
			return false
		}
	}
	return true
}

func (p *Parser) parseCreateView() (*ast.CreateViewNode, error) {
	return p.parseCreateViewNode()
}

func (p *Parser) parseCreateOrReplaceView() (*ast.CreateViewNode, error) {
	view, err := p.parseCreateViewNode()
	if err != nil {
		return nil, err
	}
	view.SetReplace()
	return view, nil
}

func (p *Parser) parseCreateFunction(statementStart int) (ast.Node, error) {
	if err := p.expect(lexer.TokenIdentifier, "FUNCTION"); err != nil {
		return nil, err
	}
	p.skipWhitespace()

	if p.isSQLServerBracketedFunctionNameStart() {
		return p.parseCreateRawSQLServerFunction(statementStart)
	}

	functionName, err := p.parseQualifiedIdentifier("function name")
	if err != nil {
		return nil, fmt.Errorf("unsupported CREATE FUNCTION syntax: %w", err)
	}

	p.skipWhitespace()
	parameters, err := p.collectParenthesizedBody("function parameters")
	if err != nil {
		return nil, err
	}

	function := ast.NewCreateFunction(functionName).SetParameters(parameters)
	raw, err := p.parseFunctionClauses(function)
	if err != nil {
		return nil, err
	}
	if raw {
		return ast.NewRawSQL(p.rawStatement(statementStart)), nil
	}
	return function, nil
}

func (p *Parser) isSQLServerBracketedFunctionNameStart() bool {
	// Bracketed function names enter T-SQL's routine-body sub-language. Until
	// #323 adds a dialect-aware body parser, preserve the statement as raw SQL.
	return p.current.MatchOperatorValue("[")
}

func (p *Parser) parseCreateRawSQLServerFunction(statementStart int) (ast.Node, error) {
	sql, err := p.collectRawSQLServerFunction(statementStart)
	if err != nil {
		return nil, err
	}
	return ast.NewRawSQL(sql), nil
}

func (p *Parser) collectRawSQLServerFunction(statementStart int) (string, error) {
	blockDepth := 0
	caseDepth := 0
	for !p.isAtEnd() {
		if err := p.checkTimeout(); err != nil {
			return "", err
		}

		if p.current.MatchOperatorValue("[") {
			p.skipSQLServerBracketedIdentifier()
			continue
		}

		if p.current.Type == lexer.TokenSemicolon && blockDepth == 0 {
			sql := p.rawStatement(statementStart)
			p.advance()
			return sql, nil
		}

		if p.current.Type == lexer.TokenIdentifier {
			if complete := trackSQLServerRawFunctionKeyword(strings.ToUpper(p.current.Value), &blockDepth, &caseDepth); complete {
				end := p.current.End
				p.advance()
				return p.rawStatementFragment(statementStart, end), nil
			}
		}
		p.advance()
	}

	if blockDepth > 0 {
		return "", fmt.Errorf("unterminated CREATE FUNCTION body at position %d", p.current.Start)
	}
	return p.rawStatementFragment(statementStart, p.previous.End), nil
}

func (p *Parser) skipSQLServerBracketedIdentifier() {
	p.advance()
	for !p.isAtEnd() {
		if p.current.MatchOperatorValue("]") {
			p.advance()
			if p.current.MatchOperatorValue("]") {
				p.advance()
				continue
			}
			return
		}
		p.advance()
	}
}

func trackSQLServerRawFunctionKeyword(keyword string, blockDepth, caseDepth *int) bool {
	switch keyword {
	case "BEGIN":
		(*blockDepth)++
	case "CASE":
		(*caseDepth)++
	case "END":
		if *caseDepth > 0 {
			(*caseDepth)--
		} else if *blockDepth > 0 {
			(*blockDepth)--
			return *blockDepth == 0
		}
	}
	return false
}

func (p *Parser) parseCreateRawRoutineStatement(statementStart int) (ast.Node, error) {
	sql, err := p.collectRawRoutineStatement(statementStart)
	if err != nil {
		return nil, err
	}
	return ast.NewRawSQL(sql), nil
}

func (p *Parser) collectRawRoutineStatement(statementStart int) (string, error) {
	blockDepth := 0
	caseDepth := 0
	pendingEndTrailer := false
	for !p.isAtEnd() {
		if err := p.checkTimeout(); err != nil {
			return "", err
		}

		if p.current.Type == lexer.TokenSemicolon && blockDepth == 0 {
			sql := p.rawStatementFragment(statementStart, p.current.Start)
			p.advance()
			return sql, nil
		}

		if p.current.Type == lexer.TokenIdentifier {
			p.trackCurrentRoutineCompoundKeyword(&blockDepth, &caseDepth, &pendingEndTrailer)
		}
		if p.current.Type == lexer.TokenSemicolon {
			pendingEndTrailer = false
		}

		p.advance()
	}

	if blockDepth > 0 {
		return "", fmt.Errorf("unterminated CREATE routine body at position %d", p.current.Start)
	}
	return p.rawStatementFragment(statementStart, p.previous.End), nil
}

func (p *Parser) collectParenthesizedBody(label string) (string, error) {
	if err := p.expect(lexer.TokenOperator, "("); err != nil {
		return "", fmt.Errorf("expected '(' for %s: %w", label, err)
	}

	var body strings.Builder
	depth := 1
	for depth > 0 && !p.isAtEnd() {
		if err := p.checkTimeout(); err != nil {
			return "", err
		}

		if p.current.Type == lexer.TokenOperator {
			switch p.current.Value {
			case "(":
				depth++
			case ")":
				depth--
				if depth == 0 {
					p.advance()
					return strings.TrimSpace(body.String()), nil
				}
			}
		}

		body.WriteString(p.current.Value)
		p.advance()
	}

	return "", fmt.Errorf("unterminated %s at position %d", label, p.current.Start)
}

func (p *Parser) parseFunctionClauses(function *ast.CreateFunctionNode) (bool, error) {
	for !p.isAtEnd() && p.current.Type != lexer.TokenSemicolon {
		if err := p.checkTimeout(); err != nil {
			return false, err
		}

		p.skipWhitespace()
		if p.isAtEnd() || p.current.Type == lexer.TokenSemicolon {
			return false, nil
		}
		if p.current.Type != lexer.TokenIdentifier {
			return false, fmt.Errorf("unsupported CREATE FUNCTION syntax: expected function clause, got %s at position %d", p.current.Type, p.current.Start)
		}

		raw, err := p.parseFunctionClause(function)
		if err != nil {
			return false, err
		}
		if raw {
			return true, nil
		}
	}
	return false, nil
}

func (p *Parser) parseFunctionClause(function *ast.CreateFunctionNode) (bool, error) {
	keyword := strings.ToUpper(p.current.Value)
	switch keyword {
	case "RETURNS":
		return false, p.parseFunctionReturns(function)
	case "RETURN":
		return false, p.parseFunctionReturnBody(function)
	case "BEGIN":
		return p.parseFunctionBeginBody(function)
	case "AS":
		return false, p.parseFunctionBody(function)
	case "LANGUAGE":
		return false, p.parseFunctionLanguage(function)
	case "SECURITY":
		return false, p.parseFunctionSecurity(function)
	case "IMMUTABLE", "STABLE", "VOLATILE":
		function.SetVolatility(keyword)
		p.advance()
		return false, nil
	default:
		if p.skipMySQLFunctionAttribute(keyword) {
			return false, nil
		}
		return false, fmt.Errorf("unsupported CREATE FUNCTION clause: %s at position %d", p.current.Value, p.current.Start)
	}
}

func (p *Parser) skipMySQLFunctionAttribute(keyword string) bool {
	switch keyword {
	case "DETERMINISTIC":
		p.advance()
		return true
	case "NOT":
		return p.skipOptionalFollowingKeyword("DETERMINISTIC")
	case "NO", "CONTAINS":
		return p.skipOptionalFollowingKeyword("SQL")
	case "READS", "MODIFIES":
		p.advance()
		p.skipWhitespace()
		if p.current.MatchIdentifierValue("SQL") {
			p.advance()
			p.skipWhitespace()
		}
		if p.current.MatchIdentifierValue("DATA") {
			p.advance()
		}
		return true
	case "SQL":
		p.advance()
		return true
	default:
		return false
	}
}

func (p *Parser) skipOptionalFollowingKeyword(keyword string) bool {
	p.advance()
	p.skipWhitespace()
	if p.current.MatchIdentifierValue(keyword) {
		p.advance()
	}
	return true
}

func (p *Parser) parseFunctionReturns(function *ast.CreateFunctionNode) error {
	if err := p.expect(lexer.TokenIdentifier, "RETURNS"); err != nil {
		return err
	}
	p.skipWhitespace()

	var returns strings.Builder
	for !p.isAtEnd() && p.current.Type != lexer.TokenSemicolon {
		if p.current.Type == lexer.TokenIdentifier {
			switch strings.ToUpper(p.current.Value) {
			case "AS", "BEGIN", "CONTAINS", "DETERMINISTIC", "LANGUAGE", "MODIFIES", "NO", "NOT", "READS", "RETURN", "SECURITY", "SQL", "IMMUTABLE", "STABLE", "VOLATILE":
				function.SetReturns(strings.TrimSpace(returns.String()))
				return nil
			}
		}
		returns.WriteString(p.current.Value)
		p.advance()
	}

	function.SetReturns(strings.TrimSpace(returns.String()))
	return nil
}

func (p *Parser) parseFunctionBody(function *ast.CreateFunctionNode) error {
	if err := p.expect(lexer.TokenIdentifier, "AS"); err != nil {
		return err
	}
	p.skipWhitespace()

	if p.current.Type != lexer.TokenString {
		return fmt.Errorf("unsupported CREATE FUNCTION body: expected string literal after AS, got %s at position %d", p.current.Type, p.current.Start)
	}

	body := p.current.Value
	function.SetBody(stripSQLStringDelimiters(body))
	p.advance()
	return nil
}

func (p *Parser) parseFunctionReturnBody(function *ast.CreateFunctionNode) error {
	if err := p.expect(lexer.TokenIdentifier, "RETURN"); err != nil {
		return err
	}
	p.skipWhitespace()

	var body strings.Builder
	for !p.isAtEnd() && p.current.Type != lexer.TokenSemicolon {
		if err := p.checkTimeout(); err != nil {
			return err
		}

		body.WriteString(p.current.Value)
		p.advance()
	}

	function.SetReturnBody(strings.TrimSpace(body.String()))
	return nil
}

func (p *Parser) parseFunctionBeginBody(function *ast.CreateFunctionNode) (bool, error) {
	body, atomic, err := p.collectFunctionBeginBody()
	if err != nil {
		return false, err
	}
	if atomic {
		function.SetAtomicBody(body)
		return false, nil
	}
	return true, nil
}

func (p *Parser) collectFunctionBeginBody() (string, bool, error) {
	if err := p.expect(lexer.TokenIdentifier, "BEGIN"); err != nil {
		return "", false, err
	}

	var body strings.Builder
	body.WriteString("BEGIN")

	p.skipWhitespace()
	atomic := false
	if p.current.MatchIdentifierValue("ATOMIC") {
		atomic = true
		body.WriteString(" ATOMIC")
		p.advance()
	}

	blockDepth := 1
	caseDepth := 0
	pendingEndTrailer := false
	for !p.isAtEnd() {
		if err := p.checkTimeout(); err != nil {
			return "", false, err
		}

		if p.current.Type == lexer.TokenIdentifier {
			p.trackCurrentRoutineCompoundKeyword(&blockDepth, &caseDepth, &pendingEndTrailer)

			body.WriteString(p.current.Value)
			p.advance()

			if blockDepth == 0 {
				return strings.TrimSpace(body.String()), atomic, nil
			}
			continue
		}

		if p.current.Type == lexer.TokenSemicolon {
			pendingEndTrailer = false
		}
		body.WriteString(p.current.Value)
		p.advance()
	}

	return "", false, fmt.Errorf("unterminated SQL function body at position %d", p.current.Start)
}

func (p *Parser) trackCurrentRoutineCompoundKeyword(blockDepth, caseDepth *int, pendingEndTrailer *bool) {
	keyword := strings.ToUpper(p.current.Value)
	if keyword == "IF" && p.current.End < len(p.input) && p.input[p.current.End] == '(' {
		// MySQL scalar IF(...) calls are not compound IF ... END IF blocks.
		return
	}
	trackRoutineCompoundKeyword(keyword, blockDepth, caseDepth, pendingEndTrailer)
}

func trackRoutineCompoundKeyword(keyword string, blockDepth, caseDepth *int, pendingEndTrailer *bool) {
	if *pendingEndTrailer {
		*pendingEndTrailer = false
		if isRoutineEndTrailerKeyword(keyword) {
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

func isRoutineEndTrailerKeyword(keyword string) bool {
	switch keyword {
	case "CASE", "IF", "LOOP", "REPEAT", "WHILE":
		return true
	default:
		return false
	}
}

func (p *Parser) rawStatement(start int) string {
	end := p.previous.End
	if p.current.Type == lexer.TokenSemicolon {
		end = p.current.End
	}
	return p.rawStatementFragment(start, end)
}

func (p *Parser) rawStatementFragment(start, end int) string {
	if start < 0 || start > end || end > len(p.input) {
		return ""
	}
	return strings.TrimSpace(p.input[start:end])
}

func (p *Parser) parseFunctionLanguage(function *ast.CreateFunctionNode) error {
	if err := p.expect(lexer.TokenIdentifier, "LANGUAGE"); err != nil {
		return err
	}
	p.skipWhitespace()

	language, err := p.expectIdentifier()
	if err != nil {
		return fmt.Errorf("expected function language: %w", err)
	}
	function.SetLanguage(language)
	return nil
}

func (p *Parser) parseFunctionSecurity(function *ast.CreateFunctionNode) error {
	if err := p.expect(lexer.TokenIdentifier, "SECURITY"); err != nil {
		return err
	}
	p.skipWhitespace()

	security, err := p.expectIdentifier()
	if err != nil {
		return fmt.Errorf("expected function security mode: %w", err)
	}
	function.SetSecurity(strings.ToUpper(security))
	return nil
}

func (p *Parser) parseCreateTrigger(statementStart int) (ast.Node, error) {
	if err := p.expect(lexer.TokenIdentifier, "TRIGGER"); err != nil {
		return nil, err
	}
	p.skipWhitespace()

	if p.current.MatchIdentifierValue("IF") {
		if err := p.parseTriggerIfNotExists(); err != nil {
			return nil, err
		}
		p.skipWhitespace()
	}

	triggerName, err := p.parseQualifiedIdentifier("trigger name")
	if err != nil {
		return nil, err
	}
	p.skipWhitespace()

	timing, err := p.parseTriggerTiming()
	if err != nil {
		return nil, err
	}
	p.skipWhitespace()

	event, err := p.parseTriggerEvent()
	if err != nil {
		return nil, err
	}
	p.skipWhitespace()

	if err := p.expect(lexer.TokenIdentifier, "ON"); err != nil {
		return nil, fmt.Errorf("expected ON after trigger event: %w", err)
	}
	p.skipWhitespace()

	tableName, err := p.parseQualifiedIdentifier("trigger table name")
	if err != nil {
		return nil, err
	}
	p.skipWhitespace()

	forEach, err := p.parseOptionalTriggerForEach()
	if err != nil {
		return nil, err
	}
	p.skipWhitespace()

	if p.current.MatchIdentifierValue("EXECUTE") {
		sql, err := p.collectRawStatement(statementStart, "CREATE TRIGGER statement")
		if err != nil {
			return nil, err
		}
		return ast.NewRawSQL(sql), nil
	}

	body, err := p.collectTriggerBody()
	if err != nil {
		return nil, err
	}

	return ast.NewCreateTrigger(triggerName, tableName).
		SetTiming(timing).
		SetEvent(event).
		SetForEach(forEach).
		SetBody(body), nil
}

func (p *Parser) parseTriggerIfNotExists() error {
	if err := p.expect(lexer.TokenIdentifier, "IF"); err != nil {
		return err
	}
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "NOT"); err != nil {
		return fmt.Errorf("expected NOT after IF: %w", err)
	}
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "EXISTS"); err != nil {
		return fmt.Errorf("expected EXISTS after IF NOT: %w", err)
	}
	return nil
}

func (p *Parser) parseTriggerTiming() (string, error) {
	switch {
	case p.current.MatchIdentifierValue("BEFORE"), p.current.MatchIdentifierValue("AFTER"):
		timing := strings.ToUpper(p.current.Value)
		p.advance()
		return timing, nil
	case p.current.MatchIdentifierValue("INSTEAD"):
		p.advance()
		p.skipWhitespace()
		if err := p.expect(lexer.TokenIdentifier, "OF"); err != nil {
			return "", fmt.Errorf("expected OF after INSTEAD in trigger timing: %w", err)
		}
		return "INSTEAD OF", nil
	default:
		return "", fmt.Errorf("expected trigger timing, got %s at position %d", p.current.Type, p.current.Start)
	}
}

func (p *Parser) parseTriggerEvent() (string, error) {
	switch {
	case p.current.MatchIdentifierValue("INSERT"), p.current.MatchIdentifierValue("DELETE"):
		event := strings.ToUpper(p.current.Value)
		p.advance()
		return event, nil
	case p.current.MatchIdentifierValue("UPDATE"):
		return p.parseUpdateTriggerEvent(), nil
	default:
		return "", fmt.Errorf("expected trigger event, got %s at position %d", p.current.Type, p.current.Start)
	}
}

func (p *Parser) parseUpdateTriggerEvent() string {
	var event strings.Builder
	event.WriteString(strings.ToUpper(p.current.Value))
	p.advance()
	p.skipWhitespace()
	if !p.current.MatchIdentifierValue("OF") {
		return event.String()
	}

	event.WriteString(" OF")
	p.advance()
	for !p.isAtEnd() && !p.current.MatchIdentifierValue("ON") {
		event.WriteString(p.current.Value)
		p.advance()
	}
	return strings.TrimSpace(event.String())
}

func (p *Parser) parseOptionalTriggerForEach() (string, error) {
	if !p.current.MatchIdentifierValue("FOR") {
		return "ROW", nil
	}
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "EACH"); err != nil {
		return "", fmt.Errorf("expected EACH after FOR in trigger clause: %w", err)
	}
	p.skipWhitespace()

	forEach, err := p.expectIdentifier()
	if err != nil {
		return "", fmt.Errorf("expected trigger FOR EACH target: %w", err)
	}
	return strings.ToUpper(forEach), nil
}

func (p *Parser) collectTriggerBody() (string, error) {
	var body strings.Builder
	blockDepth := 0
	caseDepth := 0
	seenBegin := false

	for !p.isAtEnd() {
		if err := p.checkTimeout(); err != nil {
			return "", err
		}

		if p.current.Type == lexer.TokenIdentifier {
			keyword := strings.ToUpper(p.current.Value)
			switch keyword {
			case "BEGIN":
				seenBegin = true
				blockDepth++
			case "CASE":
				if seenBegin {
					caseDepth++
				}
			case "END":
				if caseDepth > 0 {
					caseDepth--
				} else if seenBegin {
					blockDepth--
				}
			}

			body.WriteString(p.current.Value)
			p.advance()

			if seenBegin && blockDepth == 0 {
				return strings.TrimSpace(body.String()), nil
			}
			continue
		}

		body.WriteString(p.current.Value)
		p.advance()
	}

	if !seenBegin {
		return "", fmt.Errorf("expected trigger body BEGIN at position %d", p.current.Start)
	}
	return "", fmt.Errorf("unterminated trigger body at position %d", p.current.Start)
}

func stripSQLStringDelimiters(value string) string {
	if len(value) < 2 {
		return value
	}
	if value[0] == '\'' && value[len(value)-1] == '\'' {
		return value[1 : len(value)-1]
	}
	if value[0] != '$' {
		return value
	}

	end := strings.Index(value[1:], "$")
	if end < 0 {
		return value
	}
	tag := value[:end+2]
	if !strings.HasSuffix(value, tag) {
		return value
	}
	return value[len(tag) : len(value)-len(tag)]
}

func (p *Parser) parseCreateViewNode() (*ast.CreateViewNode, error) {
	if err := p.expect(lexer.TokenIdentifier, "VIEW"); err != nil {
		return nil, err
	}
	p.skipWhitespace()

	viewName, err := p.parseQualifiedIdentifier("view name")
	if err != nil {
		return nil, err
	}
	p.skipWhitespace()

	if err := p.expect(lexer.TokenIdentifier, "AS"); err != nil {
		return nil, fmt.Errorf("expected AS after view name: %w", err)
	}

	body := p.collectStatementBody()
	if strings.TrimSpace(body) == "" {
		return nil, fmt.Errorf("expected view body after AS at position %d", p.current.Start)
	}

	return ast.NewCreateView(viewName).SetBody(body), nil
}

func (p *Parser) collectStatementBody() string {
	var body strings.Builder
	for !p.isAtEnd() && p.current.Type != lexer.TokenSemicolon {
		body.WriteString(p.current.Value)
		p.advance()
	}
	return strings.TrimSpace(body.String())
}

// parseCreateTable parses CREATE TABLE statements.
func (p *Parser) parseCreateTable() (*ast.CreateTableNode, error) {
	table, err := p.parseCreateTableHeader()
	if err != nil {
		return nil, err
	}

	done, err := p.parseCreateTableBeforeColumnList(table)
	if err != nil || done {
		return table, err
	}

	if err := p.expect(lexer.TokenOperator, "("); err != nil {
		return nil, fmt.Errorf("expected '(' after table name: %w", err)
	}

	if err := p.parseCreateTableElements(table); err != nil {
		return nil, err
	}

	if err := p.expect(lexer.TokenOperator, ")"); err != nil {
		return nil, err
	}

	if err := p.parseCreateTableSuffix(table); err != nil {
		return nil, err
	}

	return table, nil
}

func (p *Parser) parseCreateTableHeader() (*ast.CreateTableNode, error) {
	if err := p.expect(lexer.TokenIdentifier, "TABLE"); err != nil {
		return nil, err
	}

	p.skipWhitespace()

	ifNotExists, err := p.parseOptionalIfNotExists()
	if err != nil {
		return nil, err
	}
	p.skipWhitespace()

	// Get table name (could be schema.table)
	tableName, err := p.parseQualifiedIdentifier("table name")
	if err != nil {
		return nil, err
	}

	table := ast.NewCreateTable(tableName)
	if ifNotExists {
		table.SetIfNotExists()
	}

	return table, nil
}

func (p *Parser) parseCreateTableBeforeColumnList(table *ast.CreateTableNode) (bool, error) {
	p.skipWhitespace()
	if p.current.MatchIdentifierValue("AS") || p.current.MatchIdentifierValue("SELECT") {
		if err := p.parseCreateTableSelectBody(table); err != nil {
			return false, err
		}
		return true, nil
	}
	if !p.current.MatchOperatorValue("(") && p.current.Type == lexer.TokenIdentifier {
		if err := p.parseTableOptions(table); err != nil {
			return false, err
		}
		p.skipWhitespace()
		if p.current.MatchIdentifierValue("AS") || p.current.MatchIdentifierValue("SELECT") {
			if err := p.parseCreateTableSelectBody(table); err != nil {
				return false, err
			}
			return true, nil
		}
	}

	return false, nil
}

func (p *Parser) parseCreateTableElements(table *ast.CreateTableNode) error {
	// Parse column definitions and constraints
	for {
		// Check for timeout to prevent infinite loops
		if err := p.checkTimeout(); err != nil {
			return err
		}

		p.skipWhitespace()

		// Check for closing parenthesis
		if p.current.Type == lexer.TokenOperator && p.current.Value == ")" {
			break
		}

		// Parse column or constraint
		if err := p.parseTableElement(table); err != nil {
			return err
		}

		p.skipWhitespace()

		// Check for comma or closing parenthesis
		if p.current.MatchOperatorValue(",") {
			p.advance()
			continue
		}

		if p.current.MatchOperatorValue(")") {
			break
		}

		if p.current.Type == lexer.TokenWhitespace {
			// Skip whitespace and try again
			p.skipWhitespace()
			if p.current.MatchOperatorValue(",") {
				p.advance()
				continue
			}

			if p.current.MatchOperatorValue(")") {
				break
			}
		}

		// If we get here and it's an identifier, it might be another table element
		if p.current.Type == lexer.TokenIdentifier {
			continue
		}

		return fmt.Errorf("expected ',' or ')' after table element at position %d", p.current.Start)
	}

	return nil
}

func (p *Parser) parseCreateTableSuffix(table *ast.CreateTableNode) error {
	// Parse optional table options (ENGINE, etc.)
	if err := p.parseTableOptions(table); err != nil {
		return err
	}
	if p.current.MatchIdentifierValue("AS") || p.current.MatchIdentifierValue("SELECT") {
		if err := p.parseCreateTableSelectBody(table); err != nil {
			return err
		}
	}
	return nil
}

func (p *Parser) parseCreateTableSelectBody(table *ast.CreateTableNode) error {
	p.skipWhitespace()
	if p.current.MatchIdentifierValue("AS") {
		p.advance()
		p.skipWhitespace()
	}
	if !p.current.MatchIdentifierValue("SELECT") {
		return fmt.Errorf("expected SELECT in CREATE TABLE SELECT body, got %s at position %d", p.current.Type, p.current.Start)
	}
	table.SetSelectBody(p.collectStatementBody())
	return nil
}

// parseTableElement parses a column definition or table constraint.
func (p *Parser) parseTableElement(table *ast.CreateTableNode) error {
	p.skipWhitespace()

	// Check if this is a constraint (starts with CONSTRAINT, PRIMARY, UNIQUE, FOREIGN, CHECK, EXCLUDE, SPATIAL, INDEX, KEY)
	if p.current.Type == lexer.TokenIdentifier {
		keyword := strings.ToUpper(p.current.Value)
		switch keyword {
		case "CONSTRAINT", "PRIMARY", "UNIQUE", "FOREIGN", "CHECK", "EXCLUDE", "SPATIAL", "INDEX", "KEY":
			constraint, err := p.parseTableConstraint()
			if err != nil {
				return err
			}
			table.AddConstraint(constraint)
			return nil
		}
	}

	// Otherwise, parse as column definition
	column, err := p.parseColumnDefinition(table)
	if err != nil {
		return err
	}
	table.AddColumn(column)
	return nil
}

func (p *Parser) handleNotNull(column *ast.ColumnNode) error {
	// Handle NOT NULL
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "NULL"); err != nil {
		return fmt.Errorf("expected NULL after NOT: %w", err)
	}
	column.SetNotNull()
	return nil
}

func (p *Parser) handleNull(column *ast.ColumnNode) {
	// Explicit NULL (default behavior)
	p.advance()
	column.Nullable = true
}

func (p *Parser) handlePrimaryKey(column *ast.ColumnNode) error {
	// Handle PRIMARY KEY
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "KEY"); err != nil {
		return fmt.Errorf("expected KEY after PRIMARY: %w", err)
	}
	column.SetPrimary()
	return nil
}

func (p *Parser) handleUnique(column *ast.ColumnNode) {
	// Handle UNIQUE
	p.advance()
	column.SetUnique()
}

func (p *Parser) handleAutoIncrement(column *ast.ColumnNode) {
	// Handle AUTO_INCREMENT / AUTOINCREMENT
	p.advance()
	column.SetAutoIncrement()
}

func (p *Parser) handleDefault(column *ast.ColumnNode) error {
	// Handle DEFAULT
	p.advance()
	p.skipWhitespace()
	defaultValue, err := p.parseDefaultValue()
	if err != nil {
		return fmt.Errorf("expected default value: %w", err)
	}
	if defaultValue.Expression != "" {
		column.SetDefaultExpression(defaultValue.Expression)
	} else {
		column.SetDefault(defaultValue.Value)
	}

	return nil
}

func (p *Parser) handleCheck(column *ast.ColumnNode) error {
	// Handle CHECK
	p.advance()
	p.skipWhitespace()
	checkExpr, err := p.parseCheckExpression()
	if err != nil {
		return fmt.Errorf("expected check expression: %w", err)
	}
	column.SetCheck(checkExpr)
	return nil
}

func (p *Parser) handleReferences(column *ast.ColumnNode) error {
	// Handle REFERENCES
	p.advance()
	fkRef, err := p.parseForeignKeyReference()
	if err != nil {
		return fmt.Errorf("expected foreign key reference: %w", err)
	}
	column.ForeignKey = fkRef
	return nil
}

func (p *Parser) handleAs(column *ast.ColumnNode) error {
	// Handle AS (for generated columns)
	// Handle MySQL/MariaDB virtual columns (AS (expression) STORED)
	p.advance()
	p.skipWhitespace()

	// Parse the generation expression
	if err := p.expect(lexer.TokenOperator, "("); err != nil {
		return fmt.Errorf("expected '(' for generated expression: %w", err)
	}

	// Collect the expression until closing parenthesis
	var expr strings.Builder
	parenCount := 1
	for parenCount > 0 && !p.isAtEnd() {
		if p.current.Type == lexer.TokenOperator {
			switch p.current.Value {
			case "(":
				parenCount++
			case ")":
				parenCount--
			}
		}
		if parenCount > 0 {
			expr.WriteString(p.current.Value)
		}
		p.advance()
	}

	generatedKind := ""
	p.skipWhitespace()
	if p.current.Type == lexer.TokenIdentifier {
		storageType := strings.ToUpper(p.current.Value)
		if storageType == "STORED" || storageType == "VIRTUAL" {
			generatedKind = storageType
			p.advance()
		}
	}

	column.SetGenerated(expr.String(), generatedKind)
	return nil
}

func (p *Parser) handleGenerated(column *ast.ColumnNode) error {
	p.advance()
	p.skipWhitespace()

	switch strings.ToUpper(p.current.Value) {
	case "BY":
		p.advance()
		if err := p.expect(lexer.TokenIdentifier, "DEFAULT"); err != nil {
			return fmt.Errorf("expected DEFAULT after GENERATED BY: %w", err)
		}
		if err := p.expect(lexer.TokenIdentifier, "AS"); err != nil {
			return fmt.Errorf("expected AS after GENERATED BY DEFAULT: %w", err)
		}
		return p.handleGeneratedIdentity(column, "BY_DEFAULT")
	case "ALWAYS":
		p.advance()
	default:
		return fmt.Errorf("expected ALWAYS or BY after GENERATED: got %q at position %d", p.current.Value, p.current.Start)
	}

	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "AS"); err != nil {
		return fmt.Errorf("expected AS after ALWAYS: %w", err)
	}
	p.skipWhitespace()
	if p.current.Type == lexer.TokenIdentifier && strings.EqualFold(p.current.Value, "IDENTITY") {
		return p.handleGeneratedIdentity(column, "ALWAYS")
	}

	// Parse the generation expression
	if err := p.expect(lexer.TokenOperator, "("); err != nil {
		return fmt.Errorf("expected '(' for generated expression: %w", err)
	}

	// Collect the expression until closing parenthesis
	var expr strings.Builder
	parenCount := 1
	for parenCount > 0 && !p.isAtEnd() {
		if p.current.Type == lexer.TokenOperator {
			switch p.current.Value {
			case "(":
				parenCount++
			case ")":
				parenCount--
			}
		}
		if parenCount > 0 {
			expr.WriteString(p.current.Value)
		}
		p.advance()
	}

	generatedKind := ""
	p.skipWhitespace()
	if p.current.Type == lexer.TokenIdentifier && strings.ToUpper(p.current.Value) == "STORED" {
		generatedKind = "STORED"
		p.advance()
	}

	column.SetGenerated(expr.String(), generatedKind)

	return nil
}

func (p *Parser) handleGeneratedIdentity(column *ast.ColumnNode, generation string) error {
	if err := p.expect(lexer.TokenIdentifier, "IDENTITY"); err != nil {
		return fmt.Errorf("expected IDENTITY after GENERATED AS: %w", err)
	}
	start, increment, options := p.parseGeneratedIdentityOptions()
	column.SetIdentity(generation, start, increment)
	column.SetIdentityOptions(options)
	return nil
}

func (p *Parser) parseGeneratedIdentityOptions() (start, increment, options string) {
	p.skipWhitespace()
	if p.current.Type != lexer.TokenOperator || p.current.Value != "(" {
		return "", "", ""
	}
	optionsStart := p.current.End
	p.advance()
	for !p.isAtEnd() {
		p.skipWhitespace()
		if p.current.Type == lexer.TokenOperator && p.current.Value == ")" {
			options = strings.TrimSpace(p.input[optionsStart:p.current.Start])
			p.advance()
			return start, increment, options
		}
		if p.current.Type != lexer.TokenIdentifier {
			p.advance()
			continue
		}

		switch strings.ToUpper(p.current.Value) {
		case "START":
			start = p.parseIdentityOptionValue("WITH")
		case "INCREMENT":
			increment = p.parseIdentityOptionValue("BY")
		default:
			p.advance()
		}
	}
	return start, increment, strings.TrimSpace(p.input[optionsStart:])
}

func (p *Parser) parseIdentityOptionValue(optionalKeyword string) string {
	p.advance()
	p.skipWhitespace()
	if p.current.Type == lexer.TokenIdentifier && strings.EqualFold(p.current.Value, optionalKeyword) {
		p.advance()
		p.skipWhitespace()
	}
	if p.isAtEnd() || p.current.Type == lexer.TokenOperator && p.current.Value == ")" {
		return ""
	}
	value := p.current.Value
	p.advance()
	if value == "-" || value == "+" {
		p.skipWhitespace()
	}
	if (value == "-" || value == "+") && p.current.Type == lexer.TokenIdentifier {
		value += p.current.Value
		p.advance()
	}
	return value
}

func (p *Parser) handleCharacter(column *ast.ColumnNode) {
	// Handle MySQL/MariaDB CHARACTER SET
	p.advance()
	p.skipWhitespace()
	if p.current.Type != lexer.TokenIdentifier {
		return
	}

	if strings.ToUpper(p.current.Value) != "SET" {
		return
	}

	p.advance()
	p.skipWhitespace()
	if p.current.Type == lexer.TokenIdentifier {
		column.Charset = p.current.Value
		p.advance()
	}
}

func (p *Parser) handleCharset(column *ast.ColumnNode) error {
	p.advance()
	p.skipWhitespace()

	value, err := p.expectIdentifier()
	if err != nil {
		return fmt.Errorf("expected charset name: %w", err)
	}
	column.Charset = value
	return nil
}

func (p *Parser) handleCollate(column *ast.ColumnNode) error {
	// Handle PostgreSQL/MySQL COLLATE
	p.advance()
	p.skipWhitespace()

	var collation string
	switch p.current.Type {
	case lexer.TokenString:
		// Quoted collation name like "C"
		collation = p.current.Value
		p.advance()
	case lexer.TokenIdentifier:
		// Unquoted collation name
		collation = p.current.Value
		p.advance()
	default:
		return fmt.Errorf("expected collation name: got %s at position %d", p.current.Type, p.current.Start)
	}

	column.Collate = collation

	return nil
}

func (p *Parser) handleColumnComment(column *ast.ColumnNode) error {
	p.advance()
	p.skipWhitespace()
	if p.current.Type != lexer.TokenString {
		return fmt.Errorf("expected column comment string, got %s at position %d", p.current.Type, p.current.Start)
	}
	appendColumnComment(column, "COMMENT "+p.current.Value)
	p.advance()
	return nil
}

func appendColumnComment(column *ast.ColumnNode, text string) {
	if column.Comment == "" {
		column.SetComment(text)
		return
	}
	column.SetComment(column.Comment + "; " + text)
}

func (p *Parser) handleOn(column *ast.ColumnNode) {
	// Handle MySQL/MariaDB ON UPDATE syntax
	p.advance()
	p.skipWhitespace()
	if !p.current.MatchIdentifierValue("UPDATE") {
		return
	}

	p.advance()
	p.skipWhitespace()

	updateExpr := p.parseOnUpdateExpression()
	if updateExpr == "" {
		return
	}
	column.SetUpdateExpression(updateExpr)
}

func (p *Parser) parseOnUpdateExpression() string {
	var expr strings.Builder
	depth := 0
	for !p.isAtEnd() && p.current.Type != lexer.TokenSemicolon {
		if depth == 0 && expr.Len() > 0 && p.current.Type == lexer.TokenIdentifier &&
			isOnUpdateExpressionTerminator(p.current.Value) {
			return strings.TrimSpace(expr.String())
		}
		if p.current.Type == lexer.TokenOperator {
			switch p.current.Value {
			case "(":
				depth++
			case ")":
				if depth == 0 {
					return strings.TrimSpace(expr.String())
				}
				depth--
			case ",":
				if depth == 0 {
					return strings.TrimSpace(expr.String())
				}
			}
		}
		expr.WriteString(p.current.Value)
		p.advance()
	}
	return strings.TrimSpace(expr.String())
}

func isOnUpdateExpressionTerminator(value string) bool {
	switch strings.ToUpper(value) {
	case "NOT", "NULL", "PRIMARY", "UNIQUE", "AUTO_INCREMENT", "AUTOINCREMENT", "DEFAULT", "CHECK",
		"CONSTRAINT", "REFERENCES", "AS", "GENERATED", "CHARACTER", "CHARSET", "COLLATE", "COMMENT", "ON":
		return true
	default:
		return false
	}
}

func (p *Parser) parseColumnConstraintsAndAttributes(table *ast.CreateTableNode, column *ast.ColumnNode) error {
	var err error
	for {
		// Check for timeout to prevent infinite loops
		if err := p.checkTimeout(); err != nil {
			return err
		}

		p.skipWhitespace()

		if p.current.Type != lexer.TokenIdentifier {
			break
		}

		keyword := strings.ToUpper(p.current.Value)
		err = p.parseColumnConstraintOrAttribute(table, column, keyword)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Parser) parseColumnConstraintOrAttribute(table *ast.CreateTableNode, column *ast.ColumnNode, keyword string) error {
	switch keyword {
	case "NOT":
		return p.handleNotNull(column)
	case "NULL":
		p.handleNull(column)
	case "PRIMARY":
		return p.handlePrimaryKeyAttribute(table, column)
	case "UNIQUE":
		p.handleUnique(column)
	case "AUTO_INCREMENT", "AUTOINCREMENT":
		p.handleAutoIncrement(column)
	case "DEFAULT":
		return p.handleDefault(column)
	case "CHECK":
		return p.handleCheck(column)
	case "CONSTRAINT":
		return p.handleColumnConstraint(column)
	case "REFERENCES":
		return p.handleReferences(column)
	case "AS":
		return p.handleAs(column)
	case "GENERATED":
		return p.handleGenerated(column)
	case "CHARACTER":
		p.handleCharacter(column)
	case "CHARSET":
		return p.handleCharset(column)
	case "COLLATE":
		return p.handleCollate(column)
	case "COMMENT":
		return p.handleColumnComment(column)
	case "ON":
		p.handleOn(column)
	default:
		return fmt.Errorf("unsupported column attribute: %s at position %d", keyword, p.current.Start)
	}
	return nil
}

func (p *Parser) handleColumnConstraint(column *ast.ColumnNode) error {
	p.advance()
	p.skipWhitespace()
	name, err := p.expectIdentifier()
	if err != nil {
		return fmt.Errorf("expected column constraint name: %w", err)
	}
	p.skipWhitespace()
	if !p.current.MatchIdentifierValue("CHECK") {
		return fmt.Errorf("unsupported column constraint %q at position %d", p.current.Value, p.current.Start)
	}
	if err := p.handleCheck(column); err != nil {
		return err
	}
	column.SetCheckName(name)
	return nil
}

func (p *Parser) handlePrimaryKeyAttribute(table *ast.CreateTableNode, column *ast.ColumnNode) error {
	if table == nil {
		return p.handlePrimaryKey(column)
	}
	handled, err := p.handlePrimaryKeyOrInlineTableConstraint(table, column)
	if err != nil {
		return err
	}
	if handled {
		return nil
	}
	return p.handlePrimaryKey(column)
}

func (p *Parser) handlePrimaryKeyOrInlineTableConstraint(table *ast.CreateTableNode, column *ast.ColumnNode) (bool, error) {
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "KEY"); err != nil {
		return false, fmt.Errorf("expected KEY after PRIMARY: %w", err)
	}
	p.skipWhitespace()
	if !p.current.MatchOperatorValue("(") {
		column.SetPrimary()
		return true, nil
	}

	constraint := &ast.ConstraintNode{Type: ast.PrimaryKeyConstraint}
	if err := p.parseTableColumnList(constraint); err != nil {
		return false, err
	}
	table.AddConstraint(constraint)
	return true, nil
}

// parseColumnDefinition parses a column definition.
func (p *Parser) parseColumnDefinition(table *ast.CreateTableNode) (*ast.ColumnNode, error) {
	p.skipWhitespace()

	// Get column name
	columnName, err := p.expectIdentifier()
	if err != nil {
		return nil, fmt.Errorf("expected column name: %w", err)
	}

	p.skipWhitespace()

	// Get column type. SQLite permits columns without an explicit type.
	columnType, err := p.parseOptionalColumnType()
	if err != nil {
		return nil, fmt.Errorf("expected column type: %w", err)
	}

	column := ast.NewColumn(columnName, columnType)
	err = p.parseColumnConstraintsAndAttributes(table, column)
	if err != nil {
		return nil, err
	}

	return column, nil
}

func (p *Parser) parseOptionalColumnType() (string, error) {
	switch {
	case p.current.Type == lexer.TokenIdentifier && isColumnConstraintStart(p.current.Value):
		return "", nil
	case p.current.Type == lexer.TokenSemicolon || p.current.MatchOperatorValue(",") || p.current.MatchOperatorValue(")"):
		return "", nil
	default:
		return p.parseColumnType()
	}
}

func isColumnConstraintStart(value string) bool {
	switch strings.ToUpper(value) {
	case "NOT", "NULL", "PRIMARY", "UNIQUE", "AUTO_INCREMENT", "AUTOINCREMENT", "DEFAULT", "CHECK", "REFERENCES", "COMMENT", "COLLATE", "GENERATED", "AS":
		return true
	default:
		return false
	}
}

func (p *Parser) handleMultiWordType(typeName string) string {
	// Handle multi-word types like DOUBLE PRECISION, CHARACTER VARYING, etc.
	p.skipWhitespace()
	if p.current.Type != lexer.TokenIdentifier {
		return typeName
	}

	firstWord := strings.ToUpper(typeName)
	secondWord := strings.ToUpper(p.current.Value)

	// Check for known multi-word type combinations
	switch firstWord {
	case "DOUBLE":
		if secondWord == "PRECISION" {
			typeName += " " + p.current.Value
			p.advance()
		}
	case "CHARACTER":
		if secondWord == "VARYING" {
			typeName += " " + p.current.Value
			p.advance()
		}
	case "TIME":
		if secondWord != "WITH" && secondWord != "WITHOUT" {
			break
		}

		typeName = p.current.Value + " " + typeName
		p.advance()
		p.skipWhitespace()

		if !p.current.MatchIdentifierValue("TIME") {
			break
		}

		typeName += " " + p.current.Value
		p.advance()
		p.skipWhitespace()
		if !p.current.MatchIdentifierValue("ZONE") {
			break
		}

		typeName += " " + p.current.Value
		p.advance()
	case "TIMESTAMP":
		if secondWord != "WITH" && secondWord != "WITHOUT" {
			break
		}

		typeName = p.current.Value + " " + typeName
		p.advance()
		p.skipWhitespace()

		if !p.current.MatchIdentifierValue("TIME") {
			break
		}

		typeName += " " + p.current.Value
		p.advance()
		p.skipWhitespace()

		if !p.current.MatchIdentifierValue("ZONE") {
			break
		}

		typeName += " " + p.current.Value
		p.advance()
	}

	return typeName
}

func (p *Parser) handleMySQLLikeTypeModifiers(typeName string) string {
	p.skipWhitespace()
	for p.current.Type == lexer.TokenIdentifier {
		modifier := strings.ToUpper(p.current.Value)
		switch modifier {
		case "UNSIGNED", "SIGNED", "ZEROFILL":
			typeName += " " + p.current.Value
			p.advance()
			p.skipWhitespace()
		default:
			// Not a type modifier, stop processing
			return typeName
		}
	}
	return typeName
}

func (p *Parser) handlePostgresArrayNotation(typeName string) string {
	p.skipWhitespace()
	if !p.current.MatchOperatorValue("[") {
		return typeName
	}

	typeName += "["
	p.advance()

	// Handle multi-dimensional arrays like INT[][] or NUMERIC(5,2)[]
	for p.current.MatchOperatorValue("]") {
		typeName += "]"
		p.advance()

		if !p.current.MatchOperatorValue("[") {
			break
		}

		typeName += "["
		p.advance()
	}

	return typeName
}

// parseColumnType parses a column data type (e.g., INTEGER, VARCHAR(255), DECIMAL(10,2), DOUBLE PRECISION).
func (p *Parser) parseColumnType() (string, error) {
	if p.current.Type != lexer.TokenIdentifier {
		return "", fmt.Errorf("expected column type, got %s at position %d", p.current.Type, p.current.Start)
	}

	typeName := p.current.Value
	p.advance()

	// Handle multi-word types like DOUBLE PRECISION, CHARACTER VARYING, etc.
	typeName = p.handleMultiWordType(typeName)

	// Check for type parameters (e.g., VARCHAR(255), NUMERIC(10,2))
	p.skipWhitespace()
	if p.current.MatchOperatorValue("(") {
		typeName += "("
		p.advance()

		// Collect everything inside parentheses
		parenCount := 1
		for parenCount > 0 && p.current.Type != lexer.TokenEOF {
			switch {
			case p.current.MatchOperatorValue("("):
				parenCount++
			case p.current.MatchOperatorValue(")"):
				parenCount--
			}
			typeName += p.current.Value
			p.advance()
		}
	}

	// Check for MySQL/MariaDB type modifiers (UNSIGNED, ZEROFILL, etc.)
	typeName = p.handleMySQLLikeTypeModifiers(typeName)

	// Check for array notation (PostgreSQL) - must come after type parameters
	typeName = p.handlePostgresArrayNotation(typeName)

	return typeName, nil
}

func (p *Parser) handleStringLiteral() (*ast.DefaultValue, error) {
	value := p.current.Value
	p.advance()

	// Check for PostgreSQL type casting like '{}'::jsonb
	if p.current.Type == lexer.TokenOperator && p.current.Value == ":" {
		p.advance()
		if p.current.Type == lexer.TokenOperator && p.current.Value == ":" {
			p.advance()
			if p.current.Type == lexer.TokenIdentifier {
				value += "::" + p.current.Value
				p.advance()
			}
		}
	}

	return &ast.DefaultValue{Value: value, ValueSet: true}, nil
}

func (p *Parser) parseArrayLiteral(value string) (*string, error) {
	var result *string

	p.skipWhitespace()

	if !p.current.MatchOperatorValue("[") {
		return result, nil
	}

	// Parse array literal
	arrayLiteral := value + "["
	p.advance()

	// Collect array elements
	for {
		if p.current.Type == lexer.TokenEOF {
			return result, fmt.Errorf("unexpected end of input while parsing array at position %d", p.current.Start)
		}
		if p.current.MatchOperatorValue("]") {
			arrayLiteral += "]"
			p.advance()
			break
		}
		if p.current.MatchOperatorValue("[") {
			return result, fmt.Errorf("nested arrays are not supported at position %d", p.current.Start)
		}

		arrayLiteral += p.current.Value
		p.advance()
	}

	// Handle type cast like ::TEXT[]
	if !p.current.MatchOperatorValue(":") {
		// No type cast, return array literal
		return new(arrayLiteral), nil
	}
	p.advance()
	if !p.current.MatchOperatorValue(":") {
		return result, fmt.Errorf("expected '::' for type cast, got %s at position %d", p.current.Type, p.current.Start)
	}
	p.advance()
	// Get the cast type
	if p.current.Type != lexer.TokenIdentifier {
		return new(arrayLiteral), nil
	}

	arrayLiteral += "::" + p.current.Value
	p.advance()
	// Handle array brackets in cast
	if !p.current.MatchOperatorValue("[") {
		return new(arrayLiteral), nil
	}
	arrayLiteral += "["
	p.advance()
	if !p.current.MatchOperatorValue("]") {
		return result, fmt.Errorf("expected ']' for array cast, got %s at position %d", p.current.Type, p.current.Start)
	}
	arrayLiteral += "]"
	p.advance()

	return new(arrayLiteral), nil
}

func (p *Parser) handleFunctionCallOrKeyword() (*ast.DefaultValue, error) {
	value := p.current.Value
	p.advance()

	upperValue := strings.ToUpper(value)
	if upperValue == "E" && p.current.Type == lexer.TokenString {
		value += p.current.Value
		p.advance()
		return &ast.DefaultValue{Value: value, ValueSet: true}, nil
	}

	// Check if it's a function call
	if p.current.Type == lexer.TokenOperator && p.current.Value == "(" {
		// Parse function call
		p.advance()
		p.skipWhitespace()

		// Consume closing parenthesis
		if err := p.expect(lexer.TokenOperator, ")"); err != nil {
			return nil, err
		}

		return &ast.DefaultValue{Expression: value + "()"}, nil
	}

	// Handle MySQL/PostgreSQL functions that can be used without parentheses
	if upperValue == "CURRENT_TIMESTAMP" || upperValue == "NOW" || upperValue == "CURRENT_DATE" || upperValue == "CURRENT_TIME" {
		return &ast.DefaultValue{Expression: value + "()"}, nil
	}

	// Handle PostgreSQL-specific functions
	if upperValue == "GEN_RANDOM_UUID" {
		return &ast.DefaultValue{Expression: value + "()"}, nil
	}

	// Handle PostgreSQL array literals like ARRAY[]::TEXT[]
	if upperValue == "ARRAY" {
		pArrayLiteral, err := p.parseArrayLiteral(value)
		if err != nil {
			return nil, err
		}

		return &ast.DefaultValue{Expression: *pArrayLiteral}, nil
	}

	// Regular identifier/keyword
	return &ast.DefaultValue{Value: value, ValueSet: true}, nil
}

func (p *Parser) handleNumber() (*ast.DefaultValue, error) {
	if p.current.Value == "-" || p.current.Value == "+" {
		sign := p.current.Value
		p.advance()
		p.skipWhitespace()
		if p.current.Type == lexer.TokenIdentifier || p.current.Type == lexer.TokenOperator {
			value := sign + p.current.Value
			p.advance()
			return &ast.DefaultValue{Value: value, ValueSet: true}, nil
		}
	}
	// Check if it's a number that the lexer tokenized as an operator (like "0", "1", etc.)
	// Numbers might be tokenized as operators by the simple lexer
	value := p.current.Value
	// Check if this looks like a number
	if isNumeric(value) {
		p.advance()
		return &ast.DefaultValue{Value: value, ValueSet: true}, nil
	}

	return nil, fmt.Errorf("unexpected token for default value: %s at position %d", p.current.Value, p.current.Start)
}

// parseDefaultValue parses a default value (literal or function call).
func (p *Parser) parseDefaultValue() (*ast.DefaultValue, error) {
	p.skipWhitespace()

	var value *ast.DefaultValue
	var err error
	switch p.current.Type {
	case lexer.TokenString:
		// String literal
		value, err = p.handleStringLiteral()
	case lexer.TokenIdentifier:
		// Could be a function call or keyword like NULL, TRUE, FALSE
		value, err = p.handleFunctionCallOrKeyword()
	case lexer.TokenOperator:
		// Could be a number (positive or negative) or just a number
		value, err = p.handleNumber()
	default:
		return nil, fmt.Errorf("expected default value, got %s at position %d", p.current.Type, p.current.Start)
	}
	if err != nil {
		return nil, err
	}
	return p.extendDefaultExpression(value), nil
}

func (p *Parser) extendDefaultExpression(value *ast.DefaultValue) *ast.DefaultValue {
	p.skipWhitespace()
	if !p.current.MatchOperatorValue("+") {
		return value
	}

	var expr strings.Builder
	if value.Expression != "" {
		expr.WriteString(value.Expression)
	} else {
		expr.WriteString(value.Value)
	}

	depth := 0
	for !p.isAtEnd() && p.current.Type != lexer.TokenSemicolon {
		if p.current.Type == lexer.TokenOperator {
			switch p.current.Value {
			case "(":
				depth++
			case ")":
				if depth == 0 {
					return &ast.DefaultValue{Expression: strings.TrimSpace(expr.String())}
				}
				depth--
			case ",":
				if depth == 0 {
					return &ast.DefaultValue{Expression: strings.TrimSpace(expr.String())}
				}
			}
		}

		expr.WriteString(p.current.Value)
		p.advance()
	}
	return &ast.DefaultValue{Expression: strings.TrimSpace(expr.String())}
}

// parseCheckExpression parses a CHECK constraint expression.
func (p *Parser) parseCheckExpression() (string, error) {
	p.skipWhitespace()

	// Expect opening parenthesis
	if err := p.expect(lexer.TokenOperator, "("); err != nil {
		return "", fmt.Errorf("expected '(' for check expression: %w", err)
	}

	// Collect everything until closing parenthesis
	var expr strings.Builder
	parenCount := 1

	for parenCount > 0 && !p.isAtEnd() {
		if p.current.Type == lexer.TokenOperator {
			switch p.current.Value {
			case "(":
				parenCount++
			case ")":
				parenCount--
			}
		}

		if parenCount > 0 {
			expr.WriteString(p.current.Value)
		}
		p.advance()
	}

	return expr.String(), nil
}

// parseForeignKeyReference parses a REFERENCES clause.
func (p *Parser) parseForeignKeyReference() (*ast.ForeignKeyRef, error) {
	p.skipWhitespace()

	// Get referenced table name
	tableName, err := p.expectIdentifier()
	if err != nil {
		return nil, fmt.Errorf("expected table name in REFERENCES: %w", err)
	}

	p.skipWhitespace()

	// Expect opening parenthesis
	if err := p.expect(lexer.TokenOperator, "("); err != nil {
		return nil, fmt.Errorf("expected '(' after table name in REFERENCES: %w", err)
	}

	p.skipWhitespace()

	// Get referenced column names
	var columnNames []string
	for {
		columnName, err := p.expectIdentifier()
		if err != nil {
			return nil, fmt.Errorf("expected column name in REFERENCES: %w", err)
		}
		columnNames = append(columnNames, columnName)

		p.skipWhitespace()
		if !p.current.MatchOperatorValue(",") {
			break
		}
		p.advance()
		p.skipWhitespace()
	}

	p.skipWhitespace()

	// Expect closing parenthesis
	if err := p.expect(lexer.TokenOperator, ")"); err != nil {
		return nil, fmt.Errorf("expected ')' after column name in REFERENCES: %w", err)
	}

	fkRef := &ast.ForeignKeyRef{
		Table:  tableName,
		Column: columnNames[0],
	}
	if len(columnNames) > 1 {
		fkRef.Columns = columnNames
	}

	// Parse optional ON DELETE/UPDATE actions
	for {
		p.skipWhitespace()

		if p.current.Type != lexer.TokenIdentifier || strings.ToUpper(p.current.Value) != "ON" {
			break
		}

		p.advance() // consume ON
		p.skipWhitespace()

		if p.current.Type != lexer.TokenIdentifier {
			break
		}

		action := strings.ToUpper(p.current.Value)
		p.advance()
		p.skipWhitespace()

		// Get the action value (CASCADE, SET NULL, etc.)
		var actionValue string
		if p.current.Type == lexer.TokenIdentifier {
			actionValue = strings.ToUpper(p.current.Value)
			p.advance()

			// Handle multi-word actions like "SET NULL"
			if actionValue == "SET" {
				p.skipWhitespace()
				if p.current.Type == lexer.TokenIdentifier {
					actionValue += " " + strings.ToUpper(p.current.Value)
					p.advance()
				}
			}
		}

		switch action {
		case "DELETE":
			fkRef.OnDelete = actionValue
		case "UPDATE":
			fkRef.OnUpdate = actionValue
		}
	}

	return fkRef, nil
}

func (p *Parser) handleTableConstraintName(constraint *ast.ConstraintNode) error {
	if !p.current.MatchIdentifierValue("CONSTRAINT") {
		return nil
	}

	p.advance()
	p.skipWhitespace()

	// Get constraint name
	name, err := p.expectIdentifier()
	if err != nil {
		return fmt.Errorf("expected constraint name: %w", err)
	}
	constraint.Name = name
	p.skipWhitespace()

	return nil
}

func (p *Parser) handleTableConstraintPrimaryKey(constraint *ast.ConstraintNode) error {
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "KEY"); err != nil {
		return fmt.Errorf("expected KEY after PRIMARY: %w", err)
	}
	constraint.Type = ast.PrimaryKeyConstraint
	return nil
}

func (p *Parser) handleTableConstraintUnique(constraint *ast.ConstraintNode) error {
	p.advance()
	p.skipWhitespace()
	// Optional KEY or INDEX keyword
	if p.current.Type == lexer.TokenIdentifier {
		keyword := strings.ToUpper(p.current.Value)
		if keyword == "KEY" || keyword == "INDEX" {
			p.advance()
			p.skipWhitespace()
			// Check for optional constraint name after UNIQUE KEY
			if p.current.Type == lexer.TokenIdentifier && p.current.Value != "(" {
				constraint.Name = p.current.Value
				p.advance()
				p.skipWhitespace()
			}
		}
	}
	constraint.Type = ast.UniqueConstraint

	return nil
}

func (p *Parser) handleTableConstraintForeignKey(constraint *ast.ConstraintNode) error {
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "KEY"); err != nil {
		return fmt.Errorf("expected KEY after FOREIGN: %w", err)
	}
	constraint.Type = ast.ForeignKeyConstraint
	return nil
}

func (p *Parser) handleTableConstraintCheck(constraint *ast.ConstraintNode) {
	p.advance()
	constraint.Type = ast.CheckConstraint
}

func (p *Parser) handleTableConstraintExclude(constraint *ast.ConstraintNode) error {
	p.advance()
	p.skipWhitespace()

	// Expect USING keyword
	if err := p.expect(lexer.TokenIdentifier, "USING"); err != nil {
		return fmt.Errorf("expected USING after EXCLUDE: %w", err)
	}

	p.skipWhitespace()

	// Get the index method (e.g., "gist", "btree")
	usingMethod, err := p.expectIdentifier()
	if err != nil {
		return fmt.Errorf("expected index method after USING: %w", err)
	}

	p.skipWhitespace()

	// Expect opening parenthesis for elements
	if err := p.expect(lexer.TokenOperator, "("); err != nil {
		return fmt.Errorf("expected '(' after USING method: %w", err)
	}

	// Parse exclude elements until closing parenthesis
	var elements strings.Builder
	parenCount := 1
	lastWasSpace := false

	for parenCount > 0 && !p.isAtEnd() {
		if p.current.Type == lexer.TokenOperator {
			switch p.current.Value {
			case "(":
				parenCount++
			case ")":
				parenCount--
			}
		}
		if parenCount > 0 {
			if p.current.Type == lexer.TokenWhitespace {
				// Only add one space if we haven't just added one
				if !lastWasSpace && elements.Len() > 0 {
					elements.WriteString(" ")
					lastWasSpace = true
				}
			} else {
				elements.WriteString(p.current.Value)
				lastWasSpace = false
			}
		}
		p.advance()
	}

	constraint.Type = ast.ExcludeConstraint
	constraint.UsingMethod = usingMethod
	constraint.ExcludeElements = strings.TrimSpace(elements.String())

	return nil
}

func (p *Parser) handleTableConstraintSpatial(constraint *ast.ConstraintNode) error {
	p.advance()
	p.skipWhitespace()
	// Expect INDEX keyword
	if err := p.expect(lexer.TokenIdentifier, "INDEX"); err != nil {
		return fmt.Errorf("expected INDEX after SPATIAL: %w", err)
	}
	// Treat as a special unique constraint for now
	constraint.Type = ast.UniqueConstraint
	constraint.Name = "SPATIAL_INDEX"
	return nil
}

func (p *Parser) handleTableConstraintIndex(constraint *ast.ConstraintNode) {
	p.advance()
	p.skipWhitespace()
	// Check for optional constraint name after INDEX/KEY
	if p.current.Type == lexer.TokenIdentifier && p.current.Value != "(" {
		constraint.Name = p.current.Value
		p.advance()
		p.skipWhitespace()
	}
	// Treat as a unique constraint for now
	constraint.Type = ast.UniqueConstraint
}

func (p *Parser) parseTableColumnList(constraint *ast.ConstraintNode) error {
	// Parse column list for PRIMARY KEY, UNIQUE, FOREIGN KEY
	// Skip for CHECK and EXCLUDE constraints as they have different syntax
	if constraint.Type == ast.CheckConstraint || constraint.Type == ast.ExcludeConstraint {
		return nil
	}
	if err := p.expect(lexer.TokenOperator, "("); err != nil {
		return fmt.Errorf("expected '(' for constraint columns: %w", err)
	}

	p.skipWhitespace()

	// Parse column names
	for {
		column, err := p.parseConstraintColumn()
		if err != nil {
			return err
		}
		constraint.Columns = append(constraint.Columns, column.Name)
		constraint.ColumnParts = append(constraint.ColumnParts, column)

		p.skipWhitespace()

		if p.current.MatchOperatorValue(",") {
			p.advance()
			p.skipWhitespace()
			continue
		}

		if p.current.MatchOperatorValue(")") {
			break
		}

		return fmt.Errorf("expected ',' or ')' in column list at position %d", p.current.Start)
	}

	if err := p.expect(lexer.TokenOperator, ")"); err != nil {
		return err
	}

	return nil
}

func (p *Parser) handleTablePrimaryKeyInclude(constraint *ast.ConstraintNode) error {
	if constraint.Type != ast.PrimaryKeyConstraint {
		return nil
	}
	p.skipWhitespace()
	if !p.current.MatchIdentifierValue("INCLUDE") {
		return nil
	}
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenOperator, "("); err != nil {
		return fmt.Errorf("expected '(' after PRIMARY KEY INCLUDE: %w", err)
	}
	p.skipWhitespace()
	for {
		column, err := p.expectIdentifier()
		if err != nil {
			return fmt.Errorf("expected PRIMARY KEY INCLUDE column name: %w", err)
		}
		constraint.IncludeColumns = append(constraint.IncludeColumns, column)
		p.skipWhitespace()
		if p.current.MatchOperatorValue(",") {
			p.advance()
			p.skipWhitespace()
			continue
		}
		if p.current.MatchOperatorValue(")") {
			break
		}
		return fmt.Errorf("expected ',' or ')' in PRIMARY KEY INCLUDE list at position %d", p.current.Start)
	}
	return p.expect(lexer.TokenOperator, ")")
}

func (p *Parser) parseConstraintColumn() (ast.ConstraintColumn, error) {
	columnName, err := p.expectIdentifier()
	if err != nil {
		return ast.ConstraintColumn{}, fmt.Errorf("expected column name: %w", err)
	}
	column := ast.ConstraintColumn{Name: columnName}

	p.skipWhitespace()
	if p.current.MatchOperatorValue("(") {
		p.advance()
		p.skipWhitespace()
		prefix, err := p.expectIdentifier()
		if err != nil {
			return ast.ConstraintColumn{}, fmt.Errorf("expected column prefix length: %w", err)
		}
		column.Prefix = prefix
		p.skipWhitespace()
		if err := p.expect(lexer.TokenOperator, ")"); err != nil {
			return ast.ConstraintColumn{}, fmt.Errorf("expected ')' after column prefix length: %w", err)
		}
		p.skipWhitespace()
	}

	if p.current.MatchIdentifierValue("DESC") {
		column.Desc = true
		p.advance()
		p.skipWhitespace()
	} else if p.current.MatchIdentifierValue("ASC") {
		p.advance()
		p.skipWhitespace()
	}

	return column, nil
}

func (p *Parser) handleTableForeignKey(constraint *ast.ConstraintNode) error {
	if constraint.Type != ast.ForeignKeyConstraint {
		return nil
	}
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "REFERENCES"); err != nil {
		return fmt.Errorf("expected REFERENCES after FOREIGN KEY: %w", err)
	}

	fkRef, err := p.parseForeignKeyReference()
	if err != nil {
		return err
	}
	constraint.Reference = fkRef
	return nil
}

func (p *Parser) handleTableCheck(constraint *ast.ConstraintNode) error {
	if constraint.Type != ast.CheckConstraint {
		return nil
	}
	expr, err := p.parseCheckExpression()
	if err != nil {
		return err
	}
	constraint.Expression = expr
	return nil
}

func (p *Parser) handleTableExcludeWhere(constraint *ast.ConstraintNode) error {
	if constraint.Type != ast.ExcludeConstraint {
		return nil
	}

	p.skipWhitespace()

	// Check for optional WHERE clause
	if p.current.Type == lexer.TokenIdentifier && strings.ToUpper(p.current.Value) == "WHERE" {
		whereCondition, err := p.parseExcludeWhereCondition()
		if err != nil {
			return err
		}
		constraint.WhereCondition = whereCondition
	}

	return nil
}

func (p *Parser) parseExcludeWhereCondition() (string, error) {
	p.advance()
	p.skipWhitespace()

	// Expect opening parenthesis for WHERE condition
	if err := p.expect(lexer.TokenOperator, "("); err != nil {
		return "", fmt.Errorf("expected '(' after WHERE: %w", err)
	}

	// Parse WHERE condition until closing parenthesis
	var condition strings.Builder
	parenCount := 1
	for parenCount > 0 && !p.isAtEnd() {
		if p.current.Type == lexer.TokenOperator {
			switch p.current.Value {
			case "(":
				parenCount++
			case ")":
				parenCount--
			}
		}
		if parenCount > 0 {
			// Skip whitespace tokens but preserve structure
			if p.current.Type != lexer.TokenWhitespace {
				if condition.Len() > 0 {
					condition.WriteString(" ")
				}
				condition.WriteString(p.current.Value)
			}
		}
		p.advance()
	}

	return strings.TrimSpace(condition.String()), nil
}

// parseTableConstraint parses table-level constraints.
func (p *Parser) parseTableConstraint() (*ast.ConstraintNode, error) {
	p.skipWhitespace()

	constraint := &ast.ConstraintNode{}

	// Check for CONSTRAINT name
	if err := p.handleTableConstraintName(constraint); err != nil {
		return nil, err
	}

	// Parse constraint type
	if p.current.Type != lexer.TokenIdentifier {
		return nil, fmt.Errorf("expected constraint type, got %s at position %d", p.current.Type, p.current.Start)
	}

	var err error
	constraintType := strings.ToUpper(p.current.Value)
	switch constraintType {
	case "PRIMARY":
		err = p.handleTableConstraintPrimaryKey(constraint)
	case "UNIQUE":
		err = p.handleTableConstraintUnique(constraint)
	case "FOREIGN":
		err = p.handleTableConstraintForeignKey(constraint)
	case "CHECK":
		p.handleTableConstraintCheck(constraint)
	case "EXCLUDE":
		err = p.handleTableConstraintExclude(constraint)
	case "SPATIAL":
		err = p.handleTableConstraintSpatial(constraint)
	case "INDEX", "KEY":
		p.handleTableConstraintIndex(constraint)
	default:
		err = fmt.Errorf("unsupported constraint type: %s at position %d", constraintType, p.current.Start)
	}

	if err != nil {
		return nil, err
	}

	p.skipWhitespace()
	if constraint.Type == ast.UniqueConstraint {
		constraint.NullsDistinct, err = p.parseNullsDistinctClause()
		if err != nil {
			return nil, err
		}
		p.skipWhitespace()
	}

	// Parse column list for PRIMARY KEY, UNIQUE, FOREIGN KEY
	err = p.parseTableColumnList(constraint)
	if err != nil {
		return nil, err
	}

	err = p.handleTablePrimaryKeyInclude(constraint)
	if err != nil {
		return nil, err
	}

	// Handle FOREIGN KEY REFERENCES
	err = p.handleTableForeignKey(constraint)
	if err != nil {
		return nil, err
	}

	// Handle CHECK expression
	err = p.handleTableCheck(constraint)
	if err != nil {
		return nil, err
	}

	// Handle EXCLUDE WHERE clause
	err = p.handleTableExcludeWhere(constraint)
	if err != nil {
		return nil, err
	}

	return constraint, nil
}

func (p *Parser) handleTableEngine(table *ast.CreateTableNode) error {
	// Handle MySQL/MariaDB ENGINE
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenOperator, "="); err != nil {
		return fmt.Errorf("expected '=' after ENGINE: %w", err)
	}
	p.skipWhitespace()
	value, err := p.expectIdentifier()
	if err != nil {
		return fmt.Errorf("expected engine value: %w", err)
	}
	table.SetOption("ENGINE", value)
	return nil
}

func (p *Parser) handleTableCharset(table *ast.CreateTableNode, option string) error {
	// Handle CHARSET
	p.advance()
	p.skipWhitespace()
	if option == "CHARACTER" {
		if err := p.expect(lexer.TokenIdentifier, "SET"); err != nil {
			return fmt.Errorf("expected SET after CHARACTER: %w", err)
		}
		p.skipWhitespace()
	}
	if p.current.MatchOperatorValue("=") {
		p.advance()
		p.skipWhitespace()
	}
	value, err := p.expectIdentifier()
	if err != nil {
		return fmt.Errorf("expected charset value: %w", err)
	}
	table.SetOption("CHARSET", value)
	return nil
}

func (p *Parser) handleTableCollate(table *ast.CreateTableNode) error {
	// Handle COLLATE
	p.advance()
	p.skipWhitespace()
	if p.current.MatchOperatorValue("=") {
		p.advance()
		p.skipWhitespace()
	}
	value, err := p.expectIdentifier()
	if err != nil {
		return fmt.Errorf("expected collate value: %w", err)
	}
	table.SetOption("COLLATE", value)
	return nil
}

func (p *Parser) handleTableComment(table *ast.CreateTableNode) error {
	// Handle COMMENT
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenOperator, "="); err != nil {
		return fmt.Errorf("expected '=' after COMMENT: %w", err)
	}
	p.skipWhitespace()
	if p.current.Type != lexer.TokenString {
		return fmt.Errorf("expected string for comment value at position %d", p.current.Start)
	}
	table.Comment = p.current.Value
	p.advance()
	return nil
}

func (p *Parser) handleTableAutoIncrement(table *ast.CreateTableNode) error {
	// Handle AUTO_INCREMENT
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenOperator, "="); err != nil {
		return fmt.Errorf("expected '=' after AUTO_INCREMENT: %w", err)
	}
	p.skipWhitespace()
	// Handle numeric values which might be tokenized as operators
	var value string
	switch {
	case p.current.Type == lexer.TokenIdentifier:
		value = p.current.Value
		p.advance()
	case p.current.Type == lexer.TokenOperator && isNumeric(p.current.Value):
		value = p.current.Value
		p.advance()
	default:
		return fmt.Errorf("expected auto increment value: got %s at position %d", p.current.Type, p.current.Start)
	}

	table.SetOption("AUTO_INCREMENT", value)
	return nil
}

func (p *Parser) handleTableDefault(table *ast.CreateTableNode) error {
	// Handle DEFAULT CHARSET
	// Handle DEFAULT CHARSET syntax
	p.advance()
	p.skipWhitespace()

	if !p.current.MatchIdentifierValue("CHARSET") {
		// Unknown DEFAULT option, stop parsing
		return fmt.Errorf("unsupported DEFAULT option: %s at position %d", p.current.Value, p.current.Start)
	}

	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenOperator, "="); err != nil {
		return fmt.Errorf("expected '=' after DEFAULT CHARSET: %w", err)
	}
	p.skipWhitespace()
	value, err := p.expectIdentifier()
	if err != nil {
		return fmt.Errorf("expected charset value: %w", err)
	}
	table.SetOption("CHARSET", value)
	return nil
}

func (p *Parser) handleRowFormat(table *ast.CreateTableNode) error {
	// Handle ROW_FORMAT
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenOperator, "="); err != nil {
		return fmt.Errorf("expected '=' after ROW_FORMAT: %w", err)
	}
	p.skipWhitespace()
	value, err := p.expectIdentifier()
	if err != nil {
		return fmt.Errorf("expected row format value: %w", err)
	}
	table.SetOption("ROW_FORMAT", value)
	return nil
}

func (p *Parser) handleTablespace(table *ast.CreateTableNode) error {
	// Handle TABLESPACE
	p.advance()
	p.skipWhitespace()
	value, err := p.expectIdentifier()
	if err != nil {
		return fmt.Errorf("expected tablespace name: %w", err)
	}
	table.SetOption("TABLESPACE", value)
	return nil
}

// parseTableOptions parses table options like ENGINE, CHARSET, etc.
func (p *Parser) parseTableOptions(table *ast.CreateTableNode) error {
	for {
		// Check for timeout to prevent infinite loops
		if err := p.checkTimeout(); err != nil {
			return err
		}

		p.skipWhitespace()
		if p.current.MatchOperatorValue(",") {
			p.advance()
			continue
		}

		if p.current.Type != lexer.TokenIdentifier {
			break
		}

		var err error
		option := strings.ToUpper(p.current.Value)
		if option == "AS" || option == "SELECT" || option == "GO" {
			return nil
		}
		switch option {
		case "ENGINE":
			err = p.handleTableEngine(table)
		case "CHARSET", "CHARACTER":
			err = p.handleTableCharset(table, option)
		case "COLLATE":
			err = p.handleTableCollate(table)
		case "COMMENT":
			err = p.handleTableComment(table)
		case "AUTO_INCREMENT":
			err = p.handleTableAutoIncrement(table)
		case "DEFAULT":
			// Handle DEFAULT CHARSET syntax
			err = p.handleTableDefault(table)
		case "WITH":
			// Handle PostgreSQL WITH clause
			err = p.parsePostgreSQLWithClause(table)
		case "ROW_FORMAT":
			err = p.handleRowFormat(table)
		case "TABLESPACE":
			// Handle PostgreSQL TABLESPACE
			err = p.handleTablespace(table)
		case "WITHOUT":
			err = p.handleSQLiteWithoutRowID(table)
		case "STRICT":
			table.SetOption("STRICT", "true")
			p.advance()
		default:
			// Unknown option, stop parsing
			err = fmt.Errorf("unsupported table option: %s at position %d", option, p.current.Start)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Parser) handleSQLiteWithoutRowID(table *ast.CreateTableNode) error {
	if err := p.expect(lexer.TokenIdentifier, "WITHOUT"); err != nil {
		return err
	}
	if err := p.expect(lexer.TokenIdentifier, "ROWID"); err != nil {
		return fmt.Errorf("expected ROWID after WITHOUT: %w", err)
	}
	table.SetOption("WITHOUT_ROWID", "true")
	return nil
}

// parsePostgreSQLWithClause parses PostgreSQL WITH clause for table options.
func (p *Parser) parsePostgreSQLWithClause(table *ast.CreateTableNode) error {
	if err := p.expect(lexer.TokenIdentifier, "WITH"); err != nil {
		return err
	}

	p.skipWhitespace()

	// Expect opening parenthesis
	if err := p.expect(lexer.TokenOperator, "("); err != nil {
		return fmt.Errorf("expected '(' after WITH: %w", err)
	}

	// Parse key-value pairs
	for {
		p.skipWhitespace()

		// Check for closing parenthesis
		if p.current.Type == lexer.TokenOperator && p.current.Value == ")" {
			break
		}

		// Get option name
		if p.current.Type != lexer.TokenIdentifier {
			return fmt.Errorf("expected option name in WITH clause, got %s at position %d", p.current.Type, p.current.Start)
		}
		optionName := p.current.Value
		p.advance()

		p.skipWhitespace()

		// Expect equals sign
		if err := p.expect(lexer.TokenOperator, "="); err != nil {
			return fmt.Errorf("expected '=' after option name '%s': %w", optionName, err)
		}

		p.skipWhitespace()

		// Get option value (can be identifier, number, or boolean)
		var optionValue string
		switch p.current.Type {
		case lexer.TokenIdentifier:
			optionValue = p.current.Value
			p.advance()
		case lexer.TokenString:
			optionValue = p.current.Value
			p.advance()
		default:
			// Handle numeric values and other tokens
			optionValue = p.current.Value
			p.advance()
		}

		// Store the option
		table.SetOption(optionName, optionValue)

		p.skipWhitespace()

		// Check for comma (more options) or closing parenthesis
		if p.current.MatchOperatorValue(",") {
			p.advance()
			continue
		}

		if p.current.MatchOperatorValue(")") {
			break
		}

		return fmt.Errorf("expected ',' or ')' in WITH clause at position %d", p.current.Start)
	}

	// Consume closing parenthesis
	if err := p.expect(lexer.TokenOperator, ")"); err != nil {
		return err
	}

	return nil
}

// parseAlterStatement parses ALTER TABLE statements.
func (p *Parser) parseAlterStatement() (*ast.AlterTableNode, error) {
	if err := p.expect(lexer.TokenIdentifier, "ALTER"); err != nil {
		return nil, err
	}

	p.skipWhitespace()

	if err := p.expect(lexer.TokenIdentifier, "TABLE"); err != nil {
		return nil, fmt.Errorf("expected TABLE after ALTER: %w", err)
	}

	p.skipWhitespace()

	if p.current.MatchIdentifierValue("ONLY") {
		p.advance()
		p.skipWhitespace()
	}

	// Get table name
	tableName, err := p.parseQualifiedIdentifier("table name")
	if err != nil {
		return nil, err
	}

	alterNode := &ast.AlterTableNode{
		Name:       tableName,
		Operations: make([]ast.AlterOperation, 0),
	}

	// Parse alter operations
	for {
		p.skipWhitespace()

		if p.isAtEnd() || p.current.Type == lexer.TokenSemicolon {
			break
		}

		operation, err := p.parseAlterOperation()
		if err != nil {
			return nil, err
		}

		if operation != nil {
			alterNode.Operations = append(alterNode.Operations, operation)
		}

		p.skipWhitespace()

		// Check for comma (multiple operations)
		if p.current.Type == lexer.TokenOperator && p.current.Value == "," {
			p.advance()
			continue
		}

		break
	}

	return alterNode, nil
}

func (p *Parser) parseDropStatement() (ast.Node, error) {
	if err := p.expect(lexer.TokenIdentifier, "DROP"); err != nil {
		return nil, err
	}

	p.skipWhitespace()

	if p.current.Type != lexer.TokenIdentifier {
		return nil, fmt.Errorf("expected DROP target, got %s at position %d", p.current.Type, p.current.Start)
	}

	target := strings.ToUpper(p.current.Value)
	switch target {
	case "TABLE":
		return p.parseDropTable()
	default:
		return nil, fmt.Errorf("unsupported DROP target: %s at position %d", target, p.current.Start)
	}
}

func (p *Parser) parseDropTable() (*ast.DropTableNode, error) {
	if err := p.expect(lexer.TokenIdentifier, "TABLE"); err != nil {
		return nil, err
	}

	p.skipWhitespace()

	ifExists := false
	if p.current.MatchIdentifierValue("IF") {
		p.advance()
		p.skipWhitespace()
		if err := p.expect(lexer.TokenIdentifier, "EXISTS"); err != nil {
			return nil, fmt.Errorf("expected EXISTS after DROP TABLE IF: %w", err)
		}
		p.skipWhitespace()
		ifExists = true
	}

	tableNames, err := p.parseDropTableNames()
	if err != nil {
		return nil, err
	}

	dropTable := ast.NewDropTable(tableNames[0]).SetNames(tableNames)
	if ifExists {
		dropTable.SetIfExists()
	}

	p.skipWhitespace()
	if p.current.MatchIdentifierValue("CASCADE") {
		dropTable.SetCascade()
		p.advance()
		return dropTable, nil
	}
	if p.current.MatchIdentifierValue("RESTRICT") {
		p.advance()
	}
	return dropTable, nil
}

func (p *Parser) parseDropTableNames() ([]string, error) {
	var names []string
	for {
		tableName, err := p.parseQualifiedIdentifier("table name")
		if err != nil {
			return nil, err
		}
		names = append(names, tableName)

		p.skipWhitespace()
		if !p.current.MatchOperatorValue(",") {
			return names, nil
		}
		p.advance()
		p.skipWhitespace()
	}
}

// parseAlterOperation parses individual ALTER TABLE operations.
func (p *Parser) parseAlterOperation() (ast.AlterOperation, error) {
	p.skipWhitespace()

	if p.current.Type != lexer.TokenIdentifier {
		return nil, fmt.Errorf("expected ALTER operation, got %s at position %d", p.current.Type, p.current.Start)
	}

	operation := strings.ToUpper(p.current.Value)
	switch operation {
	case "ADD":
		return p.parseAddOperation()
	case "DROP":
		return p.parseDropOperation()
	case "MODIFY", "ALTER":
		return p.parseModifyOperation()
	case "RENAME":
		return p.parseRenameOperation()
	default:
		return nil, fmt.Errorf("unsupported ALTER operation: %s at position %d", operation, p.current.Start)
	}
}

func (p *Parser) parseRenameOperation() (ast.AlterOperation, error) {
	if err := p.expect(lexer.TokenIdentifier, "RENAME"); err != nil {
		return nil, err
	}
	p.skipWhitespace()

	if p.current.MatchIdentifierValue("COLUMN") {
		return p.parseRenameColumnOperation()
	}
	if !p.current.MatchIdentifierValue("TO") {
		return nil, fmt.Errorf("expected TO or COLUMN after RENAME at position %d", p.current.Start)
	}

	p.advance()
	p.skipWhitespace()
	newName, err := p.parseQualifiedIdentifier("new table name")
	if err != nil {
		return nil, err
	}
	return &ast.RenameTableOperation{NewName: newName}, nil
}

func (p *Parser) parseRenameColumnOperation() (*ast.RenameColumnOperation, error) {
	if err := p.expect(lexer.TokenIdentifier, "COLUMN"); err != nil {
		return nil, err
	}
	p.skipWhitespace()

	oldName, err := p.expectIdentifier()
	if err != nil {
		return nil, fmt.Errorf("expected old column name: %w", err)
	}
	p.skipWhitespace()

	if err := p.expect(lexer.TokenIdentifier, "TO"); err != nil {
		return nil, fmt.Errorf("expected TO after old column name: %w", err)
	}
	p.skipWhitespace()

	newName, err := p.expectIdentifier()
	if err != nil {
		return nil, fmt.Errorf("expected new column name: %w", err)
	}
	return &ast.RenameColumnOperation{OldName: oldName, NewName: newName}, nil
}

// parseAddOperation parses ADD COLUMN and ADD CONSTRAINT operations.
func (p *Parser) parseAddOperation() (ast.AlterOperation, error) {
	if err := p.expect(lexer.TokenIdentifier, "ADD"); err != nil {
		return nil, err
	}

	p.skipWhitespace()

	if p.isAlterAddConstraintStart() {
		constraint, err := p.parseTableConstraint()
		if err != nil {
			return nil, err
		}
		return &ast.AddConstraintOperation{Constraint: constraint}, nil
	}

	// Optional COLUMN keyword
	if p.current.Type == lexer.TokenIdentifier && strings.ToUpper(p.current.Value) == "COLUMN" {
		p.advance()
		p.skipWhitespace()
	}

	// Parse column definition
	column, err := p.parseColumnDefinition(nil)
	if err != nil {
		return nil, err
	}

	return &ast.AddColumnOperation{Column: column}, nil
}

func (p *Parser) isAlterAddConstraintStart() bool {
	if p.current.Type != lexer.TokenIdentifier {
		return false
	}
	switch strings.ToUpper(p.current.Value) {
	case "CONSTRAINT", "PRIMARY", "UNIQUE", "FOREIGN", "CHECK", "EXCLUDE":
		return true
	default:
		return false
	}
}

// parseDropOperation parses DROP COLUMN operations.
func (p *Parser) parseDropOperation() (*ast.DropColumnOperation, error) {
	if err := p.expect(lexer.TokenIdentifier, "DROP"); err != nil {
		return nil, err
	}

	p.skipWhitespace()

	// Optional COLUMN keyword
	if p.current.Type == lexer.TokenIdentifier && strings.ToUpper(p.current.Value) == "COLUMN" {
		p.advance()
		p.skipWhitespace()
	}

	// Get column name
	columnName, err := p.expectIdentifier()
	if err != nil {
		return nil, fmt.Errorf("expected column name: %w", err)
	}

	return &ast.DropColumnOperation{ColumnName: columnName}, nil
}

// parseModifyOperation parses MODIFY/ALTER COLUMN operations.
func (p *Parser) parseModifyOperation() (*ast.ModifyColumnOperation, error) {
	operation := strings.ToUpper(p.current.Value)
	p.advance()

	p.skipWhitespace()

	// For ALTER COLUMN, expect COLUMN keyword
	switch operation {
	case "ALTER":
		if err := p.expect(lexer.TokenIdentifier, "COLUMN"); err != nil {
			return nil, fmt.Errorf("expected COLUMN after ALTER: %w", err)
		}
		p.skipWhitespace()
	case "MODIFY":
		// Optional COLUMN keyword for MODIFY
		if p.current.Type == lexer.TokenIdentifier && strings.ToUpper(p.current.Value) == "COLUMN" {
			p.advance()
			p.skipWhitespace()
		}
	}

	// Parse column definition
	column, err := p.parseColumnDefinition(nil)
	if err != nil {
		return nil, err
	}

	return &ast.ModifyColumnOperation{Column: column}, nil
}

// parseCreateIndex parses CREATE INDEX statements.
func (p *Parser) parseCreateIndex() (*ast.IndexNode, error) {
	if err := p.expect(lexer.TokenIdentifier, "INDEX"); err != nil {
		return nil, err
	}

	return p.parseCreateIndexAfterKeyword(regularIndexType)
}

// regularIndexType is the zero value for IndexNode.Type: a plain CREATE INDEX
// without a MySQL-family FULLTEXT or SPATIAL prefix.
const regularIndexType = ""

func (p *Parser) parseCreateIndexAfterKeyword(indexType string) (*ast.IndexNode, error) {
	p.skipWhitespace()

	// Get index name
	indexName, err := p.expectIdentifier()
	if err != nil {
		return nil, fmt.Errorf("expected index name: %w", err)
	}

	p.skipWhitespace()

	if err := p.expect(lexer.TokenIdentifier, "ON"); err != nil {
		return nil, fmt.Errorf("expected ON after index name: %w", err)
	}

	p.skipWhitespace()

	// Get table name
	tableName, err := p.parseQualifiedIdentifier("table name")
	if err != nil {
		return nil, err
	}

	p.skipWhitespace()
	if p.current.MatchIdentifierValue("USING") {
		p.advance()
		p.skipWhitespace()
		usingType, err := p.expectIdentifier()
		if err != nil {
			return nil, fmt.Errorf("expected index method after USING: %w", err)
		}
		indexType = usingType
		p.skipWhitespace()
	}

	columns, err := p.parseCreateIndexColumns()
	if err != nil {
		return nil, err
	}
	p.skipWhitespace()
	includeColumns, err := p.parseCreateIndexIncludeColumns()
	if err != nil {
		return nil, err
	}
	p.skipWhitespace()
	nullsDistinct, err := p.parseNullsDistinctClause()
	if err != nil {
		return nil, err
	}
	p.skipWhitespace()
	storageParams, err := p.parseCreateIndexStorageParams()
	if err != nil {
		return nil, err
	}

	index := ast.NewIndex(indexName, tableName, columns...)
	index.Type = indexType
	index.IncludeColumns = includeColumns
	index.NullsDistinct = nullsDistinct
	index.StorageParams = storageParams
	return index, nil
}

func (p *Parser) parseCreateIndexIncludeColumns() ([]string, error) {
	if !p.current.MatchIdentifierValue("INCLUDE") {
		return nil, nil
	}
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenOperator, "("); err != nil {
		return nil, fmt.Errorf("expected '(' after index INCLUDE: %w", err)
	}
	p.skipWhitespace()
	var columns []string
	for {
		column, err := p.expectIdentifier()
		if err != nil {
			return nil, fmt.Errorf("expected index INCLUDE column name: %w", err)
		}
		columns = append(columns, column)
		p.skipWhitespace()
		if p.current.MatchOperatorValue(",") {
			p.advance()
			p.skipWhitespace()
			continue
		}
		if p.current.MatchOperatorValue(")") {
			break
		}
		return nil, fmt.Errorf("expected ',' or ')' in index INCLUDE list at position %d", p.current.Start)
	}
	return columns, p.expect(lexer.TokenOperator, ")")
}

func (p *Parser) parseNullsDistinctClause() (*bool, error) {
	if !p.current.MatchIdentifierValue("NULLS") {
		return nil, nil
	}
	p.advance()
	p.skipWhitespace()
	nullsDistinct := true
	if p.current.MatchIdentifierValue("NOT") {
		nullsDistinct = false
		p.advance()
		p.skipWhitespace()
	}
	if err := p.expect(lexer.TokenIdentifier, "DISTINCT"); err != nil {
		return nil, fmt.Errorf("expected DISTINCT after NULLS clause: %w", err)
	}
	return &nullsDistinct, nil
}

func (p *Parser) parseCreateIndexStorageParams() (map[string]string, error) {
	if !p.current.MatchIdentifierValue("WITH") {
		return nil, nil
	}
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenOperator, "("); err != nil {
		return nil, fmt.Errorf("expected '(' after index WITH: %w", err)
	}
	p.skipWhitespace()

	params := map[string]string{}
	for {
		key, err := p.expectIdentifier()
		if err != nil {
			return nil, fmt.Errorf("expected index storage parameter name: %w", err)
		}
		p.skipWhitespace()
		if err := p.expect(lexer.TokenOperator, "="); err != nil {
			return nil, fmt.Errorf("expected '=' after index storage parameter %q: %w", key, err)
		}
		p.skipWhitespace()
		value, err := p.parseCreateIndexStorageParamValue()
		if err != nil {
			return nil, err
		}
		params[key] = value
		p.skipWhitespace()

		if p.current.MatchOperatorValue(",") {
			p.advance()
			p.skipWhitespace()
			continue
		}
		if p.current.MatchOperatorValue(")") {
			break
		}
		return nil, fmt.Errorf("expected ',' or ')' in index WITH parameter list at position %d", p.current.Start)
	}
	if err := p.expect(lexer.TokenOperator, ")"); err != nil {
		return nil, err
	}
	return params, nil
}

func (p *Parser) parseCreateIndexStorageParamValue() (string, error) {
	if p.current.Type == lexer.TokenEOF || p.current.MatchOperatorValue(",") || p.current.MatchOperatorValue(")") {
		return "", fmt.Errorf("expected index storage parameter value at position %d", p.current.Start)
	}
	value := p.current.Value
	p.advance()
	if unquoted, err := strconv.Unquote(value); err == nil {
		return unquoted, nil
	}
	if len(value) >= 2 && strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
		value = strings.TrimSuffix(strings.TrimPrefix(value, "'"), "'")
		return strings.ReplaceAll(value, "''", "'"), nil
	}
	return value, nil
}

func (p *Parser) parseCreateIndexColumns() ([]string, error) {
	if err := p.expect(lexer.TokenOperator, "("); err != nil {
		return nil, fmt.Errorf("expected '(' for index columns: %w", err)
	}

	var columns []string
	var column strings.Builder
	parenDepth := 0
	lastWasSpace := false

columnList:
	for !p.isAtEnd() {
		if p.current.Type == lexer.TokenOperator {
			switch p.current.Value {
			case "(":
				parenDepth++
			case ")":
				if parenDepth == 0 {
					break columnList
				}
				parenDepth--
			case ",":
				if parenDepth == 0 {
					columnText := strings.TrimSpace(column.String())
					if columnText == "" {
						return nil, fmt.Errorf("expected column or expression before ',' in index column list at position %d", p.current.Start)
					}
					columns = append(columns, columnText)
					column.Reset()
					lastWasSpace = false
					p.advance()
					p.skipWhitespace()
					continue
				}
			}
		}

		appendTokenValue(&column, p.current, &lastWasSpace)
		p.advance()
	}

	if p.isAtEnd() {
		return nil, fmt.Errorf("expected ')' for index columns before end of input")
	}

	columns = append(columns, strings.TrimSpace(column.String()))
	if slices.Contains(columns, "") {
		return nil, fmt.Errorf("expected column or expression in index column list at position %d", p.current.Start)
	}

	if err := p.expect(lexer.TokenOperator, ")"); err != nil {
		return nil, err
	}
	return columns, nil
}

func appendTokenValue(builder *strings.Builder, token lexer.Token, lastWasSpace *bool) {
	if token.Type == lexer.TokenWhitespace {
		if builder.Len() > 0 && !*lastWasSpace {
			builder.WriteString(" ")
			*lastWasSpace = true
		}
		return
	}

	builder.WriteString(token.Value)
	*lastWasSpace = false
}

// parseCreateUniqueIndex parses CREATE UNIQUE INDEX statements.
// Note: The INDEX token has already been consumed by parseCreateStatement
func (p *Parser) parseCreateUniqueIndex() (*ast.IndexNode, error) {
	index, err := p.parseCreateIndexAfterKeyword(regularIndexType)
	if err != nil {
		return nil, err
	}
	index.SetUnique()
	return index, nil
}

// parseCreateType parses CREATE TYPE statements (for enums).
func (p *Parser) parseCreateType() (*ast.EnumNode, error) {
	if err := p.expect(lexer.TokenIdentifier, "TYPE"); err != nil {
		return nil, err
	}

	p.skipWhitespace()

	// Get type name
	typeName, err := p.expectIdentifier()
	if err != nil {
		return nil, fmt.Errorf("expected type name: %w", err)
	}

	p.skipWhitespace()

	if err := p.expect(lexer.TokenIdentifier, "AS"); err != nil {
		return nil, fmt.Errorf("expected AS after type name: %w", err)
	}

	p.skipWhitespace()

	if err := p.expect(lexer.TokenIdentifier, "ENUM"); err != nil {
		return nil, fmt.Errorf("expected ENUM after AS: %w", err)
	}

	p.skipWhitespace()

	// Parse enum values
	if err := p.expect(lexer.TokenOperator, "("); err != nil {
		return nil, fmt.Errorf("expected '(' for enum values: %w", err)
	}

	var values []string
	for {
		p.skipWhitespace()

		if p.current.Type == lexer.TokenOperator && p.current.Value == ")" {
			break
		}

		if p.current.Type != lexer.TokenString {
			return nil, fmt.Errorf("expected string value for enum at position %d", p.current.Start)
		}

		// Remove quotes from string value
		value := p.current.Value
		if len(value) >= 2 && (value[0] == '\'' || value[0] == '"') {
			value = value[1 : len(value)-1]
		}
		values = append(values, value)
		p.advance()

		p.skipWhitespace()

		if p.current.MatchOperatorValue(",") {
			p.advance()
			continue
		}

		if p.current.MatchOperatorValue(")") {
			break
		}

		return nil, fmt.Errorf("expected ',' or ')' in enum values at position %d", p.current.Start)
	}

	if err := p.expect(lexer.TokenOperator, ")"); err != nil {
		return nil, err
	}

	return ast.NewEnum(typeName, values...), nil
}

// parseCreateDomain parses CREATE DOMAIN statements (PostgreSQL).
func (p *Parser) parseCreateDomain() (*ast.CreateTypeNode, error) {
	if err := p.expect(lexer.TokenIdentifier, "DOMAIN"); err != nil {
		return nil, err
	}

	p.skipWhitespace()

	// Get domain name
	domainName, err := p.parseQualifiedIdentifier("domain name")
	if err != nil {
		return nil, err
	}

	p.skipWhitespace()

	if err := p.expect(lexer.TokenIdentifier, "AS"); err != nil {
		return nil, fmt.Errorf("expected AS after domain name: %w", err)
	}

	p.skipWhitespace()

	// Get base type
	baseType, err := p.parseColumnType()
	if err != nil {
		return nil, fmt.Errorf("expected base type: %w", err)
	}

	domainDef := ast.NewDomainTypeDef(baseType)

	// Parse optional domain constraints.
	for {
		p.skipWhitespace()

		if p.current.Type != lexer.TokenIdentifier {
			break
		}

		keyword := strings.ToUpper(p.current.Value)
		handled := true
		switch keyword {
		case "CHECK":
			p.advance()
			p.skipWhitespace()
			checkExpr, err := p.parseCheckExpression()
			if err != nil {
				return nil, fmt.Errorf("expected check expression: %w", err)
			}
			domainDef.SetCheck(checkExpr)
		case "NOT":
			p.advance()
			p.skipWhitespace()
			if err := p.expect(lexer.TokenIdentifier, "NULL"); err != nil {
				return nil, fmt.Errorf("expected NULL after NOT in domain definition: %w", err)
			}
			domainDef.SetNotNull()
		case "DEFAULT":
			p.advance()
			defaultValue, err := p.parseDefaultValue()
			if err != nil {
				return nil, fmt.Errorf("expected domain default value: %w", err)
			}
			domainDef.Default = defaultValue
		default:
			handled = false
		}
		if !handled {
			break
		}
	}

	return ast.NewCreateType(domainName, domainDef), nil
}

// parseCommentStatement parses COMMENT ON statements (PostgreSQL).
func (p *Parser) parseCommentStatement() (*ast.CommentNode, error) {
	if err := p.expect(lexer.TokenIdentifier, "COMMENT"); err != nil {
		return nil, err
	}

	p.skipWhitespace()

	if err := p.expect(lexer.TokenIdentifier, "ON"); err != nil {
		return nil, fmt.Errorf("expected ON after COMMENT: %w", err)
	}

	p.skipWhitespace()

	// Parse the object type (TABLE, COLUMN, etc.)
	objectType, err := p.expectIdentifier()
	if err != nil {
		return nil, fmt.Errorf("expected object type: %w", err)
	}

	p.skipWhitespace()

	// Parse the object name (could be table.column for columns)
	var objectName strings.Builder
	for p.current.Type == lexer.TokenIdentifier || p.current.MatchOperatorValue(".") {
		objectName.WriteString(p.current.Value)
		p.advance()
	}

	p.skipWhitespace()

	if err := p.expect(lexer.TokenIdentifier, "IS"); err != nil {
		return nil, fmt.Errorf("expected IS after object name: %w", err)
	}

	p.skipWhitespace()

	// Get the comment text
	if p.current.Type != lexer.TokenString {
		return nil, fmt.Errorf("expected string for comment text at position %d", p.current.Start)
	}

	commentText := fmt.Sprintf("COMMENT ON %s %s IS %s",
		strings.ToUpper(objectType), objectName.String(), p.current.Value)
	p.advance()

	return ast.NewComment(commentText), nil
}
