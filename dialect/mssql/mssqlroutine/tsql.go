package mssqlroutine

import (
	"slices"
	"strings"

	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlcompound"
)

// kind is what a statement of a T-SQL routine body is, by the word it begins
// with.
type kind string

const (
	kindRaw         kind = "raw"
	kindDeclaration kind = "declaration"
	kindAssignment  kind = "assignment"
	kindBlock       kind = "block"
	kindIf          kind = "if"
	kindWhile       kind = "while"
	kindReturn      kind = "return"
	kindInsert      kind = "insert"
	kindSelect      kind = "select"
	// kindExec and kindExecute are the two spellings of a statement that runs
	// SQL it builds from text. EXEC that calls a procedure by name is raw.
	kindExec    kind = "exec"
	kindExecute kind = "execute"
	// kindTransaction is BEGIN TRAN, BEGIN TRANSACTION or BEGIN DISTRIBUTED
	// TRANSACTION: a BEGIN that opens no block.
	kindTransaction kind = "transaction"
)

// statement is one top-level statement of a T-SQL routine body. A
// control-flow statement carries its whole text, and the statements it runs
// are not split out of it.
type statement struct {
	kind kind
	sql  string
}

// parseBody splits a T-SQL routine body into its top-level statements: the
// statements of an outer BEGIN ... END, or of the body itself when it has
// none.
//
// T-SQL does not require a semicolon between statements, so a statement also
// ends where a keyword that can only start one begins at the top level.
func parseBody(bodySQL string) []statement {
	parser := bodyParser{input: bodySQL, tokens: tokenize(bodySQL)}
	return parser.parseStatements()
}

type bodyParser struct {
	input  string
	tokens []lexer.Token
}

func (p bodyParser) parseStatements() []statement {
	startIdx := p.nextSignificant(0)
	if startIdx == -1 {
		return nil
	}
	if p.tokens[startIdx].MatchIdentifierValue("BEGIN") && !p.isNonBlockBegin(startIdx) {
		return p.parseOuterBlockStatements(startIdx)
	}
	return p.parseStatementRange(startIdx, len(p.tokens)-1)
}

func (p bodyParser) parseOuterBlockStatements(beginIdx int) []statement {
	endIdx := p.findMatchingBlockEnd(beginIdx)
	if endIdx == -1 {
		return []statement{p.statement(beginIdx, len(p.tokens)-1)}
	}
	return p.parseStatementRange(beginIdx+1, endIdx)
}

func (p bodyParser) parseStatementRange(startIdx, endIdx int) []statement {
	statements := make([]statement, 0)
	statementStartIdx := -1
	var depth nesting
	for i := startIdx; i < endIdx; i++ {
		tok := p.tokens[i]
		if tok.MatchOperatorValue("[") {
			i = bracketEnd(p.tokens, i)
			continue
		}
		depth.trackParenthesis(tok)
		if statementStartIdx == -1 {
			if isTrivia(tok) {
				continue
			}
			statementStartIdx = i
		}
		if tok.Type == lexer.TokenIdentifier {
			trackKeyword(tok.Value, p.nextSignificantValue(i+1), &depth.block, &depth.branch)
		}
		if depth.atTopLevel() && p.opensNewStatement(statementStartIdx, i) {
			statements = append(statements, p.statementUntil(statementStartIdx, tok.Start))
			statementStartIdx = i
		}
		if tok.Type == lexer.TokenSemicolon && depth.atTopLevel() && statementStartIdx != -1 {
			statements = append(statements, p.statement(statementStartIdx, i))
			statementStartIdx = -1
		}
	}
	if statementStartIdx != -1 {
		statements = append(statements, p.statementUntil(statementStartIdx, p.tokens[endIdx].Start))
	}
	return statements
}

// nesting counts the nestings that make a keyword something other than the
// start of a statement.
//
// Parentheses count because a keyword inside them belongs to a subquery or an
// expression: without them, `UPDATE t SET c = (SELECT max(v) FROM u)` came
// apart at the SELECT, into three statements (stokaro/ptah#2451).
type nesting struct {
	block  int
	branch int
	paren  int
}

// trackParenthesis follows one token's effect on the parenthesis nesting.
func (d *nesting) trackParenthesis(tok lexer.Token) {
	if tok.MatchOperatorValue("(") {
		d.paren++
		return
	}
	if tok.MatchOperatorValue(")") && d.paren > 0 {
		d.paren--
	}
}

// atTopLevel reports whether a keyword here could begin a statement at all.
func (d nesting) atTopLevel() bool {
	return d.block == 0 && d.branch == 0 && d.paren == 0
}

// opensNewStatement reports whether the token at idx begins a statement rather
// than continuing the one already open.
func (p bodyParser) opensNewStatement(statementStartIdx, idx int) bool {
	return p.startsRecoverableStatement(idx) &&
		statementStartIdx != idx &&
		!p.keywordBelongsToCurrentStatement(statementStartIdx, idx)
}

func (p bodyParser) statement(startIdx, endIdx int) statement {
	if startIdx < 0 || endIdx >= len(p.tokens) || startIdx > endIdx {
		return statement{}
	}
	end := p.tokens[endIdx].End
	if p.tokens[endIdx].Type == lexer.TokenEOF {
		end = p.tokens[endIdx].Start
	}
	return p.statementUntil(startIdx, end)
}

func (p bodyParser) statementUntil(startIdx, end int) statement {
	return statement{
		kind: p.classifyStatement(startIdx),
		sql:  strings.TrimSpace(p.rawFragment(p.tokens[startIdx].Start, end)),
	}
}

func (p bodyParser) classifyStatement(startIdx int) kind {
	if startIdx < 0 || startIdx >= len(p.tokens) {
		return kindRaw
	}
	if p.tokens[startIdx].MatchOperatorValue("@") {
		return kindAssignment
	}
	if p.tokens[startIdx].Type != lexer.TokenIdentifier {
		return kindRaw
	}
	switch keyword := strings.ToUpper(p.tokens[startIdx].Value); keyword {
	case "BEGIN":
		return p.classifyBegin(startIdx)
	case "EXEC", "EXECUTE":
		if p.composesAtRunTime(startIdx) {
			return kind(strings.ToLower(keyword))
		}
		return kindRaw
	default:
		if classified, ok := kindsByKeyword[keyword]; ok {
			return classified
		}
		return kindRaw
	}
}

var kindsByKeyword = map[string]kind{
	"DECLARE": kindDeclaration,
	"IF":      kindIf,
	"WHILE":   kindWhile,
	"RETURN":  kindReturn,
	"SELECT":  kindSelect,
	"INSERT":  kindInsert,
	"SET":     kindAssignment,
}

// classifyBegin tells a BEGIN that opens a block from one that is a statement
// of its own. A transaction statement writes no table. BEGIN DIALOG and BEGIN
// CONVERSATION TIMER are Service Broker statements, read as written.
func (p bodyParser) classifyBegin(startIdx int) kind {
	switch next := p.nextSignificantValue(startIdx + 1); {
	case sqlcompound.SQLServerBeginOpensBlock(next):
		return kindBlock
	case next == "TRAN", next == "TRANSACTION", next == "DISTRIBUTED":
		return kindTransaction
	default:
		return kindRaw
	}
}

// composesAtRunTime reports whether the EXEC or EXECUTE at idx runs a
// statement it builds from text.
//
// EXEC('...') and EXEC (@sql) run a string, and sp_executesql runs one under
// any qualification, with or without a return status assigned to a variable
// first. EXEC naming any other procedure calls a routine that is written down,
// and EXECUTE AS changes the security context; neither composes SQL.
func (p bodyParser) composesAtRunTime(idx int) bool {
	next := p.nextSignificant(idx + 1)
	if next == -1 {
		return false
	}
	if p.tokens[next].MatchOperatorValue("(") {
		return true
	}
	if p.tokens[next].MatchOperatorValue("@") {
		next = p.afterReturnStatus(next)
	}
	return strings.EqualFold(p.lastNamePart(next), "sp_executesql")
}

// afterReturnStatus skips `@status =` from the @ at idx and returns the token
// after the equals sign, or -1 when the variable is not assigned: `EXEC @name`
// runs the procedure a variable names.
func (p bodyParser) afterReturnStatus(idx int) int {
	variable := p.nextSignificant(idx + 1)
	if variable == -1 {
		return -1
	}
	equals := p.nextSignificant(variable + 1)
	if equals == -1 || !p.tokens[equals].MatchOperatorValue("=") {
		return -1
	}
	return p.nextSignificant(equals + 1)
}

// lastNamePart reads the possibly qualified name that starts at idx and
// returns its last part without quotes.
//
// The lexer reads a bracketed name as one identifier, brackets included, and
// a double-quoted one as a string, which T-SQL reads as a name under its
// default QUOTED_IDENTIFIER ON. Trimming either leaves the name; a
// single-quoted string keeps its quotes and names nothing.
func (p bodyParser) lastNamePart(idx int) string {
	last := ""
	for idx != -1 {
		last = strings.Trim(p.tokens[idx].Value, `[]"`)
		dot := p.nextSignificant(idx + 1)
		if dot == -1 || p.tokens[dot].Value != "." {
			return last
		}
		idx = p.nextSignificant(dot + 1)
	}
	return last
}

func (p bodyParser) findMatchingBlockEnd(beginIdx int) int {
	blockDepth := 0
	caseDepth := 0
	for i := beginIdx; i < len(p.tokens); i++ {
		tok := p.tokens[i]
		if tok.MatchOperatorValue("[") {
			i = bracketEnd(p.tokens, i)
			continue
		}
		if tok.Type != lexer.TokenIdentifier {
			continue
		}
		trackKeyword(tok.Value, p.nextSignificantValue(i+1), &blockDepth, &caseDepth)
		if blockDepth == 0 && tok.MatchIdentifierValue("END") {
			return i
		}
	}
	return -1
}

// trackKeyword follows the nesting a keyword opens or closes. Whether a BEGIN
// opens a block and an END closes one is the parser's answer too, which is why
// both ask [sqlcompound].
func trackKeyword(value, nextValue string, blockDepth, caseDepth *int) {
	switch strings.ToUpper(value) {
	case "BEGIN":
		if !sqlcompound.SQLServerBeginOpensBlock(nextValue) {
			return
		}
		(*blockDepth)++
	case "CASE":
		(*caseDepth)++
	case "END":
		if !sqlcompound.SQLServerEndClosesBlock(nextValue) {
			return
		}
		if *caseDepth > 0 {
			(*caseDepth)--
		} else if *blockDepth > 0 {
			(*blockDepth)--
		}
	}
}

func (p bodyParser) nextSignificant(startIdx int) int {
	return nextToken(p.tokens, startIdx)
}

func (p bodyParser) nextSignificantValue(startIdx int) string {
	idx := p.nextSignificant(startIdx)
	if idx == -1 {
		return ""
	}
	return strings.ToUpper(p.tokens[idx].Value)
}

func (p bodyParser) isNonBlockBegin(idx int) bool {
	return !sqlcompound.SQLServerBeginOpensBlock(p.nextSignificantValue(idx + 1))
}

func (p bodyParser) startsRecoverableStatement(idx int) bool {
	if idx < 0 || idx >= len(p.tokens) || p.tokens[idx].Type != lexer.TokenIdentifier {
		return false
	}
	if previousSignificantIsOperator(p.tokens, idx, "@") {
		return false
	}
	switch strings.ToUpper(p.tokens[idx].Value) {
	case "DECLARE", "EXEC", "EXECUTE", "IF", "INSERT", "RETURN", "SELECT", "SET", "WHILE":
		return true
	default:
		return false
	}
}

func (p bodyParser) keywordBelongsToCurrentStatement(statementStartIdx, keywordIdx int) bool {
	if statementStartIdx < 0 || keywordIdx < 0 || keywordIdx >= len(p.tokens) {
		return false
	}
	currentKind := p.classifyStatement(statementStartIdx)
	switch {
	case p.tokens[keywordIdx].MatchIdentifierValue("SET") &&
		p.setClauseBelongsToStatement(statementStartIdx, keywordIdx):
		return true
	case currentKind == kindInsert && p.tokens[keywordIdx].MatchIdentifierValue("SELECT"):
		return true
	case isExec(p.tokens[keywordIdx]) && p.statementOpensWith(statementStartIdx, "INSERT", "GRANT", "DENY", "REVOKE"):
		// INSERT ... EXEC stores what a procedure returns, and GRANT,
		// DENY and REVOKE name the EXECUTE permission.
		return true
	case currentKind == kindReturn && p.tokens[keywordIdx].MatchIdentifierValue("SELECT"):
		return true
	case currentKind == kindIf && !p.tokens[keywordIdx].MatchIdentifierValue("RETURN"):
		return true
	case currentKind == kindWhile && !p.tokens[keywordIdx].MatchIdentifierValue("RETURN"):
		return true
	default:
		return false
	}
}

// isExec reports whether a token is EXEC or EXECUTE.
func isExec(tok lexer.Token) bool {
	return tok.MatchIdentifierValue("EXEC") || tok.MatchIdentifierValue("EXECUTE")
}

// setClauseBelongsToStatement reports whether a SET is the clause of the
// statement it sits inside rather than an assignment of its own.
//
// SET is two different things in T-SQL. `SET @total = 0` is a statement, and
// the SET in `UPDATE t SET c = 1` is a clause. Treating every one as a
// statement split an UPDATE in two and left neither half an update
// (stokaro/ptah#2451).
//
// An UPDATE has exactly one SET clause, so a second SET at the same depth is a
// new statement -- which is what `UPDATE t SET c = 1 SET @x = 2` means, written
// without a separator. A MERGE has one per WHEN branch and T-SQL requires the
// terminating semicolon, so every SET inside it is a clause.
func (p bodyParser) setClauseBelongsToStatement(statementStartIdx, keywordIdx int) bool {
	if p.statementOpensWith(statementStartIdx, "MERGE") {
		return true
	}
	if !p.statementOpensWith(statementStartIdx, "UPDATE") {
		return false
	}
	return !p.hasSetBetween(statementStartIdx, keywordIdx)
}

// statementOpensWith reports whether the statement's first word is one of the
// given keywords. The statement kind cannot answer this: an UPDATE is
// classified raw, which is also what an unrecognized statement is.
func (p bodyParser) statementOpensWith(statementStartIdx int, keywords ...string) bool {
	if statementStartIdx < 0 || statementStartIdx >= len(p.tokens) {
		return false
	}
	if p.tokens[statementStartIdx].Type != lexer.TokenIdentifier {
		return false
	}
	return slices.Contains(keywords, strings.ToUpper(p.tokens[statementStartIdx].Value))
}

// hasSetBetween reports whether a SET already appears inside the statement.
func (p bodyParser) hasSetBetween(fromIdx, toIdx int) bool {
	for i := fromIdx + 1; i < toIdx && i < len(p.tokens); i++ {
		if p.tokens[i].MatchIdentifierValue("SET") {
			return true
		}
	}
	return false
}

func (p bodyParser) rawFragment(start, end int) string {
	if start < 0 || start > end || end > len(p.input) {
		return ""
	}
	return p.input[start:end]
}

// tokenize reads a body with the lexer options the T-SQL routine parser reads
// a routine with: bracketed identifiers, and no # comments, since # starts a
// temporary table name.
func tokenize(sql string) []lexer.Token {
	l := lexer.NewLexerWithOptions(sql, lexer.Options{
		BracketIdentifiers:  true,
		DisableHashComments: true,
	})
	tokens := make([]lexer.Token, 0)
	for {
		tok := l.NextToken()
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

// bracketEnd returns the index of the ] that closes the [ at openIdx, reading
// ]] as an escaped bracket, or openIdx when nothing closes it.
func bracketEnd(tokens []lexer.Token, openIdx int) int {
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

func previousSignificantIsOperator(tokens []lexer.Token, idx int, operator string) bool {
	for i := idx - 1; i >= 0; i-- {
		if isTrivia(tokens[i]) {
			continue
		}
		return tokens[i].MatchOperatorValue(operator)
	}
	return false
}

func isTrivia(tok lexer.Token) bool {
	return tok.Type == lexer.TokenWhitespace || tok.Type == lexer.TokenComment
}
