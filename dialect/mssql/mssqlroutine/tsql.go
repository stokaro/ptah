package mssqlroutine

import (
	"strings"

	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlcompound"
	"ptah.run/internal/sqlserverbatch"
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
	// kindLabel is a label that a GOTO jumps to. It runs nothing.
	kindLabel kind = "label"
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
// The statement boundaries are the ones the dev replay guard judges, read by
// ptah.run/internal/sqlserverbatch: T-SQL does not require a semicolon
// between statements, and the server ends one where a word that can only
// start a statement follows a word that can end one. A body whose boundaries
// that reader cannot find is one opaque block, so a host reports what it does
// as unknown rather than as what a guess at its statements would say.
func parseBody(bodySQL string) []statement {
	read, err := sqlserverbatch.Read(bodySQL)
	if err != nil {
		return []statement{{kind: kindBlock, sql: strings.TrimSpace(bodySQL)}}
	}
	if len(read) > 0 && read[0].Kind == sqlserverbatch.Block && !opensTry(read[0].SQL) {
		read = append(read[0].Statements, read[1:]...)
	}
	statements := make([]statement, 0, len(read))
	for index := 0; index < len(read); index++ {
		current := read[index]
		if index+1 < len(read) && returnsQuery(current, read[index+1]) {
			statements = append(statements, statement{kind: kindReturn, sql: bodySQL[current.Start:read[index+1].End]})
			index++
			continue
		}
		statements = append(statements, statement{kind: classify(current), sql: current.SQL})
	}
	return statements
}

// opensTry reports whether a block is BEGIN TRY ... END CATCH. Its statements
// run with the CATCH block's after them, so it stays one control statement.
func opensTry(blockSQL string) bool {
	return newBodyParser(blockSQL).nextSignificantValue(1) == "TRY"
}

// returnsQuery reports whether a bare RETURN is followed by the query an
// inline table-valued function returns: RETURN SELECT ... or RETURN WITH ...
// SELECT .... The reader splits them, as the server splits them in a batch;
// in a function body they are one statement.
func returnsQuery(current, next sqlserverbatch.Statement) bool {
	if current.Kind != sqlserverbatch.Simple || !strings.EqualFold(current.SQL, "RETURN") || next.Kind != sqlserverbatch.Simple {
		return false
	}
	first := newBodyParser(next.SQL).nextSignificantValue(0)
	return first == "SELECT" || first == "WITH"
}

// classify names a statement the reader found. A procedure named with no EXEC
// in front of it runs as EXEC of the same name, so it is classified as one.
func classify(read sqlserverbatch.Statement) kind {
	switch read.Kind {
	case sqlserverbatch.Block:
		return kindBlock
	case sqlserverbatch.If:
		return kindIf
	case sqlserverbatch.While:
		return kindWhile
	case sqlserverbatch.Label:
		return kindLabel
	case sqlserverbatch.Module:
		return kindRaw
	case sqlserverbatch.Call:
		return newBodyParser("EXEC " + read.SQL).classifyStatement(0)
	default:
		return newBodyParser(read.SQL).classifyStatement(0)
	}
}

// bodyParser reads the words of one statement the reader found.
type bodyParser struct {
	tokens []lexer.Token
}

func newBodyParser(sql string) bodyParser {
	return bodyParser{tokens: tokenize(sql)}
}

func (p bodyParser) classifyStatement(startIdx int) kind {
	startIdx = p.nextSignificant(startIdx)
	if startIdx == -1 || p.tokens[startIdx].Type != lexer.TokenIdentifier {
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
	"RETURN":  kindReturn,
	"SELECT":  kindSelect,
	"INSERT":  kindInsert,
	"SET":     kindAssignment,
}

// classifyBegin tells a transaction statement, which writes no table, from
// the other BEGIN statements. BEGIN DIALOG and BEGIN CONVERSATION TIMER are
// Service Broker statements, read as written. A BEGIN that opens a block is a
// block the reader found, and never reaches this.
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

func isTrivia(tok lexer.Token) bool {
	return tok.Type == lexer.TokenWhitespace || tok.Type == lexer.TokenComment
}
