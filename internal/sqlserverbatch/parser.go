package sqlserverbatch

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlcompound"
)

// parser reads the tokens of one batch. Whitespace and comments are dropped;
// semicolons stay, since they end statements.
type parser struct {
	text   string
	tokens []lexer.Token
}

func unreadableAt(offset int, reason string) error {
	return &UnreadableError{Reason: reason, Offset: offset}
}

func (p *parser) unreadable(index int, what string) error {
	offset := len(p.text)
	if index < len(p.tokens) {
		offset = p.tokens[index].Start
	}
	return unreadableAt(offset, what)
}

// list reads statements from index until the text ends or an END or an ELSE
// stands where a statement would start, and returns the index it stopped at.
// first is true for the statements of a whole batch, whose first statement
// may be a [Call].
func (p *parser) list(index int, first bool) ([]Statement, int, error) {
	var statements []Statement
	for {
		for index < len(p.tokens) && p.tokens[index].Type == lexer.TokenSemicolon {
			index++
		}
		if index >= len(p.tokens) || p.closesBlock(index) || p.isElse(index) {
			return statements, index, nil
		}
		statement, next, err := p.statement(index, first && len(statements) == 0)
		if err != nil {
			return nil, 0, err
		}
		statements = append(statements, statement)
		index = next
	}
}

// statement reads the statement that starts at index.
func (p *parser) statement(index int, first bool) (Statement, int, error) {
	switch {
	case p.isLabel(index):
		return p.span(Label, index, index+1), index + 2, nil
	case p.keyword(index) == "BEGIN" && sqlcompound.SQLServerBeginOpensBlock(p.word(index+1)):
		if p.word(index+1) == "CATCH" {
			return Statement{}, 0, p.unreadable(index, "a CATCH block that follows no TRY block")
		}
		return p.block(index)
	case p.keyword(index) == "IF":
		return p.conditional(index, If)
	case p.keyword(index) == "WHILE":
		return p.conditional(index, While)
	case p.startsStatement(index):
		return p.simple(index, Simple)
	case first && p.namesProcedure(index):
		return p.simple(index, Call)
	default:
		return Statement{}, 0, p.unreadable(index, fmt.Sprintf("a statement that starts with %q", p.tokens[index].Value))
	}
}

// block reads BEGIN ... END at index, with TRY, CATCH or ATOMIC after BEGIN.
// A TRY block is one statement with the CATCH block that has to follow it.
func (p *parser) block(index int) (Statement, int, error) {
	opener := p.word(index + 1)
	inner := index + 1
	switch opener {
	case "TRY", "CATCH":
		inner++
	case "ATOMIC":
		inner++
		if p.word(inner) == "WITH" && p.op(inner+1, "(") {
			inner = p.closingParenthesis(inner+1) + 1
			if inner == 0 {
				return Statement{}, 0, p.unreadable(index, "a BEGIN ATOMIC WITH whose parentheses do not close")
			}
		}
	}
	statements, next, err := p.list(inner, false)
	if err != nil {
		return Statement{}, 0, err
	}
	if next >= len(p.tokens) || !p.closesBlock(next) {
		return Statement{}, 0, p.unreadable(index, "a BEGIN with no END")
	}
	end := next
	next++
	if opener == "TRY" || opener == "CATCH" {
		if p.word(next) != opener {
			return Statement{}, 0, p.unreadable(end, "an END that does not close BEGIN "+opener)
		}
		end = next
		next++
	}
	if opener == "TRY" {
		if p.word(next) != "BEGIN" || p.word(next+1) != "CATCH" {
			return Statement{}, 0, p.unreadable(index, "a TRY block with no CATCH block")
		}
		catch, after, err := p.block(next)
		if err != nil {
			return Statement{}, 0, err
		}
		statements = append(statements, catch.Statements...)
		block := p.span(Block, index, after-1)
		block.Head, block.Statements = "", statements
		return block, after, nil
	}
	if next < len(p.tokens) && p.tokens[next].Type == lexer.TokenSemicolon {
		end = next
		next++
	}
	block := p.span(Block, index, end)
	block.Head, block.Statements = "", statements
	return block, next, nil
}

// conditional reads an IF or a WHILE at index: the condition, the statement
// it runs and, for an IF, the statement an ELSE runs. The condition ends where
// a statement starts after a word that can end one; a condition cannot hold a
// statement, so its end is where the server reads the statement it runs.
func (p *parser) conditional(index int, kind Kind) (Statement, int, error) {
	what := "an IF"
	if kind == While {
		what = "a WHILE"
	}
	body, err := p.condition(index, what)
	if err != nil {
		return Statement{}, 0, err
	}
	run, next, err := p.statement(body, false)
	if err != nil {
		return Statement{}, 0, err
	}
	statements := []Statement{run}
	if kind == If && next < len(p.tokens) && p.isElse(next) {
		if next+1 >= len(p.tokens) {
			return Statement{}, 0, p.unreadable(next, "an ELSE that runs no statement")
		}
		other, after, err := p.statement(next+1, false)
		if err != nil {
			return Statement{}, 0, err
		}
		statements = append(statements, other)
		next = after
	}
	statement := Statement{
		Kind:       kind,
		SQL:        p.text[p.tokens[index].Start:statements[len(statements)-1].End],
		Start:      p.tokens[index].Start,
		End:        statements[len(statements)-1].End,
		Head:       p.text[p.tokens[index].Start:p.tokens[body-1].End],
		Statements: statements,
	}
	return statement, next, nil
}

// condition reads the condition of the IF or WHILE at index, which what
// names, and returns the index of the statement it runs.
func (p *parser) condition(index int, what string) (int, error) {
	var nested nesting
	for body := index + 1; body < len(p.tokens); body++ {
		if body > index+1 && !nested.open() &&
			(p.isLabel(body) || p.startsStatement(body) && !p.cannotEnd(body-1)) {
			return body, nil
		}
		if p.tokens[body].Type == lexer.TokenSemicolon || !nested.open() && p.isElse(body) {
			return 0, p.unreadable(index, what+" that runs no statement")
		}
		if _, err := p.follow(&nested, body, "an END inside the condition of "+what); err != nil {
			return 0, err
		}
	}
	return 0, p.unreadable(index, what+" that runs no statement")
}

// nesting counts the parentheses and the CASE expressions open at a token. No
// statement starts inside either.
type nesting struct {
	depth, cases int
}

// open reports whether a parenthesis or a CASE is open.
func (n nesting) open() bool {
	return n.depth > 0 || n.cases > 0
}

// follow takes the token at index into nested, and reports whether it opens
// or closes a parenthesis or a CASE. An END that closes a block with no CASE
// open is unreadable here, and inside names where it stands.
func (p *parser) follow(nested *nesting, index int, inside string) (bool, error) {
	switch {
	case p.op(index, "("):
		nested.depth++
	case p.op(index, ")"):
		if nested.depth == 0 {
			return true, p.unreadable(index, "a closing parenthesis that closes none")
		}
		nested.depth--
	case p.keyword(index) == "CASE":
		nested.cases++
	case p.keyword(index) == "END" && (nested.cases > 0 || p.closesBlock(index)):
		if nested.cases == 0 {
			return true, p.unreadable(index, inside)
		}
		nested.cases--
	default:
		return false, nil
	}
	return true, nil
}

// simple reads a statement that runs no other, or a [Call], from index to its
// end: a semicolon, a word that closes a block or starts an ELSE or a label,
// or a statement that starts after a word that can end one and does not
// continue this one (see [scan.continues]).
func (p *parser) simple(index int, kind Kind) (Statement, int, error) {
	if kind == Simple && p.module(index) {
		return p.span(Module, index, len(p.tokens)-1), len(p.tokens), nil
	}
	s := p.newScan(index, kind)
	var nested nesting
	for next := index; next < len(p.tokens); next++ {
		if p.tokens[next].Type == lexer.TokenSemicolon {
			if nested.open() {
				return Statement{}, 0, p.unreadable(next, "a semicolon inside parentheses or a CASE")
			}
			return s.finish(next), next + 1, nil
		}
		if next > index && !nested.open() {
			ends, err := s.endsBefore(next)
			if err != nil {
				return Statement{}, 0, err
			}
			if ends {
				return s.finish(next - 1), next, nil
			}
		}
		nests, err := p.follow(&nested, next, "an END inside parentheses")
		if err != nil {
			return Statement{}, 0, err
		}
		if !nests && nested.depth == 0 {
			s.track(next)
		}
	}
	if nested.open() {
		return Statement{}, 0, p.unreadable(index, "parentheses or a CASE that the text does not close")
	}
	return s.finish(len(p.tokens) - 1), len(p.tokens), nil
}

// span is the statement of kind whose words run from token first to token
// last.
func (p *parser) span(kind Kind, first, last int) Statement {
	start, end := p.tokens[first].Start, p.tokens[last].End
	return Statement{Kind: kind, SQL: p.text[start:end], Start: start, End: end, Head: p.text[start:end]}
}

// module reports a CREATE, ALTER or CREATE OR ALTER at index of a procedure,
// function, trigger or view. The server reads the rest of the batch as its
// body: a statement after it, with or without a semicolon, fails the batch
// (Msg 156) and nothing runs.
func (p *parser) module(index int) bool {
	next := index + 1
	switch p.keyword(index) {
	case "CREATE":
		if p.is(next, "OR", "ALTER") {
			next += 2
		}
	case "ALTER":
	default:
		return false
	}
	switch p.word(next) {
	case "PROC", "PROCEDURE", "FUNCTION", "TRIGGER", "VIEW":
		return true
	default:
		return false
	}
}

// namesProcedure reports a name at index, which as the first statement of a
// batch the server runs as a procedure call: an unquoted name that starts no
// statement, a bracketed or double-quoted one, or a temporary procedure's.
func (p *parser) namesProcedure(index int) bool {
	tok := p.tokens[index]
	switch tok.Type {
	case lexer.TokenString:
		return strings.HasPrefix(tok.Value, `"`)
	case lexer.TokenIdentifier:
		first := tok.Value[0]
		return first < '0' || first > '9'
	default:
		return false
	}
}

// startsStatement reports whether a statement can start at index: a word
// only a statement starts with, or a parenthesized query. A word that also
// names things starts a statement only before the word that makes it one.
func (p *parser) startsStatement(index int) bool {
	if p.op(index, "(") {
		return p.parenthesizedQuery(index)
	}
	switch word := p.keyword(index); word {
	case "":
		return false
	case "ADD":
		return p.is(index+1, "SIGNATURE", "TO") || p.is(index+1, "COUNTER", "SIGNATURE") ||
			p.is(index+1, "SENSITIVITY", "CLASSIFICATION")
	case "ENABLE", "DISABLE":
		return p.is(index+1, "TRIGGER")
	case "GET", "MOVE":
		return p.is(index+1, "CONVERSATION")
	case "SEND":
		return p.is(index+1, "ON")
	case "WITH":
		return p.statementWith(index)
	case "END":
		return !sqlcompound.SQLServerEndClosesBlock(p.word(index + 1))
	default:
		return slices.Contains(statementWords, word)
	}
}

// statementWords are the words a T-SQL statement starts with. The words
// [parser.startsStatement] reads in context are not among them.
var statementWords = []string{
	"ALTER", "BACKUP", "BEGIN", "BREAK", "BULK", "CHECKPOINT", "CLOSE", "COMMIT", "CONTINUE", "CREATE",
	"DBCC", "DEALLOCATE", "DECLARE", "DELETE", "DENY", "DISK", "DROP", "DUMP", "EXEC", "EXECUTE", "FETCH",
	"GOTO", "GRANT", "IF", "INSERT", "KILL", "LINENO", "LOAD", "MERGE", "OPEN", "PRINT", "RAISERROR",
	"READTEXT", "RECEIVE", "RECONFIGURE", "RESTORE", "RETURN", "REVERT", "REVOKE", "ROLLBACK", "SAVE",
	"SELECT", "SET", "SETUSER", "SHUTDOWN", "THROW", "TRUNCATE", "UPDATE", "UPDATETEXT", "USE", "WAITFOR",
	"WHILE", "WRITETEXT",
}

// statementWith reports a WITH at index that starts a statement: a common
// table expression, WITH XMLNAMESPACES or WITH CHANGE_TRACKING_CONTEXT. Every
// other WITH is a clause, such as WITH (NOLOCK) or WITH RECOMPILE.
func (p *parser) statementWith(index int) bool {
	next := index + 1
	if p.is(next, "XMLNAMESPACES") || p.is(next, "CHANGE_TRACKING_CONTEXT") {
		return p.op(next+1, "(")
	}
	if !p.isName(next) {
		return false
	}
	next++
	if p.op(next, "(") {
		next = p.closingParenthesis(next) + 1
		if next == 0 {
			return false
		}
	}
	return p.is(next, "AS") && p.op(next+1, "(")
}

// parenthesizedQuery reports a ( at index that opens a query: SELECT after
// one or more parentheses. The server runs one as a statement of its own after
// a word that can end a statement, SELECT INTO included.
func (p *parser) parenthesizedQuery(index int) bool {
	for p.op(index, "(") {
		index++
	}
	return p.keyword(index) == "SELECT"
}

// cannotEnd reports whether no statement can end with the token at index, so
// that a statement word after it continues the statement it is in. Each word
// here is reserved, so it cannot be a name either; APPLY and ALL are read in
// context, since APPLY is not reserved and ALL can end DISABLE TRIGGER ALL.
// A closing parenthesis ends many statements. An asterisk ends only SELECT *
// with no FROM, which the server refuses to compile (Msg 263), so before a
// parenthesized query it multiplies.
func (p *parser) cannotEnd(index int) bool {
	if index < 0 {
		return false
	}
	if p.tokens[index].Type == lexer.TokenOperator {
		return slices.Contains(continuingOperators, p.tokens[index].Value)
	}
	switch word := p.keyword(index); word {
	case "ALL":
		previous := p.keyword(index - 1)
		return previous == "UNION" || previous == "SELECT" ||
			index > 0 && p.tokens[index-1].Type == lexer.TokenOperator && p.tokens[index-1].Value != ")"
	case "APPLY":
		previous := p.keyword(index - 1)
		return previous == "CROSS" || previous == "OUTER"
	default:
		return slices.Contains(continuingWords, word)
	}
}

// continuingOperators are the operators no statement ends with.
var continuingOperators = []string{",", "(", ".", "=", "<", ">", "!", "+", "-", "*", "/", "%", "&", "|", "^", "~", ":", "{"}

// continuingWords are the reserved words no statement ends with.
var continuingWords = []string{
	"AND", "ANY", "AS", "BETWEEN", "BULK", "BY", "CASE", "COLLATE", "DENY", "DISTINCT", "ESCAPE", "EXCEPT",
	"EXEC", "EXECUTE", "EXISTS", "FOR", "FROM", "GRANT", "HAVING", "IF", "IN", "INTERSECT", "INTO", "IS", "JOIN",
	"LIKE", "NOT", "OF", "OR", "PRINT", "REVOKE", "SELECT", "SOME", "THEN", "TO", "TOP", "UNION", "WHEN",
	"WHERE", "WHILE", "WITH",
}

// closesBlock reports an END at index that closes a block. END CONVERSATION
// is a statement.
func (p *parser) closesBlock(index int) bool {
	return p.keyword(index) == "END" && sqlcompound.SQLServerEndClosesBlock(p.word(index+1))
}

// isElse reports an ELSE at index.
func (p *parser) isElse(index int) bool {
	return p.keyword(index) == "ELSE"
}

// isLabel reports a label at index: a name and one colon. Two colons are a
// scope qualifier, as in SCHEMA::dbo.
func (p *parser) isLabel(index int) bool {
	return index < len(p.tokens) && p.tokens[index].Type == lexer.TokenIdentifier && !p.namePart(index) &&
		p.op(index+1, ":") && !p.op(index+2, ":")
}

// isName reports a name at index: an identifier, bracketed or not, or a
// double-quoted name.
func (p *parser) isName(index int) bool {
	if index >= len(p.tokens) {
		return false
	}
	tok := p.tokens[index]
	return tok.Type == lexer.TokenIdentifier || tok.Type == lexer.TokenString && strings.HasPrefix(tok.Value, `"`)
}

// afterName returns the index after the possibly qualified name at index, or
// index when no name stands there.
func (p *parser) afterName(index int) int {
	if !p.isName(index) {
		return index
	}
	index++
	for p.op(index, ".") {
		index++
		if p.isName(index) {
			index++
		}
	}
	return index
}

// closingParenthesis returns the index of the ) that closes the ( at index,
// or -1.
func (p *parser) closingParenthesis(index int) int {
	depth := 0
	for ; index < len(p.tokens); index++ {
		switch {
		case p.op(index, "("):
			depth++
		case p.op(index, ")"):
			depth--
			if depth == 0 {
				return index
			}
		case p.tokens[index].Type == lexer.TokenSemicolon:
			return -1
		}
	}
	return -1
}

// keyword is [parser.word] for a word in a keyword's place: one that does not
// follow a dot, an @ or a scope qualifier, where it would be part of a name.
func (p *parser) keyword(index int) string {
	if p.namePart(index) {
		return ""
	}
	return p.word(index)
}

// namePart reports a token after a dot, an @ or two colons, which is part of
// a name whatever it spells.
func (p *parser) namePart(index int) bool {
	return p.op(index-1, ".") || p.op(index-1, "@") || p.op(index-1, ":") && p.op(index-2, ":")
}

// word returns the unquoted word at index in upper case, or "" for anything
// else: a bracketed or double-quoted name, a temporary name, a number, an
// operator. Only ASCII letters are folded, as the server folds a keyword;
// ſELECT, with a long s, is a name.
func (p *parser) word(index int) string {
	if index < 0 || index >= len(p.tokens) || p.tokens[index].Type != lexer.TokenIdentifier {
		return ""
	}
	value := p.tokens[index].Value
	if first := value[0]; !isASCIILetter(first) && first != '_' {
		return ""
	}
	return strings.Map(func(r rune) rune {
		if 'a' <= r && r <= 'z' {
			return r - ('a' - 'A')
		}
		return r
	}, value)
}

// is reports whether the words from index on are words, in order.
func (p *parser) is(index int, words ...string) bool {
	for offset, word := range words {
		if p.keyword(index+offset) != word {
			return false
		}
	}
	return true
}

// op reports the operator op at index.
func (p *parser) op(index int, op string) bool {
	return index >= 0 && index < len(p.tokens) && p.tokens[index].MatchOperatorValue(op)
}

// isASCIILetter reports whether b is an ASCII letter.
func isASCIILetter(b byte) bool {
	return 'a' <= b && b <= 'z' || 'A' <= b && b <= 'Z'
}
