package mysqlreplay

import (
	"slices"
	"strings"

	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
	"ptah.run/internal/mysqlstored"
)

// bodyOperation is [sessionAccount.operation] for each statement of the body
// that tokens, a statement that defines an object of definition's kind,
// writes, and names the first refusal one of them is given. An unqualified
// table name in the body is in the object's database: the one its name is
// qualified with, or else database.
func (s sessionAccount) bodyOperation(
	stmt string,
	tokens []lexer.Token,
	reading mysqlstored.Server,
	definition mysqlstored.Definition,
	database string,
) string {
	if schema, _, qualified := strings.Cut(definition.Name, "."); qualified {
		database = schema
	}
	for _, statement := range bodyStatements(stmt, tokens, reading, definition.Kind) {
		if operation := s.operation(stmt, statement, reading, database); operation != "" {
			return operation
		}
	}
	return ""
}

// bodyStatements returns the statements of the body that tokens, the
// significant tokens of stmt that define an object of kind, write, each as
// its significant tokens. A statement begins:
//
//   - after a procedure's parameter list and its characteristics, and after a
//     trigger's FOR EACH ROW and the FOLLOWS or PRECEDES clause after it,
//     where a body that is one statement begins;
//   - after DO, which begins an event's body and the list of a WHILE or of
//     MariaDB's FOR;
//   - after BEGIN, BEGIN NOT ATOMIC, THEN, ELSE, LOOP and REPEAT, which begin
//     a list of a compound statement;
//   - after the conditions of a DECLARE ... HANDLER FOR, where the handler's
//     statement begins;
//   - after each semicolon, which ends a statement of a list.
//
// A label begins no statement of its own: what follows one is BEGIN, LOOP,
// REPEAT, WHILE or FOR, whose lists begin after the words above. A statement
// runs to the next semicolon or to the end of tokens. THEN and
// ELSE begin no statement in a CASE expression, and what follows them there
// is an expression, which starts with no statement the rule reads. A
// function's body begins after its RETURNS clause, whose type is not read
// here: the server refuses a function whose body holds no RETURN, so a body
// that is one statement is a RETURN, and a longer one is a compound statement
// whose words above begin its statements.
func bodyStatements(stmt string, tokens []lexer.Token, reading mysqlstored.Server, kind string) [][]lexer.Token {
	text := tokensWithSemicolons(stmt, tokens, reading)
	starts := statementStarts(text)
	if kind == mysqlstored.Procedure {
		if start, ok := procedureBodyStart(text); ok {
			starts = append(starts, start)
		}
	}
	statements := make([][]lexer.Token, 0, len(starts))
	for _, start := range starts {
		end := start
		for end < len(text) && text[end].Type != lexer.TokenSemicolon {
			end++
		}
		statements = append(statements, text[start:end])
	}
	return statements
}

// tokensWithSemicolons is tokens, the significant tokens [replay.Tokens]
// reads from stmt, with the semicolons between them, which it leaves out.
func tokensWithSemicolons(stmt string, tokens []lexer.Token, reading mysqlstored.Server) []lexer.Token {
	if len(tokens) == 0 {
		return nil
	}
	from := tokens[0].Start
	lex := lexer.NewLexerWithOptions(stmt[from:tokens[len(tokens)-1].End], mysqlstored.LexerOptions(reading))
	var text []lexer.Token
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return text
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		}
		token.Start += from
		token.End += from
		text = append(text, token)
	}
}

// statementStarts returns the index at which each statement of a body begins
// after one of the words [bodyStatements] lists, other than a procedure's
// head.
func statementStarts(tokens []lexer.Token) []int {
	var starts []int
	for index, token := range tokens {
		switch {
		case token.Type == lexer.TokenSemicolon,
			isKeyword(token, "BEGIN", "THEN", "ELSE", "DO", "LOOP", "REPEAT"),
			isKeyword(token, "ATOMIC") && replay.TokenSequenceAt(tokens, index-2, "BEGIN", "NOT"):
			starts = append(starts, index+1)
		case replay.TokenSequenceAt(tokens, index, "FOR", "EACH", "ROW"):
			starts = append(starts, triggerBodyStart(tokens, index+3))
		case replay.TokenSequenceAt(tokens, index, "HANDLER", "FOR"):
			starts = append(starts, handlerStatementStart(tokens, index+2))
		}
	}
	return starts
}

// triggerBodyStart returns where a trigger's body begins: at at, after FOR
// EACH ROW, or after the trigger a FOLLOWS or PRECEDES clause there names.
func triggerBodyStart(tokens []lexer.Token, at int) int {
	if !isKeyword(replay.TokenAt(tokens, at), "FOLLOWS", "PRECEDES") {
		return at
	}
	_, next := replay.QualifiedIdentifierAt(tokens, at+1)
	return next
}

// handlerStatementStart returns where the statement of a DECLARE ... HANDLER
// FOR begins, after the conditions that begin at at, separated by commas:
// SQLSTATE [VALUE] 'state', NOT FOUND, or one word or number, such as
// SQLEXCEPTION, 1062 or the name of a condition.
func handlerStatementStart(tokens []lexer.Token, at int) int {
	for {
		switch {
		case isKeyword(replay.TokenAt(tokens, at), "SQLSTATE"):
			at++
			if isKeyword(replay.TokenAt(tokens, at), "VALUE") {
				at++
			}
			at++
		case replay.TokenSequenceAt(tokens, at, "NOT", "FOUND"):
			at += 2
		default:
			at++
		}
		if !isOperatorAt(tokens, at, ",") {
			return at
		}
		at++
	}
}

// procedureBodyStart returns where a procedure's body begins, after the
// parameter list that follows its name and the characteristics after the
// list, and reports false where no parameter list follows the name.
func procedureBodyStart(tokens []lexer.Token) (int, bool) {
	kind := slices.IndexFunc(tokens, func(token lexer.Token) bool { return isKeyword(token, "PROCEDURE") })
	if kind < 0 {
		return 0, false
	}
	at := kind + 1
	if replay.TokenSequenceAt(tokens, at, "IF", "NOT", "EXISTS") {
		at += 3
	}
	_, open := replay.QualifiedIdentifierAt(tokens, at)
	if open == at || !isOperatorAt(tokens, open, "(") {
		return 0, false
	}
	depth := 0
	for index := open; index < len(tokens); index++ {
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth--
			if depth == 0 {
				return mysqlstored.CharacteristicsEnd(tokens, index+1), true
			}
		}
	}
	return 0, false
}
