package schemalineage

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/lexer"
	"go.5x5.cz/ptah/internal/parser"
)

// RoutineWrite is one table or column a routine body writes.
//
// Column is empty where the statement names the table and no column -- a
// DELETE, a TRUNCATE, or an INSERT with no column list. That is the whole
// table, not an unknown column, and the two must not be spelled the same way.
type RoutineWrite struct {
	Table     string `json:"table"`
	Column    string `json:"column,omitempty"`
	ByRoutine string `json:"by_routine"`
	// Kind separates a function from a procedure.
	Kind string `json:"kind,omitempty"`
	// Statement names what does the writing: insert, update, delete, truncate.
	Statement string `json:"statement"`
}

// deriveProceduralWrites resolves the writes a procedural body performs.
//
// Reads are deliberately not derived here, and the caller says so on every
// procedural routine: a body whose writes were resolved and whose reads were
// not must not read as a body that reads nothing, which is the same rule the
// view half draws its undecided line on (stokaro/ptah#2394).
func deriveProceduralWrites(routine schemamodel.Function, kind string) (writes []RoutineWrite, unresolved []string) {
	body := parser.ParseRoutineBody(routine.Body, routine.Language)
	return walkRoutineStatements(body.Statements, routine.Name, kind)
}

// walkRoutineStatements classifies each statement, descending into the ones
// that carry others.
func walkRoutineStatements(statements []ast.PostgresRoutineStatement, routine, kind string) (writes []RoutineWrite, unresolved []string) {
	for _, statement := range statements {
		stmtWrites, stmtUnresolved := classifyRoutineStatement(statement, routine, kind)
		writes = append(writes, stmtWrites...)
		unresolved = append(unresolved, stmtUnresolved...)
	}
	return writes, unresolved
}

// classifyRoutineStatement answers one of three things about a statement: the
// writes it performs, that it cannot write, or that this analysis cannot tell.
//
// The third is the one that has to stay reachable. An EXECUTE composes SQL at
// run time and can do anything, which is what #1270 is about; a statement whose
// leading word is not recognized is treated the same way, because CALL, MERGE
// and COPY all write and guessing which is worse than saying so.
func classifyRoutineStatement(statement ast.PostgresRoutineStatement, routine, kind string) (writes []RoutineWrite, unresolved []string) {
	if carriesStatements(statement.Kind) {
		return descendOrRefuse(statement, routine, kind)
	}
	if nonWritingKind(statement.Kind) {
		return nil, nil
	}
	if statement.Kind == ast.PostgresRoutineStatementExecute {
		return nil, []string{"an EXECUTE composes its statement at run time"}
	}
	return classifyRawStatement(statement.SQL, routine, kind)
}

// descendOrRefuse walks a control-flow statement's contents, or refuses when
// the parser could not split them.
//
// An empty statement list on a control-flow statement is the parser saying it
// could not read the branch, and the branch text is still in SQL. Reporting
// nothing there would credit the routine with writing nothing inside an IF the
// analysis never opened.
func descendOrRefuse(statement ast.PostgresRoutineStatement, routine, kind string) (writes []RoutineWrite, unresolved []string) {
	if len(statement.Statements) == 0 {
		return nil, []string{fmt.Sprintf("a %s statement's contents could not be read", statement.Kind)}
	}
	return walkRoutineStatements(statement.Statements, routine, kind)
}

// carriesStatements reports whether a kind holds other statements.
func carriesStatements(kind ast.PostgresRoutineStatementKind) bool {
	switch kind {
	case ast.PostgresRoutineStatementBlock, ast.PostgresRoutineStatementException,
		ast.PostgresRoutineStatementIf, ast.PostgresRoutineStatementCase,
		ast.PostgresRoutineStatementLoop:
		return true
	default:
		return false
	}
}

// nonWritingKind reports whether a kind cannot write to a table on its own.
func nonWritingKind(kind ast.PostgresRoutineStatementKind) bool {
	switch kind {
	case ast.PostgresRoutineStatementDeclaration, ast.PostgresRoutineStatementReturn,
		ast.PostgresRoutineStatementRaise, ast.PostgresRoutineStatementPerform:
		return true
	default:
		return false
	}
}

// classifyRawStatement reads a plain statement's leading word.
func classifyRawStatement(sql, routine, kind string) (writes []RoutineWrite, unresolved []string) {
	tokens := tokenize(sql)
	if len(tokens) == 0 {
		return nil, nil
	}
	if isAssignment(tokens) {
		return nil, nil
	}
	leading := strings.ToUpper(unquote(tokens[0].Value))
	if readingOrControlWord(leading) {
		return nil, nil
	}
	target, ok := writeTarget(leading, tokens)
	if !ok {
		return nil, []string{fmt.Sprintf("a statement beginning %s was not recognized", leading)}
	}
	return target.writes(routine, kind), nil
}

// isAssignment reports whether a statement assigns to a variable, which touches
// no table.
//
// The lexer is the SQL one, where `:=` is two operator tokens rather than one:
// SQL has no assignment, and this body is the PL/pgSQL sub-language borrowing
// that tokenizer. Reading `:=` as a single token here matched nothing and made
// every variable assignment an unrecognized statement.
func isAssignment(tokens []lexer.Token) bool {
	if len(tokens) > 1 && tokens[1].Value == ":=" {
		return true
	}
	return len(tokens) > 2 && tokens[1].Value == ":" && tokens[2].Value == "="
}

// readingOrControlWord names the statements that read or steer without writing.
//
// PERFORM and SELECT read, and a read is not reported here at all: the caller
// states on every procedural routine that its reads are unresolved, so listing
// them as unresolved statements would say the same thing once per line.
func readingOrControlWord(word string) bool {
	switch word {
	case "SELECT", "PERFORM", "GET", "NULL", "OPEN", "CLOSE", "FETCH", "MOVE",
		"COMMIT", "ROLLBACK", "SET", "RESET", "ASSERT", "CONTINUE", "EXIT":
		return true
	default:
		return false
	}
}
