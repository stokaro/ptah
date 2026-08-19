package migrator

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/txrequire"
)

// preflightTransactionRequirements refuses a file that cannot run inside the
// transaction it is about to run inside.
//
// Both shapes it catches used to reach the database and fail there with the
// server's own SQLSTATE, after the earlier statements had already run:
//
//	CREATE INDEX CONCURRENTLY -> ERROR: cannot run inside a transaction block (25001)
//	a used enum value         -> ERROR: unsafe use of new value ... (55P04)
//
// Neither said which file, which statement, or what to do. The transaction
// rolls back, so nothing is half-applied -- the cost is the operator's time,
// not the schema's integrity, and that is what this turns into a sentence
// naming the statement and the two ways out (stokaro/ptah#996).
//
// It runs only for transactional execution. A file marked `no_transaction`
// runs each statement in its own transaction, where both shapes are legal, and
// checking it would refuse the very workflow the directive exists for.
func preflightTransactionRequirements(
	conn *dbschema.DatabaseConnection,
	filename,
	sql string,
	statements []string,
	mode migrationExecutionMode,
) error {
	if mode != migrationExecutionTransactional || conn == nil {
		return nil
	}
	info := conn.Info()
	result := txrequire.Analyze(info.Dialect, info.Capabilities, preflightStatements(sql, statements))
	if !result.RequiresAutocommit() {
		return nil
	}
	finding := result.Findings[0]
	return &MigrationExecutionError{
		Err: fmt.Errorf("%s cannot run inside a transaction: %s%s; %s",
			filename, statementLocation(finding.Statement), finding.Message, finding.Remedy),
		Statement:      finding.Statement.SQL,
		StatementIndex: finding.Statement.Index + 1,
		Total:          len(statements),
	}
}

// statementLocation names where the offending statement is, and says nothing
// when the line could not be established rather than printing a wrong one.
func statementLocation(statement txrequire.Statement) string {
	if statement.Line <= 0 {
		return fmt.Sprintf("statement %d ", statement.Index+1)
	}
	return fmt.Sprintf("line %d ", statement.Line)
}

// preflightStatements pairs each split statement with the line it starts on.
//
// The line is found by walking the original file, because the splitter answers
// what to execute rather than where it came from. A statement the walk cannot
// locate keeps line 0, which the diagnostic prints as-is rather than guessing:
// a wrong line number in a refusal costs more than a missing one.
func preflightStatements(sql string, statements []string) []txrequire.Statement {
	prepared := make([]txrequire.Statement, 0, len(statements))
	cursor := 0
	for index, statement := range statements {
		trimmed := strings.TrimSpace(statement)
		if trimmed == "" {
			continue
		}
		line := 0
		if offset := strings.Index(sql[cursor:], trimmed); offset >= 0 {
			line = 1 + strings.Count(sql[:cursor+offset], "\n")
			cursor += offset + len(trimmed)
		}
		prepared = append(prepared, txrequire.Statement{
			Index: index,
			Line:  line,
			SQL:   trimmed,
		})
	}
	return prepared
}
