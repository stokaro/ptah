package migrator

import (
	"fmt"
	"strings"

	"ptah.run/dbschema"
	"ptah.run/internal/txrequire"
)

// preflightTransactionRequirements refuses a file that cannot run inside the
// transaction it is about to run inside.
//
// Without it both shapes reach the database and fail there with the server's
// own SQLSTATE, after the earlier statements have run:
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
//
// remedy, when set, words the way out in place of the finding's own, for a
// file whose directory has no directive for it; see [unwrappedRemedy].
func preflightTransactionRequirements(
	conn *dbschema.DatabaseConnection,
	filename,
	sql string,
	statements []string,
	mode migrationExecutionMode,
	remedy func(txrequire.Reason) string,
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
	advice := finding.Remedy
	if remedy != nil {
		advice = remedy(finding.Reason)
	}
	return &MigrationExecutionError{
		Err: fmt.Errorf("%s cannot run inside a transaction: %s%s; %s",
			filename, statementLocation(finding.Statement), finding.Message, advice),
		Statement:      finding.Statement.SQL,
		StatementIndex: finding.Statement.Index + 1,
		Total:          len(statements),
	}
}

// unwrappedRemedy is the way out of a refusal for a file of a directory whose
// runner opens no transaction around a file. Such a runner has no directive
// for the purpose, and a file of one statement already runs without a
// transaction (see [WithOwnTransactions]), so the advice is to move the
// statement into one.
func unwrappedRemedy(reason txrequire.Reason) string {
	if reason == txrequire.ReasonEnumValueUsed {
		return "end the transaction with COMMIT after the ALTER TYPE ... ADD VALUE, or move this statement into a later migration file"
	}
	return "move the statement into a migration file of its own, which runs without a transaction"
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
