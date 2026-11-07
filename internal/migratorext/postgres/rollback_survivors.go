package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/migratorext"
)

// statementFate is what the rollback of a transaction does to a statement that
// ran in it, on an engine whose schema statements do not wait for the end of the
// transaction. It is read from the statement's first word, so it says nothing
// about what a routine the statement calls does.
type statementFate uint8

const (
	// fateOpaque is a statement that may have done either, such as a DO block
	// or a CALL, which on YugabyteDB can run schema statements of their own.
	fateOpaque statementFate = iota
	// fateUndone is a statement the rollback takes back.
	fateUndone
	// fateNeutral is a statement that changes no data, so running it again or
	// not at all changes nothing the database holds.
	fateNeutral
	// fateKept is a schema statement, which stays. Measured on CockroachDB
	// v26.3.5 and on YugabyteDB 2024.2.11, 2025.2.5 and 2026.1.2 with the
	// transaction block setting off: CREATE of a table, index, view, sequence,
	// type, schema or function, ALTER TABLE, DROP TABLE, TRUNCATE, COMMENT and
	// GRANT all stay after a ROLLBACK, on PostgreSQL 18.6 none does.
	fateKept
)

// fateOf reads the fate of statement. A statement this does not name is opaque,
// so a word nobody measured reads as "cannot tell" rather than as either answer.
func fateOf(statement string) statementFate {
	first, second := firstWords(statement)
	switch first {
	case "INSERT", "UPDATE", "DELETE", "MERGE", "SELECT", "WITH", "VALUES", "LOCK":
		return fateUndone
	case "", "SET", "RESET", "SHOW":
		return fateNeutral
	case "CREATE":
		// A temporary table lasts as long as the session, and the run discards
		// its session, so the database holds nothing of it afterward.
		if second == "TEMP" || second == "TEMPORARY" {
			return fateUndone
		}
		return fateKept
	case "ALTER", "DROP", "TRUNCATE", "COMMENT", "GRANT", "REVOKE":
		return fateKept
	default:
		return fateOpaque
	}
}

// firstWords returns the first two words of statement in upper case, past any
// comments, and nothing for a word the statement does not have.
func firstWords(statement string) (first, second string) {
	scanner := lexer.NewLexerWithOptions(statement, dialectlexer.Options(platform.CockroachDB))
	for {
		token := scanner.NextToken()
		switch {
		case token.Type == lexer.TokenEOF:
			return first, ""
		case token.Type == lexer.TokenWhitespace, token.Type == lexer.TokenComment:
			continue
		case first == "":
			first = strings.ToUpper(token.Value)
		default:
			return first, strings.ToUpper(token.Value)
		}
	}
}

// controlsTransaction reports whether statement opens, ends or marks a
// transaction itself.
func controlsTransaction(statement string) bool {
	first, _ := firstWords(statement)
	switch first {
	case "BEGIN", "START", "COMMIT", "END", "ROLLBACK", "ABORT", "SAVEPOINT", "RELEASE":
		return true
	default:
		return false
	}
}

// nothingCouldStay reports whether every statement the evidence names is one the
// rollback takes back or one that changes no data.
func nothingCouldStay(evidence migratorext.RollbackEvidence) bool {
	undone := func(statement string) bool {
		fate := fateOf(statement)
		return fate == fateUndone || fate == fateNeutral
	}
	return !slices.ContainsFunc(evidence.Ran, func(statement string) bool { return !undone(statement) }) &&
		!slices.ContainsFunc(evidence.Unanswered, func(statement string) bool { return !undone(statement) })
}

// cockroachSurvivors reads what a rollback leaves on CockroachDB.
//
// With autocommit_before_ddl on, which it is by default from v25, a schema
// statement commits the transaction around it before it runs, and the session
// then has no transaction: the statements after it commit one by one and the
// ROLLBACK finds nothing to undo. So the answer is the server's report of the
// session, whichever release or setting the server has. Inside a transaction the
// rollback takes everything, and outside one everything that ran stays, and that
// includes a statement ahead of a schema statement the server then refused.
type cockroachSurvivors struct{}

// Kept implements [migratorext.RollbackSurvivors].
func (cockroachSurvivors) Kept(_ context.Context, _ migratorext.Queryer, evidence migratorext.RollbackEvidence) (kept int, known bool) {
	if len(evidence.Unanswered) != 0 || !evidence.SessionKnown {
		return 0, nothingCouldStay(evidence)
	}
	// A body that ends its own transaction leaves a session state that says
	// nothing about the statements ahead of that point.
	if slices.ContainsFunc(evidence.Ran, controlsTransaction) {
		return 0, false
	}
	if !evidence.OutsideTransaction {
		return 0, true
	}
	return len(evidence.Ran), true
}

// yugabyteSurvivors reads what a rollback leaves on YugabyteDB.
//
// With yb_ddl_transaction_block_enabled off, its default, a schema statement
// commits on its own and the transaction around it stays open, so a rollback
// undoes the data and leaves the schema statements. The session cannot tell the
// two apart, so the statements' first words do. The setting cannot change without
// a server restart, and with it on the rollback takes the whole body, as measured
// on 2026.1.2.
type yugabyteSurvivors struct{}

// Kept implements [migratorext.RollbackSurvivors].
func (yugabyteSurvivors) Kept(ctx context.Context, q migratorext.Queryer, evidence migratorext.RollbackEvidence) (kept int, known bool) {
	if nothingCouldStay(evidence) {
		return 0, true
	}
	rollsBackSchema, err := schemaStatementsJoinTransactions(ctx, q)
	if err != nil {
		return 0, false
	}
	if rollsBackSchema {
		return 0, true
	}
	if len(evidence.Unanswered) != 0 {
		return 0, false
	}
	return leadingKept(evidence.Ran)
}

// leadingKept returns how many leading statements stay when every schema
// statement stays and everything else is undone. Schema statements behind one
// the rollback undid are no leading run: a resume could neither skip the first
// nor run the second again.
func leadingKept(statements []string) (kept int, known bool) {
	undone := false
	for index, statement := range statements {
		switch fateOf(statement) {
		case fateKept:
			if undone {
				return 0, false
			}
			kept = index + 1
		case fateUndone:
			undone = true
		case fateOpaque:
			return 0, false
		case fateNeutral:
		}
	}
	return kept, true
}

// schemaStatementsJoinTransactions asks the server for its setting. A release
// with no such setting, as 2024.2 is, keeps schema statements out of them.
func schemaStatementsJoinTransactions(ctx context.Context, q migratorext.Queryer) (bool, error) {
	var setting sql.NullString
	err := q.QueryRowContext(ctx, "SELECT current_setting('yb_ddl_transaction_block_enabled', true)").Scan(&setting)
	if err != nil {
		return false, fmt.Errorf("read yb_ddl_transaction_block_enabled: %w", err)
	}
	switch setting.String {
	case "on":
		return true, nil
	case "", "off":
		return false, nil
	default:
		return false, fmt.Errorf("read yb_ddl_transaction_block_enabled: unexpected value %q", setting.String)
	}
}
