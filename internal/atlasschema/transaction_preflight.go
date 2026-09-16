package atlasschema

import (
	"fmt"
	"strings"

	"ptah.run/core/platform/capability"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/txrequire"
	"ptah.run/migration/migrator"
)

// PreflightApplyTransaction refuses a schema apply plan that cannot run inside
// the transaction txMode is about to open.
//
// `file` and `all` both run the whole plan in one transaction, and PostgreSQL
// refuses two shapes there with its own SQLSTATE:
//
//	CREATE INDEX CONCURRENTLY -> ERROR: cannot run inside a transaction block (25001)
//	a used enum value         -> ERROR: unsafe use of new value ... (55P04)
//
// The transaction rolls back, so the schema is not at risk. What the operator
// loses is the diagnostic: the server's code names neither the statement that
// forced it nor the flag that fixes it. This names both, before the plan is
// rehearsed, confirmed or sent.
//
// The classification is [txrequire.Analyze], the rule migration/migrator
// applies to a transactional migration file. Analyze reads the statements in
// order rather than one at a time, and that matters here: a plan file or an
// edited plan can create an enum type and add a value to it in the same run,
// which PostgreSQL accepts, and a keyword check would refuse it.
//
// Statements are numbered and quoted the way the executor sees them: without
// the planner's comment headers, and with comment-only entries left out.
//
// `none` runs each statement in its own transaction, where both shapes are
// legal, so it is never refused.
func PreflightApplyTransaction(
	dialect string,
	caps capability.Capabilities,
	txMode migrator.MigrationTxMode,
	statements []string,
) error {
	if txMode == migrator.MigrationTxModeNone {
		return nil
	}
	prepared := make([]txrequire.Statement, 0, len(statements))
	for _, statement := range statements {
		executable := strings.TrimSpace(sqlutil.StripCommentsForDialect(statement, dialect))
		if executable == "" {
			continue
		}
		prepared = append(prepared, txrequire.Statement{Index: len(prepared), SQL: executable})
	}
	result := txrequire.Analyze(dialect, caps, prepared)
	if !result.RequiresAutocommit() {
		return nil
	}
	return &TransactionPreflightError{Finding: result.Findings[0], Total: len(prepared)}
}

// TransactionPreflightError is the refusal [PreflightApplyTransaction] returns.
//
// It carries the finding rather than only its sentence, because the
// compatibility surface refuses the same plan in Atlas's terms: a concurrent
// index there is a diff policy the project configured in atlas.hcl, and naming
// the flag without naming the setting sends the reader to the wrong file. The
// recognition stays here; only the sentence differs.
type TransactionPreflightError struct {
	// Finding is the first statement the analysis refused.
	Finding txrequire.Finding
	// Total is how many executable statements the plan carried, which is what
	// makes "statement 2 of 2" answerable.
	Total int
}

func (e *TransactionPreflightError) Error() string {
	return fmt.Sprintf(
		"the planned changes cannot run inside a transaction: statement %d of %d: %s; "+
			"rerun with --tx-mode none, which commits each statement as it runs\nSQL: %s",
		e.Finding.Statement.Index+1, e.Total, e.Finding.Message, e.Finding.Statement.SQL)
}
