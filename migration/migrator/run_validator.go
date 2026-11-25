package migrator

// WithStatementValidator returns a copy of the migrator that validates every
// statement of every SQL body a run executes with validator, whatever loaded
// the migration: a filesystem provider, [CreateMigrationFromSQL], or an
// [AppliedHistory] assembled from migrations loaded elsewhere. A nil validator
// removes the one a previous call installed.
//
// A run asks it of every body the run selected, repeatable migrations
// included, in the direction the run executes the body, before the run's first
// statement, so a refused statement leaves nothing of the run applied. A
// migration whose body is Go code rather than SQL has no statement to
// validate.
//
// It is asked beside the validator a filesystem provider installs with the
// [WithStatementValidator] option, not in place of it. Where the run executes a
// body with the transactions the body opens itself ([WithOwnTransactions]), a
// validator that is a [TransactionBoundaryValidator] names the body's own
// transaction control, which is then not validated as a statement. In every
// other body the run refuses transaction control before it asks the
// validator, and the validator reads each statement on its own.
func (m *Migrator) WithStatementValidator(validator StatementValidator) *Migrator {
	tmp := *m
	tmp.statementValidator = validator
	return &tmp
}

// refuseWhatTheValidatorRefuses refuses the body of direction, run as own
// says, when the validator [Migrator.WithStatementValidator] installed refuses
// a statement of it. A migration without an SQL body of direction is not
// asked.
func (m *Migrator) refuseWhatTheValidatorRefuses(
	migration *Migration,
	direction MigrationDirection,
	own ownTransactionRun,
) error {
	if m.statementValidator == nil || !migration.hasSQLExecutor(direction) {
		return nil
	}
	validator := m.statementValidator
	if own.blocks.Empty() {
		// Only a body the run executes with its own transactions has
		// transaction control the validator may name as the body's.
		validator = statementsOnly{validator}
	}
	statements := splitSQLStatementsForConnection(m.conn, migrationSQLForDirection(migration, direction))
	return validateMigrationStatements(migrationSourceName(migration, direction), statements, validator)
}

// statementsOnly is a [StatementValidator] read statement by statement: it
// hides the [TransactionBoundaryValidator] the validator it carries may be.
type statementsOnly struct {
	StatementValidator
}
