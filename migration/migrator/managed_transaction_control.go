package migrator

import (
	"fmt"

	"ptah.run/internal/txcontrol"
)

// refuseBodyForItsMode refuses a body of direction that the way it runs, own,
// cannot carry: transaction control in a body Ptah wraps in a transaction
// ([Migrator.validateManagedTransactionSQL]) or runs statement by statement
// without one ([Migrator.validateNoTransactionSQL]), and on a MySQL-family
// database in tx-mode file, a statement the transaction witness cannot follow
// ([Migrator.validateTransactionalProgressSQL]). A body that opens its own
// transactions is not asked: its transaction control is its own.
//
// Each refusal reads the body and the run's options and nothing else, so a run
// asks it of every file before the first of them runs (see
// [Migrator.refuseWhatAFileRefuses]). The step that runs a file asks it again,
// as a guard for a caller that reaches the step without that pass; every run
// path asks the pass first, so no run reaches the second refusal. The
// witness's questions to the database are not here; they are asked when the
// file runs, of the database as the files before it left it.
func (m *Migrator) refuseBodyForItsMode(migration *Migration, direction MigrationDirection, own ownTransactionRun) error {
	if !own.blocks.Empty() {
		return nil
	}
	if own.mode == MigrationTxModeNone {
		return m.validateNoTransactionSQL(migration, direction)
	}
	if err := m.validateManagedTransactionSQL(migration, direction, own.mode); err != nil {
		return err
	}
	if !usesTransactionalProgressWitness(m.connectionDialect(), own.mode) {
		return nil
	}
	return m.validateTransactionalProgressSQL(migration, direction)
}

// validateManagedTransactionSQL refuses a body that could end or replace the
// transaction Ptah owns. A COMMIT inside the body can make an earlier statement
// durable while a later failure leaves the revision claiming nothing applied.
func (m *Migrator) validateManagedTransactionSQL(
	migration *Migration,
	direction MigrationDirection,
	mode MigrationTxMode,
) error {
	if mode == MigrationTxModeNone || !migration.hasSQLExecutor(direction) {
		return nil
	}
	statements := splitSQLStatementsForConnection(m.conn, migrationSQLForDirection(migration, direction))
	for i, statement := range statements {
		if !txcontrol.Controls(statement, m.connectionDialect()) {
			continue
		}
		return fmt.Errorf(
			"migration %d cannot run tx-mode %s %s statement %d because it controls transaction state; "+
				"remove the transaction control and let Ptah manage the transaction",
			migration.Version, mode, direction, i+1,
		)
	}
	return nil
}
