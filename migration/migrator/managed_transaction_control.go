package migrator

import (
	"fmt"

	"ptah.run/internal/txcontrol"
)

func (m *Migrator) validatedUpTransactionMode(migration *Migration, mode MigrationTxMode) (ownTransactionRun, error) {
	own, err := m.upOwnTransactionMode(migration, mode)
	if err != nil {
		return ownTransactionRun{}, err
	}
	if err := m.validateManagedTransactionSQL(migration, MigrationDirectionUp, own.mode); err != nil {
		return ownTransactionRun{}, err
	}
	return own, nil
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
