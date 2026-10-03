package migrator

import (
	"context"
	"errors"

	"ptah.run/dbschema"
)

// FileModeRefusal reports why a Migrator would refuse to run body as one
// migration in transaction mode file against the database conn is connected
// to, or nil when it would run it.
//
// Only MySQL and MariaDB refuse a body here. On them a migration in
// transaction mode file runs under Ptah's transaction witness, which refuses a
// statement whose effect it cannot tie to the migration's transaction: a
// trigger, view or routine definition, LOCK TABLES, and a statement that names
// a relation a trigger fires on or a view stands for, among others. The checks
// are the ones [Migrator.MigrateUp] runs before it executes such a migration:
// the statements themselves, the session and its storage engines, and the
// catalog of every database the statements name, all read from conn without
// changing anything. Every other dialect answers nil.
//
// A caller writing a migration file uses the answer to mark the file
// no_transaction, so the file it writes is one the migrator runs. A body that
// passes here can still be refused by a database whose catalog changes before
// it is applied.
func FileModeRefusal(ctx context.Context, conn *dbschema.DatabaseConnection, body string) error {
	if conn == nil {
		return errors.New("file mode refusal requires a database connection")
	}
	if !usesTransactionalProgressWitness(conn.Info().Dialect, MigrationTxModeFile) {
		return nil
	}
	migration := CreateMigrationFromSQL(1, "file mode refusal", body, "")
	m := NewMigrator(conn, NewRegisteredMigrationProvider(migration))
	if err := m.validateTransactionalProgressSQL(migration, MigrationDirectionUp); err != nil {
		return err
	}
	if err := m.requireTransactionalTargetEngines(ctx); err != nil {
		return err
	}
	return m.requireTransactionalTargetIsolation(ctx, migration, MigrationDirectionUp)
}
