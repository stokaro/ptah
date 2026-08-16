// Package sqliterebuild opens the transaction a SQLite table rebuild needs.
//
// It exists because more than one path applies schema SQL -- schema apply and
// the migrator's three transactional appliers -- and a rebuild that works
// through one of them and destroys data through another is worse than a
// rebuild that works through none. One helper, called wherever a transaction
// is opened for schema SQL, is what keeps them from drifting apart.
package sqliterebuild

import (
	"context"
	"slices"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/sqliteforeignkeys"
)

// foreignKeyScopedTransactor is a schema writer that can suspend foreign-key
// enforcement on the connection its transaction runs on.
type foreignKeyScopedTransactor interface {
	BeginTransactionWithoutForeignKeys(ctx context.Context) (types.SchemaTransaction, error)
}

// BeginTransaction opens the transaction that statements will be applied in.
//
// A SQLite plan that rebuilds a table is bracketed by PRAGMA foreign_keys, the
// procedure SQLite's own documentation prescribes: the rebuild drops the
// original table, and that DROP is a foreign-key violation as soon as another
// table references it. Executing the pragma here would accomplish nothing --
// it is silently ignored inside a transaction -- so the enforcement is
// suspended on the connection before the transaction begins instead.
//
// Measured on SQLite 3.51, applying such a plan without this: `FOREIGN KEY
// constraint failed`, or, where the reference cascades, a DROP that quietly
// takes the referencing rows with it.
//
// Anything else gets an ordinary transaction.
func BeginTransaction(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	statements []string,
) (types.SchemaTransaction, error) {
	if sqliteforeignkeys.Brackets(statements) {
		return beginWithoutForeignKeys(ctx, conn)
	}
	return conn.SchemaWriter().BeginTransaction(ctx)
}

// BeginTransactionForSQL is BeginTransaction over SQL that has not been split
// into statements, which is the shape a migration file arrives in.
func BeginTransactionForSQL(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	sqlText string,
) (types.SchemaTransaction, error) {
	if sqliteforeignkeys.BracketsSQL(sqlText) {
		return beginWithoutForeignKeys(ctx, conn)
	}
	return conn.SchemaWriter().BeginTransaction(ctx)
}

// BeginTransactionForAnySQL is BeginTransaction for a run that applies several
// migrations in one transaction. One rebuild anywhere in the batch decides for
// the whole transaction, because the transaction is what the session scopes.
func BeginTransactionForAnySQL(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	sqlTexts []string,
) (types.SchemaTransaction, error) {
	if slices.ContainsFunc(sqlTexts, sqliteforeignkeys.BracketsSQL) {
		return beginWithoutForeignKeys(ctx, conn)
	}
	return conn.SchemaWriter().BeginTransaction(ctx)
}

// beginWithoutForeignKeys opens the transaction a rebuild needs. A writer with
// no session to suspend -- any dialect but SQLite -- gets an ordinary
// transaction, because the bracket it was handed is then somebody else's SQL
// and executing it unchanged is the only honest reading.
func beginWithoutForeignKeys(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
) (types.SchemaTransaction, error) {
	writer := conn.SchemaWriter()
	scoped, ok := writer.(foreignKeyScopedTransactor)
	if !ok || platform.NormalizeDialect(conn.Info().Dialect) != platform.SQLite {
		return writer.BeginTransaction(ctx)
	}
	return scoped.BeginTransactionWithoutForeignKeys(ctx)
}
