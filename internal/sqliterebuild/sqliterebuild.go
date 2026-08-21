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
	"go.5x5.cz/ptah/core/platform/capability"
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
	if unwrapped, ok := beginWithoutTransaction(conn.Info().Capabilities, conn.Writer()); ok {
		return unwrapped, nil
	}
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
	if unwrapped, ok := beginWithoutTransaction(conn.Info().Capabilities, conn.Writer()); ok {
		return unwrapped, nil
	}
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
	if unwrapped, ok := beginWithoutTransaction(conn.Info().Capabilities, conn.Writer()); ok {
		return unwrapped, nil
	}
	if slices.ContainsFunc(sqlTexts, sqliteforeignkeys.BracketsSQL) {
		return beginWithoutForeignKeys(ctx, conn)
	}
	return conn.SchemaWriter().BeginTransaction(ctx)
}

// beginWithoutTransaction answers for a target that refuses schema statements
// inside an explicit transaction, and reports whether it did.
//
// Spanner's PostgreSQL interface is that target: measured on the Cloud Spanner
// emulator behind PGAdapter 0.55.2, every declarative apply failed on its first
// statement with `DDL statements are only allowed outside explicit
// transactions` (SQLSTATE 25000), while the same DDL applies unwrapped. So the
// whole declarative workflow was unreachable there, and nothing said so --
// the capability line is probed every run and the probe applies nothing
// (stokaro/ptah#1793).
//
// The decision is capability.DDLInsideTransaction rather than a dialect name,
// because the reasons differ and the answers do not travel together: MySQL
// commits DDL implicitly and still takes the wrapper, ClickHouse has no
// cross-statement transactions at all, and Spanner has them and refuses DDL
// inside one.
//
// This is the only place schema SQL opens a transaction, which is why the
// answer belongs here rather than in each writer.
func beginWithoutTransaction(
	caps capability.Capabilities,
	writer types.SchemaExecutor,
) (types.SchemaTransaction, bool) {
	if writer == nil || caps.Has(capability.DDLInsideTransaction) {
		return nil, false
	}
	return unwrappedTransaction{writer: writer}, true
}

// unwrappedTransaction runs statements straight at the writer and answers
// Commit and Rollback without doing anything.
//
// Rollback is the honest part: there is no transaction to undo, and every
// statement that already ran has already taken effect. A caller that rolls back
// here is not returned to its starting state, which is exactly what a target
// without transactional DDL offers and what capability.TransactionalDDL
// records separately.
type unwrappedTransaction struct {
	writer types.SchemaExecutor
}

func (t unwrappedTransaction) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	return t.writer.ExecuteSQL(ctx, sqlExpr, args...)
}

// IsDryRun reports what the writer beneath reports, so a preview stays a
// preview through the unwrapped path.
func (t unwrappedTransaction) IsDryRun() bool { return t.writer.IsDryRun() }

func (t unwrappedTransaction) Commit() error { return nil }

func (t unwrappedTransaction) Rollback() error { return nil }

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
