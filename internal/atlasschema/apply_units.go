package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/internal/sqliterebuild"
	"ptah.run/internal/txrequire"
	"ptah.run/migration/migrator"
	"ptah.run/migration/planner"
)

// ApplyRenderedPlan executes an approved plan on its selected target. It checks
// target capabilities and all owner transaction requirements before any write.
// Callers must verify target identity, approve omissions, and acquire any lock
// their workflow requires. Native units keep their captured provider identity.
func ApplyRenderedPlan(ctx context.Context, conn *dbschema.DatabaseConnection, mode migrator.MigrationTxMode, plan planner.RenderedPlan) error {
	if conn == nil {
		return errors.New("schema apply execution requires database connection")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	info := conn.Info()
	if plan.Request.Target != info.Dialect || !maps.Equal(plan.Request.Capabilities.Map(), info.Capabilities.Map()) {
		return fmt.Errorf("%w: rendered plan target capabilities differ from the connection", renderer.ErrInvalidResult)
	}
	planned, err := plan.Units()
	if err != nil {
		return err
	}
	indexes := make([]int, len(planned))
	units := make([]renderer.Fragment, len(planned))
	for i, unit := range planned {
		indexes[i], units[i] = unit.Node, unit.Fragment
	}
	requirements, err := txrequire.OwnerRequirements(plan.Request.Nodes, indexes)
	if err != nil {
		return err
	}
	if err := checkOwnerTransactions(requirements, mode, conn); err != nil {
		return err
	}
	return applyUnits(ctx, conn, mode, units)
}

// applyUnits preflights the complete batch before opening a transaction. The
// transaction repeats validation so switching executors cannot bypass it.
func applyUnits(ctx context.Context, conn *dbschema.DatabaseConnection, mode migrator.MigrationTxMode, units []renderer.Fragment) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prepared, statements, err := cloneApplyUnits(units)
	if err != nil {
		return err
	}
	if err := validateApplyUnits(ctx, conn.Writer(), prepared, statements); err != nil {
		return err
	}
	switch mode {
	case migrator.MigrationTxModeNone:
		return executeApplyUnits(ctx, conn.Writer(), prepared, statements)
	case migrator.MigrationTxModeFile, migrator.MigrationTxModeAll:
		tx, err := beginUnitTransaction(ctx, conn, statements)
		if err != nil {
			return fmt.Errorf("begin schema apply transaction: %w", err)
		}
		if err := executeApplyUnits(ctx, tx, prepared, statements); err != nil {
			return errors.Join(err, tx.Rollback())
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit schema apply transaction: %w", errors.Join(err, tx.Rollback()))
		}
		return nil
	default:
		return fmt.Errorf("invalid tx-mode %q", mode)
	}
}

// A nil statement projection marks a native batch; an empty SQL batch has an
// allocated empty projection. No native value is represented by a SQL string.
func cloneApplyUnits(units []renderer.Fragment) ([]renderer.Fragment, []string, error) {
	prepared := make([]renderer.Fragment, len(units))
	statements := make([]string, 0, len(units))
	native := false
	for i, unit := range units {
		cloned, err := unit.Clone()
		if err != nil {
			return nil, nil, err
		}
		prepared[i] = cloned
		native = native || cloned.Native != nil
		statements = append(statements, cloned.SQL)
	}
	if native {
		statements = nil
	}
	return prepared, statements, nil
}

func validateApplyUnits(ctx context.Context, executor catalog.SchemaExecutor, units []renderer.Fragment, statements []string) error {
	if typed, ok := executor.(dbschema.UnitExecutor); ok {
		return typed.ValidateUnits(ctx, units)
	}
	if statements == nil {
		return fmt.Errorf("%w: executor does not support native operations", ptaherr.ErrUnsupportedFeature)
	}
	return ctx.Err()
}

func executeApplyUnits(ctx context.Context, executor catalog.SchemaExecutor, units []renderer.Fragment, statements []string) error {
	if err := validateApplyUnits(ctx, executor, units, statements); err != nil {
		return err
	}
	if typed, ok := executor.(dbschema.UnitExecutor); ok {
		return typed.ExecuteUnits(ctx, units)
	}
	return executeApplyStatements(ctx, executor, statements)
}

func beginUnitTransaction(ctx context.Context, conn *dbschema.DatabaseConnection, statements []string) (catalog.SchemaTransaction, error) {
	if statements == nil {
		return conn.SchemaWriter().BeginTransaction(ctx)
	}
	return sqliterebuild.BeginTransaction(ctx, conn, statements)
}
