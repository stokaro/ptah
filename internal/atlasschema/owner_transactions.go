package atlasschema

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/plangraph"
	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/sqliterebuild"
	"ptah.run/internal/txrequire"
	"ptah.run/migration/migrator"
	"ptah.run/migration/planner"
	"ptah.run/migration/safety"
)

func (c applyComputation) executionTransactions() [][]plangraph.Transaction {
	result := make([][]plangraph.Transaction, len(c.statements))
	for i, requirements := range c.transactions {
		result[i] = slices.Clone(requirements)
	}
	return slices.Insert(result, c.dataIndex(), make([][]plangraph.Transaction, len(c.dataStatements))...)
}

// checkOwnerTransactions judges owner requirements against the transaction the
// execution on conn opens. On a target whose schema statements run unwrapped,
// the all and file modes open none, so a forbidden transaction holds there and
// a required one cannot. Without a connection the mode alone decides, which
// refuses every forbidden transaction those modes name.
func checkOwnerTransactions(requirements [][]plangraph.Transaction, mode migrator.MigrationTxMode, conn *dbschema.DatabaseConnection) error {
	return checkOwnerTransactionsOpening(requirements, mode, conn == nil || sqliterebuild.OpensTransaction(conn))
}

// checkOwnerTransactionsOpening is checkOwnerTransactions once it is known
// whether the all and file modes open a transaction on the target.
func checkOwnerTransactionsOpening(requirements [][]plangraph.Transaction, mode migrator.MigrationTxMode, opensTransaction bool) error {
	if !hasOwnerTransactions(requirements) {
		return nil
	}
	switch mode {
	case migrator.MigrationTxModeNone:
		return txrequire.CheckRequirements(requirements, false)
	case migrator.MigrationTxModeAll, migrator.MigrationTxModeFile:
		return txrequire.CheckRequirements(requirements, opensTransaction)
	default:
		return fmt.Errorf("%w: unknown schema apply transaction mode %q", ptaherr.ErrUnsupportedFeature, mode)
	}
}

// PreflightExecution checks the prepared operation requirements before rehearsal
// or confirmation. Changed SQL needs a fresh owner assessment when the original
// plan carries owner requirements; retaining its old facts would judge other SQL.
func (p ApplyRuntimePlan) PreflightExecution(sqlText string) error {
	if sqlText != p.SQL() && hasOwnerTransactions(p.plan.transactions) {
		return fmt.Errorf("%w: edited SQL with owner transaction requirements needs a new owner assessment", ptaherr.ErrUnsupportedFeature)
	}
	return checkOwnerTransactions(p.plan.transactions, p.txMode, p.conn)
}

// ExecuteSQL executes the prepared SQL or an edit that PreflightExecution can
// assess. It repeats preflight at the execution boundary. An unchanged plan
// retains its statement boundaries, including provider execution units.
func (p ApplyRuntimePlan) ExecuteSQL(ctx context.Context, sqlText string) error {
	if err := p.PreflightExecution(sqlText); err != nil {
		return err
	}
	if p.dryRun {
		return nil
	}
	if p.conn == nil {
		return fmt.Errorf("schema apply execution requires database connection")
	}
	p.conn.SchemaWriter().SetDryRun(false)
	if sqlText == p.SQL() {
		return ApplyStatements(ctx, p.conn, p.txMode, p.Statements())
	}
	return ApplySQL(ctx, p.conn, p.txMode, sqlText)
}

// PreflightTransaction checks the owner requirements retained by a saved plan
// against the transaction mode opens on conn. It does not replace target
// fingerprint verification or SQL-based preflight.
func (p PlanFile) PreflightTransaction(mode migrator.MigrationTxMode, conn *dbschema.DatabaseConnection) error {
	return checkOwnerTransactions(p.OwnerTransactions(), mode, conn)
}

// OwnerTransactions returns independent requirements in StatementSQL order.
// A nil entry records no owner assessment for that statement.
func (p PlanFile) OwnerTransactions() [][]plangraph.Transaction {
	result := make([][]plangraph.Transaction, len(p.Statements))
	for i, statement := range p.Statements {
		result[i] = slices.Clone(statement.Transactions)
	}
	return result
}

// The same presence check protects edits before execution and rehearsal.
func hasOwnerTransactions(requirements [][]plangraph.Transaction) bool {
	return slices.ContainsFunc(requirements, func(statement []plangraph.Transaction) bool { return len(statement) > 0 })
}

// Execute runs a saved plan's exact statements after checking owner transaction
// requirements. The caller must verify the target and approve the plan first.
func (p PlanFile) Execute(ctx context.Context, conn *dbschema.DatabaseConnection, mode migrator.MigrationTxMode) error {
	if err := p.PreflightTransaction(mode, conn); err != nil {
		return err
	}
	return ApplyStatements(ctx, conn, mode, p.StatementSQL())
}

// captureRenderedStatements keeps SQL, provenance, and owner facts aligned.
func (c *applyComputation) captureRenderedStatements(plan planner.RenderedPlan, opts ApplyOptions) error {
	planned, err := plan.PlannedStatements()
	if err != nil {
		return err
	}
	c.statements = make([]string, 0, len(planned))
	statementNodes := make([]int, 0, len(planned))
	for _, statement := range planned {
		c.statements = append(c.statements, statement.SQL)
		statementNodes = append(statementNodes, statement.Node)
	}
	c.transactions, err = txrequire.OwnerRequirements(plan.Request.Nodes, statementNodes)
	if err != nil {
		return fmt.Errorf("assess owner transaction requirements: %w", err)
	}
	if opts.assessOwnedOperations {
		c.owned, err = safety.OwnerVerdicts(plan.Request.Nodes, statementNodes)
		if err != nil {
			return fmt.Errorf("assess owned operations: %w", err)
		}
	}
	return nil
}
