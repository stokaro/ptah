package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/atlasfilter"
	"github.com/stokaro/ptah/internal/atlasurl"
	"github.com/stokaro/ptah/internal/schemafile"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
)

type ApplyOptions struct {
	ToURLs  []string
	Exclude []string
	Policy  DiffPolicy
}

type ApplyPlan struct {
	statements []string
}

// ApplyRuntimeOptions configures Atlas schema apply planning and execution.
type ApplyRuntimeOptions struct {
	DevURL  string
	ToURLs  []string
	Exclude []string
	Policy  DiffPolicy
	TxMode  migrator.MigrationTxMode
	DryRun  bool
}

// ApplyRuntimePlan is a prepared Atlas schema apply operation for one open
// database connection.
type ApplyRuntimePlan struct {
	plan   ApplyPlan
	dryRun bool
	conn   *dbschema.DatabaseConnection
	txMode migrator.MigrationTxMode
}

func (p ApplyPlan) HasChanges() bool {
	return len(p.statements) > 0
}

func (p ApplyPlan) SQL() string {
	return FormatMigrationSQL(p.statements)
}

func (p ApplyPlan) Statements() []string {
	return slices.Clone(p.statements)
}

func PlanApply(conn *dbschema.DatabaseConnection, opts ApplyOptions) (ApplyPlan, error) {
	if conn == nil {
		return ApplyPlan{}, errors.New("schema apply planning requires database connection")
	}
	if len(opts.ToURLs) == 0 {
		return ApplyPlan{}, errors.New("schema apply planning requires desired schema URLs")
	}

	current, err := dbschema.ReadSchemaWithSchemas(conn, nil)
	if err != nil {
		return ApplyPlan{}, fmt.Errorf("read database schema: %w", err)
	}
	current, err = atlasfilter.ExcludeDatabase(current, opts.Exclude)
	if err != nil {
		return ApplyPlan{}, fmt.Errorf("apply --exclude to current schema: %w", err)
	}
	desired, err := schemafile.LoadAll(opts.ToURLs, schemafile.Options{Dialect: conn.Info().Dialect})
	if err != nil {
		return ApplyPlan{}, fmt.Errorf("load --to schema: %w", err)
	}
	desired, err = excludeDesiredSchema(desired, opts.Exclude)
	if err != nil {
		return ApplyPlan{}, fmt.Errorf("apply --exclude to desired schema: %w", err)
	}

	diff := applyDiffPolicy(schemadiff.CompareWithDialect(desired, current, conn.Info().Dialect), opts.Policy)
	if !diff.HasChanges() {
		return ApplyPlan{}, nil
	}

	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(diff, desired, conn.Info().Dialect, planner.Options{
		ConcurrentIndexes: opts.Policy.ConcurrentIndexCreate,
	})
	if err != nil {
		return ApplyPlan{}, fmt.Errorf("generate schema apply SQL: %w", err)
	}
	return ApplyPlan{statements: statements}, nil
}

// PrepareApply validates Atlas schema apply runtime inputs and builds the
// executable apply plan for the already-open target database connection.
func PrepareApply(conn *dbschema.DatabaseConnection, opts ApplyRuntimeOptions) (ApplyRuntimePlan, error) {
	if conn == nil {
		return ApplyRuntimePlan{}, errors.New("schema apply requires database connection")
	}
	if err := atlasurl.ValidateDialectMatch(opts.DevURL, conn.Info().Dialect); err != nil {
		return ApplyRuntimePlan{}, err
	}

	plan, err := PlanApply(conn, ApplyOptions{
		ToURLs:  opts.ToURLs,
		Exclude: opts.Exclude,
		Policy:  opts.Policy,
	})
	if err != nil {
		return ApplyRuntimePlan{}, err
	}
	return ApplyRuntimePlan{
		plan:   plan,
		dryRun: opts.DryRun,
		conn:   conn,
		txMode: opts.TxMode,
	}, nil
}

func (p ApplyRuntimePlan) HasChanges() bool {
	return p.plan.HasChanges()
}

func (p ApplyRuntimePlan) SQL() string {
	return p.plan.SQL()
}

func (p ApplyRuntimePlan) Statements() []string {
	return p.plan.Statements()
}

// Execute applies the prepared schema diff. Dry-run and no-op plans return
// without modifying schema state.
func (p ApplyRuntimePlan) Execute(ctx context.Context) error {
	if !p.HasChanges() || p.dryRun {
		return nil
	}
	if p.conn == nil {
		return errors.New("schema apply execution requires database connection")
	}

	p.conn.SchemaWriter().SetDryRun(false)
	return ApplySQL(ctx, p.conn, p.txMode, p.SQL())
}

func ApplySQL(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	txMode migrator.MigrationTxMode,
	sqlText string,
) error {
	if conn == nil {
		return errors.New("schema apply execution requires database connection")
	}

	statements := SplitApplyStatements(sqlText, conn.Info().Dialect)
	switch txMode {
	case migrator.MigrationTxModeNone:
		return executeApplyStatements(ctx, conn.Writer(), statements)
	case migrator.MigrationTxModeFile, migrator.MigrationTxModeAll:
		tx, err := conn.SchemaWriter().BeginTransaction(ctx)
		if err != nil {
			return fmt.Errorf("begin schema apply transaction: %w", err)
		}
		if err := executeApplyStatements(ctx, tx, statements); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit schema apply transaction: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("invalid tx-mode %q", txMode)
	}
}

func SplitApplyStatements(sqlText, dialect string) []string {
	statements := sqlutil.SplitSQLStatementsForDialect(sqlText, dialect)
	filtered := statements[:0]
	for _, stmt := range statements {
		stmt = strings.TrimSpace(sqlutil.StripComments(stmt))
		if stmt != "" {
			filtered = append(filtered, stmt)
		}
	}
	return filtered
}

func FormatMigrationSQL(statements []string) string {
	var out strings.Builder
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		out.WriteString(strings.TrimSuffix(stmt, ";"))
		out.WriteString(";\n")
	}
	return out.String()
}

func executeApplyStatements(ctx context.Context, executor types.SchemaExecutor, statements []string) error {
	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if err := executor.ExecuteSQL(ctx, stmt); err != nil {
			return &migrator.MigrationExecutionError{
				Err:            fmt.Errorf("failed to execute SQL statement: %w", err),
				Statement:      stmt,
				StatementIndex: i + 1,
				Total:          len(statements),
			}
		}
	}
	return nil
}
