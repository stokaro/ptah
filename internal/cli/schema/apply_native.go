package schema

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/migrator"
	"ptah.run/migration/safety"
)

// applyNativeSchemaPlan preserves the captured batch through approval and
// execution. Native payloads never enter a SQL editor or a SQL rehearsal.
func applyNativeSchemaPlan(ctx context.Context, cmd *cobra.Command, opts schemaApplyOptions, plan atlasschema.ApplyRuntimePlan, run *applyRun) (atlasschema.ApplyOutcome, error) {
	if opts.edit {
		if plan.HasInputReferences() {
			return "", fmt.Errorf("%w: approved execution inputs cannot be edited as SQL; edit the desired schema and plan again", ptaherr.ErrUnsupportedFeature)
		}
		return "", fmt.Errorf("%w: a native operation cannot be edited as SQL; edit the desired schema and plan again", ptaherr.ErrUnsupportedFeature)
	}
	if err := plan.Preflight(ctx); err != nil {
		return "", err
	}
	assessmentRuntime, ok := opts.runtime.(safety.Runtime)
	if !ok {
		return "", fmt.Errorf("%w: native operation assessment is unavailable", ptaherr.ErrUnsupportedFeature)
	}
	units, err := plan.ReportUnits(ctx, assessmentRuntime)
	if err != nil {
		return "", err
	}
	run.evidence.Units = units
	fmt.Fprintln(run.human, "Planned changes:")
	printApplyUnits(run.human, units, plan.Annotations())
	if opts.dryRun {
		return atlasschema.ApplyOutcomeDryRun, nil
	}
	if err := plan.SimulateOnDev(ctx, atlasschema.SimulateOptions{
		DevURL: opts.devURL, TargetURL: opts.dbURL, DesiredURLs: opts.toURLs, DevServerDisposable: opts.devServerDisposable,
	}); err != nil {
		return "", err
	}
	approved, err := confirmSchemaApply(cmd, opts, run.human)
	if err != nil {
		return "", err
	}
	if !approved {
		return atlasschema.ApplyOutcomeCanceled, nil
	}
	run.evidence.Dispatched = true
	if err := plan.Execute(ctx); err != nil {
		return "", fmt.Errorf("apply schema changes: %w", err)
	}
	return atlasschema.ApplyOutcomeApplied, nil
}

// printApplyUnits lists SQL statements as written and a native operation by its
// kind, subject and captured assessments. The operation's bytes are not shown.
func printApplyUnits(out io.Writer, units []atlasschema.ApplyUnitReport, annotations []renderer.Annotation) {
	atlasschema.WriteUnitReports(out, units, annotations)
}

// runSchemaApplyNativePlanFile applies a saved plan that holds provider-native
// operations. The operations are decoded through the selected provider and the
// whole plan is preflighted against the transaction mode before the prompt, so
// a dry run reports the refusals a real run would meet. Nothing is dispatched
// under --dry-run.
func runSchemaApplyNativePlanFile(
	ctx context.Context,
	cmd *cobra.Command,
	opts schemaApplyOptions,
	conn *dbschema.DatabaseConnection,
	plan atlasschema.PlanFile,
	txMode migrator.MigrationTxMode,
	run *applyRun,
) (atlasschema.ApplyOutcome, error) {
	execution, err := plan.ExecutionPlan(ctx, conn, opts.runtime)
	if err != nil {
		return "", err
	}
	if err := execution.Preflight(ctx, conn, txMode); err != nil {
		return "", err
	}
	if opts.dryRun {
		return atlasschema.ApplyOutcomeDryRun, nil
	}
	approved, err := confirmSchemaApply(cmd, opts, run.human)
	if err != nil {
		return "", err
	}
	if !approved {
		return atlasschema.ApplyOutcomeCanceled, nil
	}
	conn.SchemaWriter().SetDryRun(false)
	run.evidence.Dispatched = true
	if err := execution.Execute(ctx, conn, txMode); err != nil {
		return "", fmt.Errorf("apply schema changes: %w", err)
	}
	return atlasschema.ApplyOutcomeApplied, nil
}
