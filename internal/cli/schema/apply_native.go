package schema

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"ptah.run/core/ptaherr"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/safety"
)

// applyNativeSchemaPlan preserves the captured batch through approval and
// execution. Native payloads never enter a SQL editor or a SQL rehearsal.
func applyNativeSchemaPlan(ctx context.Context, cmd *cobra.Command, opts schemaApplyOptions, plan atlasschema.ApplyRuntimePlan, run *applyRun) (atlasschema.ApplyOutcome, error) {
	if opts.edit {
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
	for _, unit := range units {
		if unit.Native == nil {
			fmt.Fprintln(run.human, unit.SQL)
			continue
		}
		native := unit.Native
		fmt.Fprintf(run.human, "Native operation: %s\n  %s: %s (%s)\n", native.Kind, native.Subject, native.Reason, native.Severity)
		if native.Access != "" {
			fmt.Fprintf(run.human, "  Access %s: %s\n", native.Access, native.AccessReason)
		}
	}
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
