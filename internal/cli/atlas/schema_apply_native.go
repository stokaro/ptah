package atlas

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/migrator"
)

// applyAtlasNativeSchemaPlan keeps the batch selected by the shared planner
// through approval. A SQL display or editor cannot represent this batch.
func applyAtlasNativeSchemaPlan(cmd *cobra.Command, opts atlasSchemaApplyOptions, conn *dbschema.DatabaseConnection, plan atlasschema.ApplyRuntimePlan, mode migrator.MigrationTxMode) (string, error) {
	if err := validateAtlasNativeApplyOptions(opts); err != nil {
		return "", err
	}
	if err := plan.Preflight(cmd.Context()); err != nil {
		return "", err
	}
	units, err := plan.ReportUnits(cmd.Context(), opts.runtime)
	if err != nil {
		return "", err
	}
	if err := displayAtlasNativeApplyPlan(cmd, conn, units, mode); err != nil {
		return "", err
	}
	if opts.dryRun {
		return "", nil
	}
	approved, err := confirmAtlasSchemaApply(cmd, opts, "")
	if err != nil || !approved {
		return "", err
	}
	if err := plan.Execute(cmd.Context()); err != nil {
		return "", fmt.Errorf("apply schema changes: %w", err)
	}
	return "Schema apply completed successfully.", nil
}

func applyAtlasNativePlanFile(cmd *cobra.Command, opts atlasSchemaApplyOptions, conn *dbschema.DatabaseConnection, plan atlasschema.PlanFile, mode migrator.MigrationTxMode, desired *schemamodel.Database) (string, error) {
	if err := validateAtlasNativeApplyOptions(opts); err != nil {
		return "", err
	}
	execution, err := plan.ExecutionPlan(cmd.Context(), conn, opts.runtime)
	if err != nil {
		return "", err
	}
	if err := execution.Preflight(cmd.Context(), conn, mode); err != nil {
		return "", err
	}
	if err := displayAtlasNativeApplyPlan(cmd, conn, plan.UnitReports(), mode); err != nil {
		return "", err
	}
	if opts.dryRun {
		return "", nil
	}
	approved, err := confirmAtlasSchemaApply(cmd, opts, "")
	if err != nil || !approved {
		return "", err
	}
	conn.SchemaWriter().SetDryRun(false)
	if err := execution.Execute(cmd.Context(), conn, mode); err != nil {
		return "", fmt.Errorf("apply schema changes: %w", err)
	}
	if desired != nil {
		if err := atlasschema.VerifyAppliedPlanState(cmd.Context(), conn, desired, plan.Exclude, opts.runtime); err != nil {
			return "", err
		}
	}
	return "Schema apply completed successfully.", nil
}

func validateAtlasNativeApplyOptions(opts atlasSchemaApplyOptions) error {
	switch {
	case opts.edit:
		return fmt.Errorf("%w: a native operation cannot be edited as SQL; edit the desired schema and plan again", ptaherr.ErrUnsupportedFeature)
	case opts.formatOutput:
		return fmt.Errorf("%w: the Atlas SQL report format cannot represent native operations", ptaherr.ErrUnsupportedFeature)
	case strings.TrimSpace(opts.devURL) != "":
		return fmt.Errorf("%w: native operations cannot be rehearsed as SQL on --dev-url", ptaherr.ErrUnsupportedFeature)
	case !opts.skipLint && len(opts.lintPolicy.RuleConfigs) != 0:
		return fmt.Errorf("%w: the atlas.hcl SQL lint policy cannot judge native operations; review the owner assessments and use --skip-lint", ptaherr.ErrUnsupportedFeature)
	default:
		return nil
	}
}

func displayAtlasNativeApplyPlan(cmd *cobra.Command, conn *dbschema.DatabaseConnection, units []atlasschema.ApplyUnitReport, mode migrator.MigrationTxMode) error {
	var statements []string
	for _, unit := range units {
		if unit.Native == nil {
			statements = append(statements, unit.SQL)
		}
	}
	if err := validateAtlasSchemaApplyDiffPolicy(mode, conn.Info(), statements); err != nil {
		return err
	}
	printAtlasSchemaApplyUnits(cmd.OutOrStdout(), units)
	return nil
}

func printAtlasSchemaApplyUnits(out io.Writer, units []atlasschema.ApplyUnitReport) {
	fmt.Fprintln(out, "Planned schema changes:")
	for _, unit := range units {
		if unit.Native == nil {
			fmt.Fprintln(out, unit.SQL)
			continue
		}
		native := unit.Native
		fmt.Fprintf(out, "Native operation: %s\n  %s: %s (%s)\n", native.Kind, native.Subject, native.Reason, native.Severity)
		if native.Access != "" {
			fmt.Fprintf(out, "  Access %s: %s\n", native.Access, native.AccessReason)
		}
	}
}
