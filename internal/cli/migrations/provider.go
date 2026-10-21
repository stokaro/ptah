package migrations

import (
	"encoding/json"
	"errors"

	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/providercommand"
	"ptah.run/internal/providerdiagnostic"
	"ptah.run/migration/migrator"
)

// Provider selection belongs to the native namespace. Compatibility adapters
// construct the shared leaf commands without adding native provider flags.
func providerMigrationCommand(cmd *cobra.Command, short, long string) *cobra.Command {
	return providercommand.Configure(migrationCommand(cmd, short, long), reportProviderFailure)
}

// A setup failure precedes the leaf command's output handling. Preserve its
// JSON result contract and keep human diagnostics off standard output.
func reportProviderFailure(cmd *cobra.Command, err error) error {
	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		var report any
		diagnostic, _ := errors.AsType[*providerdiagnostic.Diagnostic](err)
		if cmd.Name() == "up" {
			dryRun, _ := cmd.Flags().GetBool("dry-run")
			report = struct {
				migrator.RunResult
				ProviderDiagnostic *providerdiagnostic.Diagnostic `json:"provider_diagnostic,omitempty"`
			}{migrator.NewRunResult(migrator.RunEvidence{Direction: migrator.MigrationDirectionUp, Err: err, DryRun: dryRun}), diagnostic}
		} else {
			report = struct {
				Error              string                         `json:"error"`
				ProviderDiagnostic *providerdiagnostic.Diagnostic `json:"provider_diagnostic,omitempty"`
			}{err.Error(), diagnostic}
		}
		encoder := json.NewEncoder(cmd.OutOrStdout())
		encoder.SetIndent("", "  ")
		err = errors.Join(err, encoder.Encode(report))
	}
	return cmdutil.Fail(cmd, err)
}
