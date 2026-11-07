package migrations

import (
	"encoding/json"
	"errors"

	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/providercommand"
	"ptah.run/internal/cli/migratedown"
	"ptah.run/internal/cli/migratestatus"
	"ptah.run/internal/cli/migrateup"
	"ptah.run/internal/errorkind"
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
		switch cmd.Name() {
		case "up":
			// Provider setup precedes the run, so nothing was selected or sent.
			dryRun, _ := cmd.Flags().GetBool("dry-run")
			report = struct {
				migrator.RunResult
				ProviderDiagnostic *providerdiagnostic.Diagnostic `json:"provider_diagnostic,omitempty"`
			}{migrator.NewRunResult(migrator.RunEvidence{
				Command:    migrateup.Command,
				Direction:  migrator.MigrationDirectionUp,
				Err:        err,
				DryRun:     dryRun,
				NotStarted: true,
			}), diagnostic}
		case "down":
			// Provider setup precedes the run, so nothing was selected or sent.
			dryRun, _ := cmd.Flags().GetBool("dry-run")
			report = struct {
				migrator.RunResult
				ProviderDiagnostic *providerdiagnostic.Diagnostic `json:"provider_diagnostic,omitempty"`
			}{migrator.NewRunResult(migrator.RunEvidence{
				Command:    migratedown.Command,
				Direction:  migrator.MigrationDirectionDown,
				Err:        err,
				DryRun:     dryRun,
				NotStarted: true,
			}), diagnostic}
		case "status":
			report = struct {
				errorkind.Failure
				ProviderDiagnostic *providerdiagnostic.Diagnostic `json:"provider_diagnostic,omitempty"`
			}{migratestatus.NewFailureDocument(err), diagnostic}
		default:
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
