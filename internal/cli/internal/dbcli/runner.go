package dbcli

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/cli/internal/migrateflags"
)

// RegisterRunnerFlag registers --runner, the tool the migration directory is
// written for ([migrateflags.Runner]). Every command that reads it registers
// it here, so the name, the default and the help cannot drift apart.
func RegisterRunnerFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, migrateflags.RunnerFlag, string(migrateflags.RunnerPtah), migrateflags.RunnerUsage)
}

// EffectiveRunner is the runner value of a command that registered
// [RegisterRunnerFlag]: --runner when the invocation set it, on the command
// line or through its environment variable, and `migration.runner` in
// ptah.yaml otherwise. [migrateflags.ParseRunner] says which runner it names,
// and refuses a value that names none, wherever it came from.
//
// `migrations up`, `migrations down` and `migrations lint` all take the
// runner from here, so a directory that one of them applies the way
// golang-migrate does is rolled back and judged the same way.
func EffectiveRunner(cmd *cobra.Command, flagValue string, projectCfg projectconfig.Config) string {
	return EffectiveString(cmd, migrateflags.RunnerFlag, flagValue, projectCfg.StringValue(projectconfig.StringMigrationRunner))
}

// ResolveRunner is the runner [EffectiveRunner] names, for a command that
// parses it on its own rather than beside its other settings.
func ResolveRunner(cmd *cobra.Command, flagValue string, projectCfg projectconfig.Config) (migrateflags.Runner, error) {
	return migrateflags.ParseRunner(EffectiveRunner(cmd, flagValue, projectCfg))
}
