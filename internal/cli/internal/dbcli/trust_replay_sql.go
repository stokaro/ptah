package dbcli

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"ptah.run/internal/replaytrust"
)

// TrustReplaySQLFlagName is the native flag that trusts the SQL a command runs
// on its dev database. Its environment binding is [replaytrust.EnvVar].
const TrustReplaySQLFlagName = replaytrust.FlagName

// RegisterTrustReplaySQLFlag registers --trust-replay-sql on a native command
// that runs SQL on a dev database: a migration directory it replays, a
// desired-state SQL file it loads there, or a plan it rehearses. The flag's
// environment binding, PTAH_TRUST_REPLAY_SQL, is installed with every other
// flag's, and parses the value with the strict boolean grammar before the
// command runs.
func RegisterTrustReplaySQLFlag(flags *pflag.FlagSet, target *bool) {
	flags.BoolVar(target, TrustReplaySQLFlagName, false,
		"Run the SQL replayed on the dev database with the dev connection's privileges, "+
			"including routines, DO blocks and roles the dev replay guard refuses by default. "+
			"Ptah still cleans only the dev database it claimed: a role or another server-level object the SQL creates stays on the server")
}

// TrustReplaySQL attaches trusted to cmd's context. Every dev database the
// command resolves through internal/devdocker with that context is then
// recorded for the replay guard, which reads the answer by the URL the dev
// connection opens; see [replaytrust].
//
// A command calls it before its first early return, with the value its flag
// parsed, so the answer reaches every path that resolves a dev database.
func TrustReplaySQL(cmd *cobra.Command, trusted bool) {
	cmd.SetContext(replaytrust.With(cmd.Context(), trusted))
}

// TrustReplaySQLDeclared resolves [replaytrust.EnvVar] for a command that takes
// no flag for it, as a ptah-compat command does, and attaches the answer as
// [TrustReplaySQL] does. A malformed value is returned as an error, so the
// command refuses before it does any work.
func TrustReplaySQLDeclared(cmd *cobra.Command) error {
	trusted, err := replaytrust.Declared()
	if err != nil {
		return err
	}
	TrustReplaySQL(cmd, trusted)
	return nil
}
