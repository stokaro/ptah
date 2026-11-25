package dbcli

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/replaytrust"
	"ptah.run/migration/shadow"
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

// RegisterShadowTrustReplaySQLFlag registers --trust-replay-sql on a native
// command whose only replay runs on its shadow database, with the help that
// names that database. It binds PTAH_TRUST_REPLAY_SQL as
// [RegisterTrustReplaySQLFlag] does.
func RegisterShadowTrustReplaySQLFlag(flags *pflag.FlagSet, target *bool) {
	flags.BoolVar(target, TrustReplaySQLFlagName, false,
		"Run the SQL replayed on the shadow database with the shadow connection's privileges, "+
			"including routines, DO blocks and roles the replay guard refuses by default. "+
			"Ptah still cleans only the shadow database it claimed: a role or another server-level object the SQL creates stays on the server")
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

// TrustReplaySQLTyped reports whether the operator typed --trust-replay-sql
// on cmd's command line with a true value. A run that replays nothing refuses
// it, as it refuses another flag it would ignore. The variable's binding is not
// typed, so a PTAH_TRUST_REPLAY_SQL a CI job sets for every command stays
// silent.
func TrustReplaySQLTyped(cmd *cobra.Command, trusted bool) bool {
	return trusted && cmdflags.SetOnCommandLine(cmd.Flags(), TrustReplaySQLFlagName)
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

// ResolveShadowDB resolves the shadow database a command replays a migration
// history on -- the --shadow-db of a native command, the --dev-url of compat
// migrate down and of a drift check -- as [devdocker.Resolve] does: it
// provisions a docker:// URL and returns any other URL unchanged, with a
// release the caller must always call.
//
// It is the one construction point for a shadow database, and it never records
// the URL's server as the run's own, whatever PTAH_DEV_SERVER_DISPOSABLE says.
// A shadow database is often a database on the target's own server, and a
// server recorded as the run's own lets the replay cleanup drop the roles the
// target uses. Only trust ([TrustReplaySQL]) and a docker:// shadow lift a
// refusal of the replay guard there.
func ResolveShadowDB(ctx context.Context, rawURL string) (string, func(), error) {
	return devdocker.Resolve(ctx, rawURL, devdocker.Options{})
}

// shadowTrustRemedy ends a shadow replay's refusal that trust would lift. It
// names the variable, which both binaries read, as [replaytrust.Remedy] does
// for a dev database.
const shadowTrustRemedy = "if you trust this SQL to run with the shadow connection's privileges, set " +
	replaytrust.EnvVar + "=1"

// shadowServerRemedy ends a shadow replay's refusal that a server the run
// discards would lift: the one way a command reaches such a server for its
// shadow database, since none declares a shadow's server disposable.
const shadowServerRemedy = "use a " + devdocker.Scheme + ":// or " + devdocker.Scheme +
	"+<driver>:// URL for the shadow database"

// ShadowReplayTrust is the trust a command passes to a replay on its shadow
// database: the answer [TrustReplaySQL] attached to ctx, with the remedies a
// refusal ends with.
func ShadowReplayTrust(ctx context.Context) shadow.ReplayTrust {
	return shadow.ReplayTrust{
		Trusted:      replaytrust.From(ctx),
		Remedy:       shadowTrustRemedy,
		ServerRemedy: shadowServerRemedy,
	}
}
