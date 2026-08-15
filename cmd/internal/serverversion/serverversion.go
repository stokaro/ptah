// Package serverversion registers the flag that lets an offline command pin
// the server it is planning for, and identifies that flag again in a built
// command tree.
//
// The identification half is the reason this is a package rather than two
// string literals. Two commands already carry a `--version` flag that has
// nothing to do with a server — `ptah migrations checkpoint` names the
// checkpoint it writes, `ptah schema push` names the artifact tag it publishes
// — so any coverage check that asks "does this command take a version?" by flag
// name answers yes for both and measures nothing. [Lookup] answers from an
// annotation only [Register] and [RegisterAs] set, so a flag counts because a
// command opted into this contract and never because of how it is spelled.
package serverversion

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// FlagName is the native spelling for a command that has no version flag yet.
//
// `ptah sql lint` predates it and keeps `--version`, which is why [RegisterAs]
// exists: the contract is what [Lookup] reads, not the name.
const FlagName = "server-version"

// contractAnnotation marks a flag as carrying the server-version contract.
//
// It is unexported because nothing outside this package should be able to
// claim the contract without going through the registrar that implements it.
const contractAnnotation = "ptah_server_version"

// Usage is the one help string every registration shares. It names the
// requirement (--dialect) and the refusal (a value naming no server), because
// both are behavior an operator meets before any schema is read.
const Usage = "Server version string used to refine target capabilities, for example 17 or " +
	"10.11.6-MariaDB (requires --dialect; a value that names no server is refused)"

// Register registers the flag under [FlagName].
func Register(flags *pflag.FlagSet, target *string) {
	RegisterAs(flags, FlagName, target)
}

// RegisterAs registers the flag under a caller-chosen name, for a command
// whose established spelling is something other than [FlagName].
func RegisterAs(flags *pflag.FlagSet, name string, target *string) {
	flags.StringVar(target, name, "", Usage)
	if err := flags.SetAnnotation(name, contractAnnotation, []string{"true"}); err != nil {
		panic(err)
	}
}

// Lookup returns the flag on cmd that carries the server-version contract, or
// nil when the command has none.
func Lookup(cmd *cobra.Command) *pflag.Flag {
	var found *pflag.Flag
	cmd.Flags().VisitAll(func(flag *pflag.Flag) {
		if found != nil || len(flag.Annotations[contractAnnotation]) == 0 {
			return
		}
		found = flag
	})
	return found
}
