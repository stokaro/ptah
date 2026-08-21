package atlas

import (
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/schema"
)

// newAtlasSchemaStatsCommand mirrors the native `schema stats` onto the
// Atlas-compatible surface, under the path Atlas spells it with.
//
// # Why a group with one sub-verb
//
// Atlas spells this `schema stats inspect`, so a script written against that
// surface types three words. Registering the body directly as `schema stats`
// would answer two of them and leave `inspect` looking like a stray argument,
// which is a worse failure than a missing verb: the script runs, the shell
// reports success, and the operator reads a usage error as output.
//
// # What the community binary does here
//
// Measured against the pinned CE oracle (v1.3.0), all three of these print the
// `atlas schema` group help at exit 0, byte-identical to each other:
//
//	atlas schema stats inspect        -> schema group help, exit 0
//	atlas schema stats                -> schema group help, exit 0
//	atlas schema frobnicate-nonsense  -> schema group help, exit 0   NONSENSE CONTROL
//
// The control is what makes the first two readable: an unknown subcommand of
// `schema` is swallowed into the group help rather than refused, so CE carries
// neither spelling. `schema --help` lists no `stats` either.
//
// That fixes the direction of the compatibility question. CE exits 0 here, so
// implementing the verb cannot violate rule (a) -- Ptah never exits 0 where CE
// exits 1 -- and this is a widening onto a spelling CE leaves empty
// (stokaro/ptah#1711).
//
// # One deliberate divergence
//
// Atlas rejects SQLite for this verb at runtime. Ptah does not, because Ptah's
// reader handles SQLite like any other dialect and refusing would copy a
// limitation this implementation does not have. A SQLite database answers with
// the same metric families as any other.
func newAtlasSchemaStatsCommand() *cobra.Command {
	group := &cobra.Command{
		Use:   "stats",
		Short: "Schema statistics",
		Long: `Atlas ` + "`atlas schema stats`" + ` command group.

` + "`inspect`" + ` reads a live database and emits a count of each schema object kind
in the OpenMetrics text format.`,
		SilenceErrors: true,
	}
	inspect := schema.NewSchemaStatsCommand()
	inspect.Use = "inspect"
	inspect.Short = "Count the objects in a live schema and emit them as OpenMetrics"
	group.AddCommand(inspect)
	return group
}
