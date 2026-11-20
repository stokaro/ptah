// Package tablerebuild declares --allow-table-rebuild, the flag a native
// command that plans schema changes takes to rebuild a table where the target
// cannot change it in place.
//
// On YDB a changed primary key, a column type change and a column made NOT
// NULL cannot be made in place; on ClickHouse neither can a changed engine,
// PARTITION BY, PRIMARY KEY or an ORDER BY that MODIFY ORDER BY cannot make. The
// planner refuses them unless the operator asks, on this one invocation, for a
// rebuild: the table created again with its rows kept, which is not atomic and
// loses rows written during it. The flag reads no environment variable, so the
// request cannot be left on in a shell profile or a CI job and reach every later
// plan.
package tablerebuild

import (
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdflags"
)

// FlagName is the flag's name, without the dashes.
const FlagName = "allow-table-rebuild"

const usage = "Plan a change the target cannot make in place as a table rebuild that keeps the rows (on YDB: a primary key " +
	"change, a column type change, SET NOT NULL; on ClickHouse: a change of engine, PARTITION BY, PRIMARY KEY, or an ORDER BY " +
	"MODIFY ORDER BY cannot make). The steps are not atomic, and rows written during them are lost"

// Register adds the flag to cmd, with no environment binding.
func Register(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.Bool(FlagName, false, usage)
	if err := cmdflags.DisableEnvBinding(flags, FlagName); err != nil {
		panic(err)
	}
}

// Requested reports whether the operator asked cmd for rebuilds.
func Requested(cmd *cobra.Command) (bool, error) {
	return cmd.Flags().GetBool(FlagName)
}
