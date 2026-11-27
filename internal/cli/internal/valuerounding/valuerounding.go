// Package valuerounding declares --allow-value-rounding, the flag a native
// command that plans schema changes takes to convert a column to a type that
// keeps fewer digits after the point without reading the values its table
// holds, and that `migrations up` and `migrations down` take to execute such a
// conversion a migration file's check would refuse.
//
// A decimal given a lower scale, or a temporal column given a lower
// fractional-second precision, is converted by most servers without an error,
// each value rounded or cut to the new digits. The plan reads the rows first
// and refuses the change while a value would change, and a generated migration
// file carries a check that reads them again where it runs; the flag asks, on
// this one invocation, for the change anyway. It reads no environment
// variable, so the request cannot be left on in a shell profile or a CI job and
// reach every later plan, apply or rollback.
// The same explicit request permits removing a time zone or offset. Without
// it, a non-NULL zoned value is refused even when its fractional digits fit.
package valuerounding

import (
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdflags"
)

// FlagName is the flag's name, without the dashes.
const FlagName = "allow-value-rounding"

// Request is how the flag is written, as a refusal names it.
const Request = "--" + FlagName

const usage = "Allow a column type change to round stored values or remove their time zone or offset without scanning " +
	"the rows: the server keeps fewer digits or discards the zone information"

const applyUsage = "Skip pre-migration checks of kind rounded-values and time-zone-loss, allowing the migration to round " +
	"stored values or discard their time zone or offset"

// Register adds the flag to cmd, a command that plans schema changes, with no
// environment binding.
func Register(cmd *cobra.Command) {
	register(cmd, usage)
}

// RegisterApply adds the flag to cmd, a command that applies migration files,
// with no environment binding, and stores what it asks for in target.
func RegisterApply(cmd *cobra.Command, target *bool) {
	cmd.Flags().BoolVar(target, FlagName, false, applyUsage)
	disableEnvBinding(cmd)
}

func register(cmd *cobra.Command, usage string) {
	cmd.Flags().Bool(FlagName, false, usage)
	disableEnvBinding(cmd)
}

// disableEnvBinding keeps the flag of cmd from reading a variable, so a request
// left in a shell profile or a CI job cannot reach every later run.
func disableEnvBinding(cmd *cobra.Command) {
	if err := cmdflags.DisableEnvBinding(cmd.Flags(), FlagName); err != nil {
		panic(err)
	}
}

// Requested reports whether the operator asked cmd for the change without the
// scan. A command that does not take the flag never asks.
func Requested(cmd *cobra.Command) (bool, error) {
	if cmd.Flags().Lookup(FlagName) == nil {
		return false, nil
	}
	return cmd.Flags().GetBool(FlagName)
}
