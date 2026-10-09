// Package secretrotation declares --rotate-secret, the flag a native command
// that plans schema changes takes to give a YDB secret a new value.
//
// A secret's value is never read back, so a plan compares a secret by its
// presence alone and cannot see that the value its declared variable holds
// has changed. The operator says so, on this one invocation, by naming the
// secret, and the plan then writes ALTER SECRET with the value taken from the
// variable when the statement runs. The flag reads no environment variable,
// so a rotation cannot be left on in a shell profile or a CI job and reach
// every later plan.
package secretrotation

import (
	"github.com/spf13/cobra"

	"ptah.run/config"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/internal/cli/internal/cmdflags"
)

// FlagName is the flag's name, without the dashes.
const FlagName = "rotate-secret"

const usage = "Give the YDB secret at this path (dir/name) the value its declared environment variable holds " +
	"when the plan runs, through ALTER SECRET. Repeatable. A secret's value is never read back, so a plan " +
	"rotates one only when asked"

// Register adds the flag to cmd, with no environment binding.
func Register(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.StringArray(FlagName, nil, usage)
	if err := cmdflags.DisableEnvBinding(flags, FlagName); err != nil {
		panic(err)
	}
}

// Requested returns the secret paths the operator asked cmd to rotate, in the
// order given.
func Requested(cmd *cobra.Command) ([]string, error) {
	return cmd.Flags().GetStringArray(FlagName)
}

// Apply adds to opts a rotation request for each secret the operator asked
// cmd to rotate (see [ydbsecret.WithRotations]). The comparison refuses a
// path the declaration does not hold before it plans anything.
func Apply(cmd *cobra.Command, opts *config.CompareOptions) error {
	requested, err := Requested(cmd)
	if err != nil {
		return err
	}
	return ydbsecret.WithRotations(opts, requested)
}
