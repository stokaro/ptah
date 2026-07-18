// Package cmdalias contains small Cobra forwarding helpers for compatibility
// command paths.
package cmdalias

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// NewForwardCommand returns a command that forwards its raw arguments to a
// native command factory. It is intended for compatibility aliases whose
// behavior, flags, and exit-code contract should stay owned by the native
// command.
func NewForwardCommand(use, short, native string, factory func() *cobra.Command) *cobra.Command {
	return NewForwardCommandWithArgs(use, short, native, factory)
}

// NewForwardCommandWithArgs returns a forwarding command that prepends fixed
// arguments before the user-provided arguments.
func NewForwardCommandWithArgs(
	use string,
	short string,
	native string,
	factory func() *cobra.Command,
	prefixArgs ...string,
) *cobra.Command {
	return &cobra.Command{
		Use:                use,
		Short:              short,
		Long:               fmt.Sprintf("Compatibility alias for `ptah %s`.", native),
		DisableFlagParsing: true,
		SilenceUsage:       true,
		SilenceErrors:      true,
		RunE: func(cmd *cobra.Command, args []string) error {
			target := factory()
			resetCommandFlags(target)
			parent := target.Parent()
			if parent != nil {
				parent.RemoveCommand(target)
				defer parent.AddCommand(target)
			}
			forwardArgs := make([]string, 0, len(prefixArgs)+len(args))
			forwardArgs = append(forwardArgs, prefixArgs...)
			forwardArgs = append(forwardArgs, args...)
			target.SetArgs(forwardArgs)
			target.SetIn(cmd.InOrStdin())
			target.SetOut(cmd.OutOrStdout())
			target.SetErr(cmd.ErrOrStderr())
			return target.Execute()
		},
	}
}

func resetCommandFlags(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(flag *pflag.Flag) {
		_ = flag.Value.Set(flag.DefValue)
		flag.Changed = false
	})
	cmd.PersistentFlags().VisitAll(func(flag *pflag.Flag) {
		_ = flag.Value.Set(flag.DefValue)
		flag.Changed = false
	})
	for _, child := range cmd.Commands() {
		resetCommandFlags(child)
	}
}
