// Package cmdadapter contains small Cobra forwarding helpers for external
// command surfaces that delegate to native Ptah command implementations.
package cmdadapter

import (
	"fmt"
	"strings"

	"github.com/go-extras/cobraflags"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const envPrefix = "PTAH"

// ArgMapper rewrites command arguments before they are forwarded to the target
// command implementation.
type ArgMapper func([]string) ([]string, error)

type helpBehavior int

const (
	adapterHelp helpBehavior = iota
	targetHelp
)

// NewForwardCommand returns a command that forwards its raw arguments to a
// native command factory. It is intended for command paths whose behavior,
// flags, and exit-code contract should stay owned by the delegated command.
func NewForwardCommand(use, short, native string, factory func() *cobra.Command) *cobra.Command {
	return NewForwardCommandWithArgs(use, short, native, factory)
}

// NewForwardCommandWithTargetHelp returns a forwarding command whose --help
// output is delegated to the target command. Use this for canonical command
// paths that should expose the target command's real flag surface.
func NewForwardCommandWithTargetHelp(
	use string,
	short string,
	native string,
	factory func() *cobra.Command,
	prefixArgs ...string,
) *cobra.Command {
	return newForwardCommandWithArgsMapper(use, short, native, factory, nil, targetHelp, prefixArgs...)
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
	return NewForwardCommandWithArgsMapper(use, short, native, factory, nil, prefixArgs...)
}

// NewForwardCommandWithArgsMapper returns a forwarding command that can rewrite
// arguments before prepending fixed arguments.
func NewForwardCommandWithArgsMapper(
	use string,
	short string,
	native string,
	factory func() *cobra.Command,
	mapper ArgMapper,
	prefixArgs ...string,
) *cobra.Command {
	return newForwardCommandWithArgsMapper(use, short, native, factory, mapper, adapterHelp, prefixArgs...)
}

func newForwardCommandWithArgsMapper(
	use string,
	short string,
	native string,
	factory func() *cobra.Command,
	mapper ArgMapper,
	help helpBehavior,
	prefixArgs ...string,
) *cobra.Command {
	return &cobra.Command{
		Use:                use,
		Short:              short,
		Long:               fmt.Sprintf("Forwards to `ptah %s`.", native),
		DisableFlagParsing: true,
		SilenceUsage:       true,
		SilenceErrors:      true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if help == adapterHelp && hasHelpArg(args) {
				return cmd.Help()
			}
			if mapper != nil {
				var err error
				args, err = mapper(args)
				if err != nil {
					return err
				}
			}
			target := factory()
			resetCommandFlags(target)
			initializeForwardedTarget(target)
			defer resetCommandFlags(target)
			parent := target.Parent()
			if parent != nil {
				parent.RemoveCommand(target)
				defer parent.AddCommand(target)
			}
			forwardArgs := make([]string, 0, len(prefixArgs)+len(args))
			forwardArgs = append(forwardArgs, prefixArgs...)
			forwardArgs = append(forwardArgs, args...)
			if help == targetHelp && hasHelpArg(forwardArgs) {
				helpCommand, _, err := target.Find(argsWithoutHelp(forwardArgs))
				if err != nil {
					return err
				}
				return renderTargetHelpWithAdapterUsage(cmd, helpCommand)
			}
			target.SetArgs(forwardArgs)
			target.SetIn(cmd.InOrStdin())
			target.SetOut(cmd.OutOrStdout())
			target.SetErr(cmd.ErrOrStderr())
			defer resetCommandIO(target)
			return target.Execute()
		},
	}
}

func initializeForwardedTarget(target *cobra.Command) {
	cobraflags.PostInitCommands(envPrefix, make(map[*pflag.Flag]bool), target)
	normalizeEnvUsage(target)
}

func normalizeEnvUsage(cmd *cobra.Command) {
	cmd.Flags().VisitAll(normalizeFlagEnvUsage)
	cmd.PersistentFlags().VisitAll(normalizeFlagEnvUsage)
	for _, child := range cmd.Commands() {
		normalizeEnvUsage(child)
	}
}

func normalizeFlagEnvUsage(flag *pflag.Flag) {
	flag.Usage = normalizeUsageEnvSuffix(flag.Usage)
}

func normalizeUsageEnvSuffix(usage string) string {
	const marker = " [env: "
	start := strings.Index(usage, marker)
	if start == -1 {
		return usage
	}
	end := strings.Index(usage[start+len(marker):], "]")
	if end == -1 {
		return usage
	}
	return usage[:start+len(marker)+end+1]
}

func hasHelpArg(args []string) bool {
	for _, arg := range args {
		if arg == "--help" || arg == "-h" {
			return true
		}
	}
	return false
}

func argsWithoutHelp(args []string) []string {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		if arg == "--help" || arg == "-h" {
			continue
		}
		out = append(out, arg)
	}
	return out
}

func renderTargetHelpWithAdapterUsage(adapter *cobra.Command, target *cobra.Command) error {
	originalUsage := target.UsageFunc()
	target.SetUsageFunc(adapterUsageFunc(adapter.CommandPath(), target))
	target.SetIn(adapter.InOrStdin())
	target.SetOut(adapter.OutOrStdout())
	target.SetErr(adapter.ErrOrStderr())
	defer target.SetUsageFunc(originalUsage)
	defer resetCommandIO(target)
	return target.Help()
}

func adapterUsageFunc(adapterPath string, target *cobra.Command) func(*cobra.Command) error {
	useLine := adapterUseLine(adapterPath, target)
	return func(cmd *cobra.Command) error {
		cmd.Println("Usage:")
		cmd.Printf("  %s\n", useLine)
		if cmd.HasAvailableLocalFlags() {
			cmd.Println()
			cmd.Println("Flags:")
			cmd.Print(cmd.LocalFlags().FlagUsages())
		}
		if cmd.HasAvailableInheritedFlags() {
			cmd.Println()
			cmd.Println("Global Flags:")
			cmd.Print(cmd.InheritedFlags().FlagUsages())
		}
		return nil
	}
}

func adapterUseLine(adapterPath string, target *cobra.Command) string {
	useLine := adapterPath
	if suffix := useSuffix(target.Use); suffix != "" {
		useLine += " " + suffix
	}
	if target.HasAvailableFlags() && !strings.Contains(useLine, "[flags]") {
		useLine += " [flags]"
	}
	return useLine
}

func useSuffix(use string) string {
	parts := strings.Fields(use)
	if len(parts) <= 1 {
		return ""
	}
	return strings.Join(parts[1:], " ")
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

func resetCommandIO(cmd *cobra.Command) {
	cmd.SetArgs(nil)
	cmd.SetIn(nil)
	cmd.SetOut(nil)
	cmd.SetErr(nil)
	for _, child := range cmd.Commands() {
		resetCommandIO(child)
	}
}
