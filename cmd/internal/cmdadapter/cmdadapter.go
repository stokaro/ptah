// Package cmdadapter contains small Cobra forwarding helpers for external
// command surfaces that delegate to native Ptah command implementations.
package cmdadapter

import (
	"encoding/csv"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/stokaro/ptah/cmd/internal/cmdflags"
)

const envPrefix = "PTAH"

const defaultSliceAnnotation = "ptah.cmdadapter.default-slice"

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
			prepareExplicitSliceFlagsForParse(target, forwardArgs)
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
	cmdflags.InitializeEnv(envPrefix, target)
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
		resetCommandFlag(flag)
	})
	cmd.PersistentFlags().VisitAll(func(flag *pflag.Flag) {
		resetCommandFlag(flag)
	})
	for _, child := range cmd.Commands() {
		resetCommandFlags(child)
	}
}

func resetCommandFlag(flag *pflag.Flag) {
	if slice, ok := resettableSliceValue(flag); ok {
		_ = slice.Replace(defaultSliceValue(flag, slice))
		if resettable, ok := slice.(*resettableSliceFlagValue); ok {
			resettable.replaceNextSet = true
		}
		flag.Changed = false
		return
	}
	_ = flag.Value.Set(flag.DefValue)
	flag.Changed = false
}

func resettableSliceValue(flag *pflag.Flag) (pflag.SliceValue, bool) {
	value, ok := flag.Value.(sliceFlagValue)
	if !ok {
		return nil, false
	}
	if resettable, ok := flag.Value.(*resettableSliceFlagValue); ok {
		return resettable, true
	}
	resettable := &resettableSliceFlagValue{flag: flag, value: value}
	flag.Value = resettable
	return resettable, true
}

func defaultSliceValue(flag *pflag.Flag, value pflag.SliceValue) []string {
	if values, ok := flag.Annotations[defaultSliceAnnotation]; ok {
		return append([]string(nil), values...)
	}
	values, err := parseSliceDefault(flag.DefValue)
	if err != nil {
		values = value.GetSlice()
	}
	if flag.Annotations == nil {
		flag.Annotations = map[string][]string{}
	}
	flag.Annotations[defaultSliceAnnotation] = append([]string(nil), values...)
	return values
}

func parseSliceDefault(value string) ([]string, error) {
	if !strings.HasPrefix(value, "[") || !strings.HasSuffix(value, "]") {
		return nil, fmt.Errorf("invalid slice default %q", value)
	}
	value = strings.TrimSuffix(strings.TrimPrefix(value, "["), "]")
	if value == "" {
		return []string{}, nil
	}
	return csv.NewReader(strings.NewReader(value)).Read()
}

func prepareExplicitSliceFlagsForParse(cmd *cobra.Command, args []string) {
	flagsByName, flagsByShorthand := commandFlagMaps(cmd)
	for name := range explicitFlagNames(args) {
		flag := flagsByName[name]
		if flag == nil {
			flag = flagsByShorthand[name]
		}
		if flag == nil {
			continue
		}
		resettable, ok := flag.Value.(*resettableSliceFlagValue)
		if ok {
			resettable.replaceNextSet = true
		}
	}
}

func commandFlagMaps(cmd *cobra.Command) (byName map[string]*pflag.Flag, byShorthand map[string]*pflag.Flag) {
	byName = map[string]*pflag.Flag{}
	byShorthand = map[string]*pflag.Flag{}
	visitCommandFlags(cmd, byName, byShorthand)
	return byName, byShorthand
}

func visitCommandFlags(cmd *cobra.Command, byName, byShorthand map[string]*pflag.Flag) {
	cmd.Flags().VisitAll(func(flag *pflag.Flag) {
		byName[flag.Name] = flag
		if flag.Shorthand != "" {
			byShorthand[flag.Shorthand] = flag
		}
	})
	cmd.PersistentFlags().VisitAll(func(flag *pflag.Flag) {
		byName[flag.Name] = flag
		if flag.Shorthand != "" {
			byShorthand[flag.Shorthand] = flag
		}
	})
	for _, child := range cmd.Commands() {
		visitCommandFlags(child, byName, byShorthand)
	}
}

func explicitFlagNames(args []string) map[string]struct{} {
	names := map[string]struct{}{}
	for _, arg := range args {
		if arg == "--" {
			break
		}
		if name, ok := strings.CutPrefix(arg, "--"); ok {
			if name == "" || strings.HasPrefix(name, "-") {
				continue
			}
			name, _, _ = strings.Cut(name, "=")
			names[name] = struct{}{}
			continue
		}
		if strings.HasPrefix(arg, "-") && arg != "-" {
			name := strings.TrimPrefix(arg, "-")
			name, _, _ = strings.Cut(name, "=")
			for _, shorthand := range name {
				names[string(shorthand)] = struct{}{}
			}
		}
	}
	return names
}

type resettableSliceFlagValue struct {
	flag           *pflag.Flag
	value          sliceFlagValue
	replaceNextSet bool
}

type sliceFlagValue interface {
	pflag.Value
	pflag.SliceValue
}

// pflag slice values keep an internal append-mode bit that is separate from
// Flag.Changed, so reset must make the next user-provided value replace defaults.
func (v *resettableSliceFlagValue) Set(value string) error {
	if v.flag.Changed && !v.replaceNextSet {
		return v.value.Set(value)
	}
	v.replaceNextSet = false
	previous := v.value.GetSlice()
	if err := v.value.Replace(nil); err != nil {
		return err
	}
	if err := v.value.Set(value); err != nil {
		_ = v.value.Replace(previous)
		return err
	}
	return nil
}

func (v *resettableSliceFlagValue) Type() string {
	return v.value.Type()
}

func (v *resettableSliceFlagValue) String() string {
	return v.value.String()
}

func (v *resettableSliceFlagValue) Append(value string) error {
	return v.value.Append(value)
}

func (v *resettableSliceFlagValue) Replace(values []string) error {
	return v.value.Replace(values)
}

func (v *resettableSliceFlagValue) GetSlice() []string {
	return v.value.GetSlice()
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
