// Package cmdflags contains small helpers for Ptah command flag wiring.
package cmdflags

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/internal/envbool"
)

const (
	disableEnvAnnotation   = "ptah.env.disabled"
	installedEnvAnnotation = "ptah.env.installed"
	appliedEnvAnnotation   = "ptah.env.applied"
)

// DisableEnvBinding makes a flag explicit-only even when the command tree has
// Ptah environment binding installed.
func DisableEnvBinding(flags *pflag.FlagSet, name string) error {
	flag := flags.Lookup(name)
	if flag == nil {
		return fmt.Errorf("flag %q does not exist", name)
	}
	if flag.Annotations == nil {
		flag.Annotations = map[string][]string{}
	}
	flag.Annotations[disableEnvAnnotation] = []string{"true"}
	return nil
}

// InstallEnvBinding installs Ptah's environment variable binding on the command
// tree. Environment variables follow PTAH_<FLAG_NAME>, with '-' and '.'
// normalized to '_'. Explicit CLI flags still win over environment values.
func InstallEnvBinding(prefix string, root *cobra.Command) {
	visited := make(map[*pflag.Flag]bool)
	annotateEnvRecursive(prefix, visited, root)
	installEnvValidationRecursive(prefix, root)
}

// InitializeEnv applies environment defaults to one selected command after its
// CLI flags have been parsed. It returns an error before command hooks or work
// run when a non-empty value is invalid for the corresponding flag.
func InitializeEnv(prefix string, cmd *cobra.Command) error {
	cmd.InheritedFlags()
	annotateEnv(prefix, make(map[*pflag.Flag]bool), cmd.Flags())
	return applyEnv(prefix, make(map[*pflag.Flag]bool), cmd.Flags())
}

func installEnvValidationRecursive(prefix string, cmd *cobra.Command) {
	if !envBindingInstalled(cmd, prefix) {
		if cmd.Args != nil || !cmd.HasSubCommands() {
			argsValidator := cmd.Args
			cmd.Args = func(cmd *cobra.Command, args []string) error {
				if !cmd.DisableFlagParsing {
					if err := InitializeEnv(prefix, cmd); err != nil {
						return err
					}
				}
				if argsValidator == nil {
					return nil
				}
				return argsValidator(cmd, args)
			}
		}
		if cmd.Annotations == nil {
			cmd.Annotations = make(map[string]string)
		}
		cmd.Annotations[installedEnvAnnotation] = prefix
	}
	for _, child := range cmd.Commands() {
		installEnvValidationRecursive(prefix, child)
	}
}

func envBindingInstalled(cmd *cobra.Command, prefix string) bool {
	return cmd.Annotations[installedEnvAnnotation] == prefix
}

func annotateEnvRecursive(prefix string, visited map[*pflag.Flag]bool, cmd *cobra.Command) {
	annotateEnv(prefix, visited, cmd.Flags())
	annotateEnv(prefix, visited, cmd.PersistentFlags())
	for _, child := range cmd.Commands() {
		annotateEnvRecursive(prefix, visited, child)
	}
}

func annotateEnv(prefix string, visited map[*pflag.Flag]bool, flags *pflag.FlagSet) {
	flags.VisitAll(func(flag *pflag.Flag) {
		if visited[flag] {
			return
		}
		visited[flag] = true
		if flag.Name == "help" {
			return
		}
		if envBindingDisabled(flag) {
			return
		}

		envName := EnvName(prefix, flag.Name)
		if !usageContainsEnv(flag.Usage) {
			flag.Usage = fmt.Sprintf("%s [env: %s]", flag.Usage, envName)
		}
	})
}

func applyEnv(prefix string, visited map[*pflag.Flag]bool, flags *pflag.FlagSet) error {
	var applyErr error
	flags.VisitAll(func(flag *pflag.Flag) {
		if applyErr != nil || visited[flag] {
			return
		}
		visited[flag] = true
		// A marker left by an earlier execution of a reused command tree does
		// not describe this one. Clearing it here, before anything can return,
		// makes SetOnCommandLine answer for the run in progress.
		clearEnvApplied(flag)
		if flag.Name == "help" || envBindingDisabled(flag) {
			return
		}
		if flag.Changed {
			return
		}
		envName := EnvName(prefix, flag.Name)
		value, ok := os.LookupEnv(envName)
		if !ok {
			return
		}
		// An empty value keeps meaning "unset" for every flag type EXCEPT bool,
		// where stokaro/ptah#1334 makes it a configuration error: for a string or
		// a uint an empty environment value is a plausible way to spell "no
		// value", while `PTAH_DRY_RUN=` is a boolean with nothing in it and there
		// is no reading of it that is not a mistake.
		if value == "" && flag.Value.Type() != "bool" {
			return
		}
		applyErr = setEnvValue(flags, flag, envName, value)
	})
	return applyErr
}

func setEnvValue(flags *pflag.FlagSet, flag *pflag.Flag, envName, value string) error {
	switch flag.Value.Type() {
	case "bool":
		// One grammar and one error shape for every boolean PTAH_* variable,
		// whether it reaches a feature through a flag or is read directly by the
		// package that owns it. See [go.5x5.cz/ptah/internal/envbool].
		if _, err := envbool.Parse(envName, value); err != nil {
			return err
		}
	case "uint", "uint64":
		if _, err := strconv.ParseUint(value, 0, 64); err != nil {
			return fmt.Errorf("invalid unsigned integer value %q for %s", value, envName)
		}
	}
	if err := flags.Set(flag.Name, value); err != nil {
		return fmt.Errorf("invalid value %q for %s: %w", value, envName, err)
	}
	markEnvApplied(flag)
	return nil
}

func envBindingDisabled(flag *pflag.Flag) bool {
	values := flag.Annotations[disableEnvAnnotation]
	return len(values) > 0 && values[0] == "true"
}

func markEnvApplied(flag *pflag.Flag) {
	if flag.Annotations == nil {
		flag.Annotations = map[string][]string{}
	}
	flag.Annotations[appliedEnvAnnotation] = []string{"true"}
}

func clearEnvApplied(flag *pflag.Flag) {
	delete(flag.Annotations, appliedEnvAnnotation)
}

func envApplied(flag *pflag.Flag) bool {
	values := flag.Annotations[appliedEnvAnnotation]
	return len(values) > 0 && values[0] == "true"
}

// SetOnCommandLine reports whether the caller typed the flag on the command
// line.
//
// pflag's Changed bit does not answer that question on a Ptah surface.
// InitializeEnv applies a PTAH_* value through FlagSet.Set, which marks the
// flag Changed exactly as an argv occurrence does, so Changed means "this flag
// carries a value from somewhere". Precedence rules want that broader question
// and should keep asking Changed; a rule about what the operator wrote must ask
// here instead.
func SetOnCommandLine(flags *pflag.FlagSet, name string) bool {
	flag := flags.Lookup(name)
	if flag == nil {
		return false
	}
	return flag.Changed && !envApplied(flag)
}

// MutuallyExclusiveOnCommandLine returns cobra's own flag-group diagnostic when
// more than one of names appeared on the command line, and nil otherwise.
//
// It stands in for Command.MarkFlagsMutuallyExclusive wherever a member of the
// group is environment-bound. cobra's ValidateFlagGroups reads Changed, so an
// exported PTAH_* variable makes such a group refuse a command line that
// carries one flag, with a message naming a second flag the operator cannot
// find in their script. The wording, the declaration-order group list and the
// sorted "were all set" list are copied from cobra so the refusal for a genuine
// pair of typed flags stays byte-identical to the one it replaces.
func MutuallyExclusiveOnCommandLine(flags *pflag.FlagSet, names ...string) error {
	typed := make([]string, 0, len(names))
	for _, name := range names {
		if SetOnCommandLine(flags, name) {
			typed = append(typed, name)
		}
	}
	if len(typed) < 2 {
		return nil
	}
	slices.Sort(typed)
	return fmt.Errorf(
		"if any flags in the group [%s] are set none of the others can be; %v were all set",
		strings.Join(names, " "), typed,
	)
}

// EnvName returns the environment variable name for a Cobra flag.
func EnvName(prefix, flagName string) string {
	name := strings.NewReplacer("-", "_", ".", "_").Replace(flagName)
	return strings.ToUpper(prefix + "_" + name)
}

// EnvBindingName returns the environment variable bound to flag when generic
// binding is installed with prefix. Explicit-only flags and help have no
// binding.
func EnvBindingName(prefix string, flag *pflag.Flag) (string, bool) {
	if flag == nil || flag.Name == "help" || envBindingDisabled(flag) {
		return "", false
	}
	return EnvName(prefix, flag.Name), true
}

func usageContainsEnv(usage string) bool {
	return strings.Contains(usage, " [env: ")
}
