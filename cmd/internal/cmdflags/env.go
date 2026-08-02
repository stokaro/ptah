// Package cmdflags contains small helpers for Ptah command flag wiring.
package cmdflags

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	disableEnvAnnotation   = "ptah.env.disabled"
	installedEnvAnnotation = "ptah.env.installed"
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
		if flag.Name == "help" || envBindingDisabled(flag) {
			return
		}
		if flag.Changed {
			return
		}
		envName := EnvName(prefix, flag.Name)
		value, ok := os.LookupEnv(envName)
		if !ok || value == "" {
			return
		}
		applyErr = setEnvValue(flags, flag, envName, value)
	})
	return applyErr
}

func setEnvValue(flags *pflag.FlagSet, flag *pflag.Flag, envName, value string) error {
	switch flag.Value.Type() {
	case "bool":
		if _, err := strconv.ParseBool(value); err != nil {
			return fmt.Errorf("invalid boolean value %q for %s", value, envName)
		}
	case "uint", "uint64":
		if _, err := strconv.ParseUint(value, 0, 64); err != nil {
			return fmt.Errorf("invalid unsigned integer value %q for %s", value, envName)
		}
	}
	if err := flags.Set(flag.Name, value); err != nil {
		return fmt.Errorf("invalid value %q for %s: %w", value, envName, err)
	}
	return nil
}

func envBindingDisabled(flag *pflag.Flag) bool {
	values := flag.Annotations[disableEnvAnnotation]
	return len(values) > 0 && values[0] == "true"
}

// EnvName returns the environment variable name for a Cobra flag.
func EnvName(prefix, flagName string) string {
	name := strings.NewReplacer("-", "_", ".", "_").Replace(flagName)
	return strings.ToUpper(prefix + "_" + name)
}

func usageContainsEnv(usage string) bool {
	return strings.Contains(usage, " [env: ")
}
