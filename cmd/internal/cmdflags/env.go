// Package cmdflags contains small helpers for Ptah command flag wiring.
package cmdflags

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const disableEnvAnnotation = "ptah.env.disabled"

var (
	envOnceMu sync.Mutex
	envOnce   = make(map[*cobra.Command]*sync.Once)
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
	once := envOnceFor(root)
	initEnv := func() {
		once.Do(func() {
			InitializeEnv(prefix, root)
		})
	}

	helpFunc := root.HelpFunc()
	root.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		initEnv()
		helpFunc(cmd, args)
	})
	cobra.OnInitialize(initEnv)
}

// InitializeEnv applies environment defaults and help usage annotations to an
// already-built command tree. It is also used by forwarding adapters that
// execute a target command outside the root command's normal initialization.
func InitializeEnv(prefix string, root *cobra.Command) {
	visited := make(map[*pflag.Flag]bool)
	initializeEnvRecursive(prefix, visited, root)
}

func envOnceFor(root *cobra.Command) *sync.Once {
	envOnceMu.Lock()
	defer envOnceMu.Unlock()
	once := envOnce[root]
	if once == nil {
		once = &sync.Once{}
		envOnce[root] = once
	}
	return once
}

func initializeEnvRecursive(prefix string, visited map[*pflag.Flag]bool, cmd *cobra.Command) {
	applyEnv(prefix, visited, cmd.Flags())
	applyEnv(prefix, visited, cmd.PersistentFlags())
	for _, child := range cmd.Commands() {
		initializeEnvRecursive(prefix, visited, child)
	}
}

func applyEnv(prefix string, visited map[*pflag.Flag]bool, flags *pflag.FlagSet) {
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
		if flag.Changed {
			return
		}
		value, ok := os.LookupEnv(envName)
		if !ok || value == "" {
			return
		}
		_ = flags.Set(flag.Name, value)
	})
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
