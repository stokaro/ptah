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

	"ptah.run/internal/envbinding"
	"ptah.run/internal/envbool"
)

const (
	forwardedEnvAnnotation = "ptah.env.forwarded"
	appliedEnvAnnotation   = "ptah.env.applied"
)

// AddForwardedEnvBinding records an environment variable the command's native
// forwarding target will bind after Atlas arguments are mapped. It does not
// advertise or apply that variable on the adapter flag itself; policy
// preflights use the annotation to account for work reachable only after
// dispatch.
func AddForwardedEnvBinding(flags *pflag.FlagSet, name, envName string) error {
	flag := flags.Lookup(name)
	if flag == nil {
		return fmt.Errorf("flag %q does not exist", name)
	}
	if flag.Annotations == nil {
		flag.Annotations = make(map[string][]string)
	}
	flag.Annotations[forwardedEnvAnnotation] = append(
		flag.Annotations[forwardedEnvAnnotation],
		envName,
	)
	return nil
}

// ForwardedEnvBindings returns environment variables consumed by a flag's
// native forwarding target. The returned slice is detached from pflag's
// annotation storage.
func ForwardedEnvBindings(flag *pflag.Flag) []string {
	if flag == nil {
		return nil
	}
	return slices.Clone(flag.Annotations[forwardedEnvAnnotation])
}

// DisableEnvBinding makes a flag explicit-only even when the command tree has
// Ptah environment binding installed.
func DisableEnvBinding(flags *pflag.FlagSet, name string) error {
	flag := flags.Lookup(name)
	if flag == nil {
		return fmt.Errorf("flag %q does not exist", name)
	}
	envbinding.Disable(flag)
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
		envbinding.MarkInstalled(cmd, prefix)
	}
	for _, child := range cmd.Commands() {
		installEnvValidationRecursive(prefix, child)
	}
}

func envBindingInstalled(cmd *cobra.Command, prefix string) bool {
	installed, ok := envbinding.InstalledPrefix(cmd)
	return ok && installed == prefix
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
		if envbinding.Disabled(flag) {
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
		if flag.Name == "help" || envbinding.Disabled(flag) {
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
		// package that owns it. See [ptah.run/internal/envbool].
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
	markEnvApplied(flag, envName)
	return nil
}

// markEnvApplied records which variable set the flag's value. The name is what
// a diagnostic has to print: an operator told that --limit conflicts with
// something has to find PTAH_LIMIT in their pipeline, and deriving the name
// again from a prefix a refusal was handed is a second answer to a question
// this annotation already holds.
func markEnvApplied(flag *pflag.Flag, envName string) {
	if flag.Annotations == nil {
		flag.Annotations = make(map[string][]string)
	}
	flag.Annotations[appliedEnvAnnotation] = []string{envName}
}

func clearEnvApplied(flag *pflag.Flag) {
	delete(flag.Annotations, appliedEnvAnnotation)
}

func envApplied(flag *pflag.Flag) bool {
	_, ok := appliedEnvName(flag)
	return ok
}

// appliedEnvName returns the environment variable whose value the flag carries.
func appliedEnvName(flag *pflag.Flag) (string, bool) {
	values := flag.Annotations[appliedEnvAnnotation]
	if len(values) == 0 || values[0] == "" {
		return "", false
	}
	return values[0], true
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

// AppliedEnvName returns the environment variable whose value the named flag
// carries, and false when the value came from the command line, from the flag's
// default, or from a flag this set does not have.
//
// A refusal about a flag names the spelling that produced the value. An
// operator who set PTAH_LOCK_TIMEOUT in a CI job and reads a message about
// --lock-timeout goes looking through a command line that never carried it.
func AppliedEnvName(flags *pflag.FlagSet, name string) (string, bool) {
	flag := flags.Lookup(name)
	if flag == nil {
		return "", false
	}
	return appliedEnvName(flag)
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
	return groupConflict(names, typed)
}

// groupConflict renders the refusal for a mutually exclusive group, and nil
// below two conflicting values.
//
// names is the group in declaration order. set is how the operator spelled
// each conflicting value: a flag name where cobra would print one, and a PTAH_*
// variable where the value arrived from the environment. Every refusal about
// such a group is built here so the sentence cannot say one thing for a typed
// pair and another for a pair the environment carried.
func groupConflict(names, set []string) error {
	if len(set) < 2 {
		return nil
	}
	sorted := slices.Clone(set)
	slices.Sort(sorted)
	return fmt.Errorf(
		"if any flags in the group [%s] are set none of the others can be; %v were all set",
		strings.Join(names, " "), sorted,
	)
}

// EnvName returns the environment variable name for a Cobra flag.
func EnvName(prefix, flagName string) string {
	return envbinding.Name(prefix, flagName)
}

// EnvBindingName returns the environment variable bound to flag when generic
// binding is installed with prefix. Explicit-only flags and help have no
// binding.
func EnvBindingName(prefix string, flag *pflag.Flag) (string, bool) {
	return envbinding.Of(prefix, flag)
}

func usageContainsEnv(usage string) bool {
	return strings.Contains(usage, " [env: ")
}

// WithdrawEnvValue restores a flag to its declared default when the value it
// carries arrived from the environment rather than from the command line.
//
// It is the second half of a precedence rule, and it has no use on its own: a
// caller reaches for it having decided that something the operator typed
// settles what a PTAH_* variable would otherwise have decided. After it
// returns, SetOnCommandLine and pflag's Changed both answer false for the
// flag, so a reader asking either question sees a flag nobody set.
//
// pflag keeps a flag's default only as the string it prints in --help, so the
// value is restored by parsing that string back. Set appends for a slice flag
// rather than replacing, which would add the default to what is already there,
// so a slice flag is refused by name instead of being mangled. No group this
// package guards has one.
func WithdrawEnvValue(flags *pflag.FlagSet, name string) error {
	flag := flags.Lookup(name)
	if flag == nil || !envApplied(flag) {
		return nil
	}
	if _, isSlice := flag.Value.(pflag.SliceValue); isSlice {
		return fmt.Errorf("cannot withdraw the environment value of slice flag --%s", name)
	}
	if err := flag.Value.Set(flag.DefValue); err != nil {
		return fmt.Errorf("restore --%s to its default %q: %w", name, flag.DefValue, err)
	}
	flag.Changed = false
	clearEnvApplied(flag)
	return nil
}

// ExclusiveValues enforces a mutually exclusive flag group whose members are
// environment-bound and whose declared defaults each mean "no value".
//
// A member left at its default is outside the conflict, whoever wrote it.
// `--limit 0` and `--to-version ""` are the documented spellings for "apply
// everything" on `ptah migrations up`, so a pipeline that always writes both
// flags and leaves one of its variables empty has asked for one bound; refusing
// it names a flag the operator deliberately left blank.
// [MutuallyExclusiveOnCommandLine] cannot read a default this way, because it
// stands in for cobra's group validation, where `--flag=false` is a spelling
// cobra refuses and the refusal has to stay identical to cobra's.
//
// Where each value came from decides the outcome:
//
//   - more than one member typed on the command line is refused in cobra's
//     wording, exactly as [MutuallyExclusiveOnCommandLine] does;
//   - exactly one member typed withdraws every other member's environment
//     value, so the flag the operator wrote decides the run;
//   - no member typed and more than one carrying a value is refused naming the
//     variables that carry them, because nothing the operator wrote says which
//     one they meant. Leaving that pair to the reader behind the group is how a
//     configuration error surfaces from wherever the two values meet: in that
//     layer's vocabulary rather than in the operator's, and after the work in
//     front of it has already run.
func ExclusiveValues(flags *pflag.FlagSet, names ...string) error {
	typed := make([]string, 0, len(names))
	for _, name := range names {
		if carriesValue(flags, name) && SetOnCommandLine(flags, name) {
			typed = append(typed, name)
		}
	}
	if err := groupConflict(names, typed); err != nil {
		return err
	}
	if len(typed) == 1 {
		for _, name := range names {
			if name == typed[0] || !carriesValue(flags, name) {
				continue
			}
			if err := WithdrawEnvValue(flags, name); err != nil {
				return err
			}
		}
	}
	// Asked again after the withdrawal rather than before it, so the refusal
	// reports what still conflicts once the typed flag has had its say.
	carried := make([]string, 0, len(names))
	for _, name := range names {
		if carriesValue(flags, name) {
			carried = append(carried, valueSource(flags, name))
		}
	}
	return groupConflict(names, carried)
}

// carriesValue reports whether a group member selects anything. A flag sitting
// at its declared default selects nothing: a group [ExclusiveValues] guards
// documents that default as the spelling for "no value", so a value equal to it
// is the operator asking for none.
func carriesValue(flags *pflag.FlagSet, name string) bool {
	flag := flags.Lookup(name)
	return flag != nil && flag.Value.String() != flag.DefValue
}

// valueSource is how the operator spelled the value a flag carries: the
// variable when it arrived from the environment, and the flag itself otherwise.
func valueSource(flags *pflag.FlagSet, name string) string {
	flag := flags.Lookup(name)
	if flag == nil {
		return name
	}
	if envName, ok := appliedEnvName(flag); ok {
		return envName
	}
	return name
}

// ExclusiveOnCommandLine enforces a mutually exclusive flag group whose members
// are environment-bound, where the reader behind the group prefers one member
// over another.
//
// Two steps happen, and doing only the first is how a loud refusal becomes a
// silent wrong answer:
//
//   - more than one member typed on the command line is refused, in cobra's own
//     wording, exactly as [MutuallyExclusiveOnCommandLine] does;
//   - exactly one member typed withdraws every other member's environment
//     value, so the flag the operator wrote is the one that decides the run.
//
// The second half is what [MutuallyExclusiveOnCommandLine] leaves out, and
// leaving it out is not a smaller fix but a different outcome. internal/cli/inference's
// specification source prefers --spec over --release, so with PTAH_SPEC
// exported -- which the inference quick start instructs -- a run of
// `describe --release <ref>` that stopped being refused would load the local
// file, never contact the registry, and exit 0 against a specification nobody
// named (stokaro/ptah#2648 finding 11). Refusing that run is wrong for a
// different reason: the operator typed one flag and the diagnostic names two.
//
// [MutuallyExclusiveOnCommandLine] stays the right call where the environment
// value winning is the safe direction. internal/cli/atlas's --dry-run and
// --auto-approve are the worked example: with PTAH_DRY_RUN exported and
// --auto-approve typed, the run prints the plan and applies nothing, so
// withdrawing the variable there would turn a rehearsal into an apply.
//
// [ExclusiveValues] is the same rule for a group whose declared defaults spell
// "no value", and it refuses a pair the environment alone carries rather than
// leaving it to the reader behind the group.
func ExclusiveOnCommandLine(flags *pflag.FlagSet, names ...string) error {
	if err := MutuallyExclusiveOnCommandLine(flags, names...); err != nil {
		return err
	}
	typed := ""
	for _, name := range names {
		if SetOnCommandLine(flags, name) {
			typed = name
			break
		}
	}
	if typed == "" {
		return nil
	}
	for _, name := range names {
		if name == typed {
			continue
		}
		if err := WithdrawEnvValue(flags, name); err != nil {
			return err
		}
	}
	return nil
}
