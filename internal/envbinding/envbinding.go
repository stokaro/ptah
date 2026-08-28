// Package envbinding holds the one rule that decides which environment
// variable a command-line flag reads, and the two cobra annotations that
// record where the rule has been applied.
//
// It exists because that rule has to be recognized in two places at once.
// cmd/internal/cmdflags INSTALLS the binding: it derives the variable name,
// annotates the flag's usage with `[env: PTAH_X]`, and reads the variable back
// when the flag was not typed on the command line. internal/cmdref REPORTS the
// binding onto the generated command reference. AGENTS.md's rule "Recognition
// that spans two functions belongs to one of them" is the reason both call the
// same functions rather than each keeping a spelling: a reference that
// re-derived `PTAH_` + upper-with-underscores would agree with the installer
// the day it was written and stop agreeing the moment either side learned a
// new character, and the reference would go on printing a variable no flag
// reads.
//
// It cannot become two packages, and the split it resists is by direction
// rather than by caller. [Disable] writes the annotation [Disabled] reads, and
// [MarkInstalled] writes the one [InstalledPrefix] reads; a reader that lived
// apart from its writer would be comparing against a key nothing sets. The
// same pairing is why cmd/internal/cmdflags keeps no copy of either constant.
package envbinding

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	// disabledAnnotation marks a flag as explicit-only: it takes a value from
	// the command line and from nowhere else.
	disabledAnnotation = "ptah.env.disabled"
	// installedAnnotation records the prefix a command tree's binding was
	// installed with, so a reader can ask a command which variables its flags
	// answer to instead of assuming one.
	installedAnnotation = "ptah.env.installed"
)

// Name returns the environment variable a flag of this name binds to under
// prefix. Both separators a Ptah flag name may carry become underscores, so
// `migration.lock-timeout` under `PTAH` is `PTAH_MIGRATION_LOCK_TIMEOUT`.
func Name(prefix, flagName string) string {
	name := strings.NewReplacer("-", "_", ".", "_").Replace(flagName)
	return strings.ToUpper(prefix + "_" + name)
}

// Disable makes a flag explicit-only even on a tree that has binding
// installed. An approval a script could grant by exporting a variable is not
// an approval, which is what every current caller is for.
func Disable(flag *pflag.Flag) {
	if flag.Annotations == nil {
		flag.Annotations = make(map[string][]string)
	}
	flag.Annotations[disabledAnnotation] = []string{"true"}
}

// Disabled reports whether Disable was called on this flag.
func Disabled(flag *pflag.Flag) bool {
	values := flag.Annotations[disabledAnnotation]
	return len(values) > 0 && values[0] == "true"
}

// MarkInstalled records on a command that binding has been installed with
// prefix, so installing twice does not wrap its argument validator twice.
func MarkInstalled(cmd *cobra.Command, prefix string) {
	if cmd.Annotations == nil {
		cmd.Annotations = make(map[string]string)
	}
	cmd.Annotations[installedAnnotation] = prefix
}

// InstalledPrefix returns the prefix this command's binding was installed
// with, and false when no binding was installed on it.
func InstalledPrefix(cmd *cobra.Command) (string, bool) {
	prefix, ok := cmd.Annotations[installedAnnotation]
	return prefix, ok && prefix != ""
}

// Of returns the environment variable bound to flag under prefix, and false
// when the flag reads none. `--help` is cobra's and never bound; an
// explicit-only flag was refused a binding on purpose.
func Of(prefix string, flag *pflag.Flag) (string, bool) {
	if flag == nil || flag.Name == "help" || Disabled(flag) {
		return "", false
	}
	return Name(prefix, flag.Name), true
}
