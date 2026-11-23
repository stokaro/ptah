// Package envbinding holds the one rule that decides which environment
// variable a command-line flag reads, and the two cobra annotations that
// record where the rule has been applied.
//
// It exists because that rule has to be recognized in two places at once.
// internal/cli/internal/cmdflags INSTALLS the binding: it derives the variable name,
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
// same pairing is why internal/cli/internal/cmdflags keeps no copy of either constant.
package envbinding

import (
	"errors"
	"fmt"
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
	// emptyRefusedAnnotation marks a flag whose variable may not be set to a
	// value that names nothing. It holds the [EmptyRule]: what leaving the flag
	// out does, then listValue when the value is a comma-separated list, then
	// unlessPrefix and a flag name for each of [EmptyRule.Unless].
	emptyRefusedAnnotation = "ptah.env.empty-refused"
	// listValue is the second element of emptyRefusedAnnotation for a list.
	listValue = "list"
	// unlessPrefix marks an element of emptyRefusedAnnotation that names a
	// flag of [EmptyRule.Unless].
	unlessPrefix = "unless="
)

// ErrEmptyValue reports a variable whose value names nothing on a flag that
// refuses one; see [RefuseEmpty].
var ErrEmptyValue = errors.New("names nothing")

// EmptyRule is what a flag's variable refuses: a value that names nothing.
// For a single value that is the empty value and a value of only white space,
// such as spaces or a tab; for a list it is a value with no field left once it
// is split and trimmed.
//
// Every other non-boolean variable reads an empty value as unset. For a flag
// that selects something, unset can select more than any value does, so an
// empty variable, which is usually a pipeline variable nobody filled, would
// widen the run without a word.
type EmptyRule struct {
	// Omitted is what leaving the flag out does, as the clause the refusal
	// prints after "to", such as "select every schema". The zero rule, with no
	// Omitted, refuses nothing.
	Omitted string
	// List reads the value as comma-separated fields, each trimmed of spaces,
	// and refuses a value with no field left: " ", "," and " , " as well as "".
	// Without it the value is read whole, and "" and " " are refused while ","
	// is passed on to the flag.
	List bool
	// Unless names the flags whose value, typed on the command line, decides
	// the run whatever the variable says. While one of them is typed with a
	// value other than its default, a value that names nothing cannot widen
	// the run, and the binding reads it as unset instead of refusing it: an
	// empty PTAH_TARGET beside a typed --limit 1 leaves the stop to --limit. A
	// typed default, such as --limit 0 or --to-tag '', selects what leaving
	// the flag out does, and so does a value of only white space; beside one
	// of those, or with none typed, a stop another variable sets would decide
	// the run in its place, so the value is refused. The binding in
	// internal/cli/internal/cmdflags applies it; [EmptyRule.Check] does not
	// read it.
	Unless []string
}

// Check returns the refusal of value, the value of envName, which --flagName
// reads, or nil when the value names something or the rule is the zero rule.
// The refusal names the variable and the flag, says what leaving both out
// does, and wraps [ErrEmptyValue].
func (r EmptyRule) Check(envName, flagName, value string) error {
	if r.Omitted == "" || !r.namesNothing(value) {
		return nil
	}
	state := "set but empty"
	if value != "" {
		state = fmt.Sprintf("set to %q, which names nothing", value)
	}
	return &emptyValueError{text: fmt.Sprintf("%s is %s: give it a value, or unset it and leave --%s out to %s",
		envName, state, flagName, r.Omitted)}
}

// emptyValueError is the refusal [EmptyRule.Check] returns. Its text names the
// variable rather than repeating the sentinel, and it still matches
// [ErrEmptyValue].
type emptyValueError struct{ text string }

func (e *emptyValueError) Error() string { return e.text }

func (e *emptyValueError) Unwrap() error { return ErrEmptyValue }

func (r EmptyRule) namesNothing(value string) bool {
	if !r.List {
		return strings.TrimSpace(value) == ""
	}
	for field := range strings.SplitSeq(value, ",") {
		if strings.TrimSpace(field) != "" {
			return false
		}
	}
	return true
}

// errNoOmitted refuses a rule that would refuse nothing; see [RefuseEmpty].
var errNoOmitted = errors.New("the rule names nothing that leaving the flag out does, so it would refuse nothing")

// RefuseEmpty declares that the variable flag binds to refuses what rule
// refuses. A rule with no [EmptyRule.Omitted] is refused: [EmptyRule.Check]
// refuses nothing under it, so the declaration would read as in effect while
// the variable's empty value still reads as unset.
func RefuseEmpty(flag *pflag.Flag, rule EmptyRule) error {
	if rule.Omitted == "" {
		return fmt.Errorf("--%s: %w", flag.Name, errNoOmitted)
	}
	if flag.Annotations == nil {
		flag.Annotations = make(map[string][]string)
	}
	values := []string{rule.Omitted}
	if rule.List {
		values = append(values, listValue)
	}
	for _, name := range rule.Unless {
		values = append(values, unlessPrefix+name)
	}
	flag.Annotations[emptyRefusedAnnotation] = values
	return nil
}

// EmptyRefused returns the rule [RefuseEmpty] declared on the flag, and false
// when it declared none.
func EmptyRefused(flag *pflag.Flag) (EmptyRule, bool) {
	if flag == nil {
		return EmptyRule{}, false
	}
	values, refused := flag.Annotations[emptyRefusedAnnotation]
	if !refused || len(values) == 0 {
		return EmptyRule{}, false
	}
	rule := EmptyRule{Omitted: values[0]}
	for _, value := range values[1:] {
		if name, ok := strings.CutPrefix(value, unlessPrefix); ok {
			rule.Unless = append(rule.Unless, name)
			continue
		}
		rule.List = rule.List || value == listValue
	}
	return rule, true
}

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
