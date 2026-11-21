package cmdflags

import (
	"encoding/csv"
	"io"
	"strings"

	"github.com/spf13/pflag"
)

// ArgChecker is a flag value that can refuse an argument without storing it.
// [ParseOnly] calls CheckArg where pflag would call Set, so a value whose
// refusal lives outside Set can still be checked by a parse that must change
// nothing.
type ArgChecker interface {
	CheckArg(arg string) error
}

// ParseOnly returns a flag set that reads arguments the way the flags in sets
// read them, and writes to none of them.
//
// Each flag of the returned set copies its original's name, shorthand, default
// and NoOptDefVal. pflag decides from those alone how a shorthand group splits,
// whether a flag takes the next argument as its value and where `--` ends the
// flags, so the copy reads an argument list exactly as the originals would. A
// name declared in more than one set keeps its first declaration, which is how
// cobra merges a parent's persistent flags beneath a command's own.
//
// A copied value stores its argument in its own field, so
// [pflag.FlagSet.GetBool] and the Changed bit report what the arguments asked
// for. It refuses what its original refuses when the original is an
// [ArgChecker], when it is one of pflag's scalar values (refused with the
// standard library's error that [InstallStdlibFlagValues] gives it), when it
// is already such a replacement, and when it is pflag's string slice, which
// reads its argument as one line of comma-separated fields. Any other value
// accepts every argument,
// because its own Set is the only statement of what it refuses and calling it
// would change the flag.
//
// The returned set reports errors and writes nothing: its output is
// [io.Discard], which is where the usage pflag prints for a help request on a
// set that declares no help flag goes.
func ParseOnly(sets ...*pflag.FlagSet) *pflag.FlagSet {
	copied := pflag.NewFlagSet("parse-only", pflag.ContinueOnError)
	copied.SetOutput(io.Discard)
	for _, set := range sets {
		set.VisitAll(func(flag *pflag.Flag) {
			if copied.Lookup(flag.Name) != nil {
				return
			}
			copied.AddFlag(&pflag.Flag{
				Name:        flag.Name,
				Shorthand:   flag.Shorthand,
				Usage:       flag.Usage,
				DefValue:    flag.DefValue,
				NoOptDefVal: flag.NoOptDefVal,
				Value: &parseOnlyValue{
					typ:   flag.Value.Type(),
					value: flag.DefValue,
					check: parseOnlyCheck(flag.Value),
				},
			})
		})
	}
	return copied
}

// parseOnlyCheck returns the refusal a copy of value applies, or nil when it
// accepts every argument.
func parseOnlyCheck(value pflag.Value) func(string) error {
	if checker, ok := value.(ArgChecker); ok {
		return checker.CheckArg
	}
	if replaced, ok := value.(*stdlibValue); ok {
		return replaced.parse
	}
	if declaredByPflag(value) {
		if value.Type() == "stringSlice" {
			return readsAsCSV
		}
		return stdlibParsers[value.Type()]
	}
	return nil
}

// readsAsCSV refuses what pflag's string slice refuses: an argument that is not
// one line of comma-separated fields. pflag reads the argument this way and
// returns the reader's error unchanged.
func readsAsCSV(arg string) error {
	if arg == "" {
		return nil
	}
	_, err := csv.NewReader(strings.NewReader(arg)).Read()
	return err
}

// parseOnlyValue is the value of one flag in a [ParseOnly] set.
type parseOnlyValue struct {
	typ   string
	value string
	check func(string) error
}

func (v *parseOnlyValue) String() string { return v.value }

func (v *parseOnlyValue) Type() string { return v.typ }

// Set refuses arg with the original's refusal, when one is known, and otherwise
// keeps arg as this copy's value.
func (v *parseOnlyValue) Set(arg string) error {
	if v.check != nil {
		if err := v.check(arg); err != nil {
			return err
		}
	}
	v.value = arg
	return nil
}
