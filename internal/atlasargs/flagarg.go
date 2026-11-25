package atlasargs

import (
	"fmt"
	"strconv"
	"strings"
)

// Arg is one flag argument as pflag reads it.
type Arg struct {
	// Name is the long name, or the shorthand letter when Shorthand is set.
	Name      string
	Shorthand bool
	// Value is the value the argument itself carries, and HasValue reports
	// whether it carries one. Without one, a flag that takes a value reads
	// the next argument.
	Value    string
	HasValue bool
}

// Spelling returns the flag as the argument names it: --name or -x.
func (a Arg) Spelling() string {
	if a.Shorthand {
		return "-" + a.Name
	}
	return "--" + a.Name
}

// boolValue reads the argument as pflag reads a boolean flag: true without a
// value, else what [strconv.ParseBool] reads, and pflag's refusal of a value
// that reads as no boolean.
func (a Arg) boolValue() (bool, error) {
	if !a.HasValue {
		return true, nil
	}
	value, err := strconv.ParseBool(a.Value)
	if err != nil {
		return false, fmt.Errorf("invalid argument %q for %q flag: %w", a.Value, a.Spelling(), err)
	}
	return value, nil
}

// ParseArg reads arg as pflag reads one flag argument. It is the one reader
// the Atlas-compatible surface recognizes a flag with, so a form the mapper
// forwards is a form every presence check sees, and the reverse.
//
// It reads --name, --name=value, -x and -x=value, and -xvalue when
// takesValue reports that the shorthand x names a flag that takes a value:
// pflag reads the rest of such an argument as the value, so -usqlite://a.db
// is -u with sqlite://a.db. It reports false for anything else: a positional,
// `-`, `--`, and a group of shorthands such as -ab, which [ExpandShorthands]
// splits before a flag is read.
func ParseArg(arg string, takesValue func(shorthand string) bool) (Arg, bool) {
	switch {
	case arg == "--" || len(arg) < 2 || arg[0] != '-':
		return Arg{}, false
	case strings.HasPrefix(arg, "--"):
		name, value, found := strings.Cut(arg[2:], "=")
		return Arg{Name: name, Value: value, HasValue: found}, true
	}
	letter, rest := arg[1:2], arg[2:]
	switch {
	case rest == "":
		return Arg{Name: letter, Shorthand: true}, true
	// pflag reads -x=value as the value for any flag, a boolean included,
	// but -x= as the value "=" for a flag that takes one.
	case len(rest) > 1 && rest[0] == '=':
		return Arg{Name: letter, Shorthand: true, Value: rest[1:], HasValue: true}, true
	case takesValue(letter):
		return Arg{Name: letter, Shorthand: true, Value: rest, HasValue: true}, true
	default:
		return Arg{}, false
	}
}

// ExpandShorthands returns args with each group of shorthands split into the
// arguments pflag reads it as. Each boolean shorthand of flags takes one
// letter, and the first letter that is not one, a shorthand that takes a
// value or a letter flags does not declare, takes the rest of the argument:
// with boolean -l and -s, -ls is -l -s and -lusqlite://a.db is -l
// -usqlite://a.db. -l=value stays one argument, while a boolean -l= is -l and
// -=, which the command refuses as pflag does. A letter flags does not declare
// is left to the command, so -lx reaches it as -l -x.
//
// It reads args as [Map] does: up to `--`, and the argument after a flag that
// takes a value is that value, never a group.
func ExpandShorthands(flags []Flag, args []string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		if args[i] == "--" {
			return append(out, args[i:]...)
		}
		pieces := splitShorthands(flags, args[i])
		out = append(out, pieces...)
		arg, flag, found := readArg(flags, pieces[len(pieces)-1])
		if found && flag.Kind != BoolFlag && !arg.HasValue && i+1 < len(args) {
			i++
			out = append(out, args[i])
		}
	}
	return out
}

// splitShorthands splits one group of shorthands for [ExpandShorthands]. It
// returns any other argument alone: a long flag, whose second dash names no
// shorthand, comes back as it is.
func splitShorthands(flags []Flag, arg string) []string {
	if !strings.HasPrefix(arg, "-") {
		return []string{arg}
	}
	var out []string
	rest := arg[1:]
	for len(rest) > 1 {
		flag, found := findShorthand(flags, rest[:1])
		if !found || flag.Kind != BoolFlag || (rest[1] == '=' && len(rest) > 2) {
			break
		}
		out = append(out, "-"+rest[:1])
		rest = rest[1:]
	}
	return append(out, "-"+rest)
}

// FlagPresent reports whether args name the flag called name, by its long
// name or its shorthand, in any form [ParseArg] reads, a group of shorthands
// included. It reads args as [Map] does: up to `--`, with the argument after
// a flag that takes a value read as that value rather than as a flag.
func FlagPresent(flags []Flag, args []string, name string) bool {
	args = ExpandShorthands(flags, args)
	for i := 0; i < len(args) && args[i] != "--"; i++ {
		arg, flag, found := readArg(flags, args[i])
		if !found {
			continue
		}
		if flag.Name == name {
			return true
		}
		if flag.Kind != BoolFlag && !arg.HasValue {
			i++
		}
	}
	return false
}

// readArg reads arg against flags, and reports the flag it names. A long name
// matches a flag's name and a letter matches its shorthand, as pflag matches
// them, so --u is not -u.
func readArg(flags []Flag, value string) (Arg, Flag, bool) {
	arg, ok := ParseArg(value, func(shorthand string) bool {
		flag, found := findShorthand(flags, shorthand)
		return found && flag.Kind != BoolFlag
	})
	if !ok {
		return Arg{}, Flag{}, false
	}
	if arg.Shorthand {
		flag, found := findShorthand(flags, arg.Name)
		return arg, flag, found
	}
	for _, flag := range flags {
		if flag.Name == arg.Name {
			return arg, flag, true
		}
	}
	return arg, Flag{}, false
}

func findShorthand(flags []Flag, shorthand string) (Flag, bool) {
	for _, flag := range flags {
		if flag.Shorthand != "" && flag.Shorthand == shorthand {
			return flag, true
		}
	}
	return Flag{}, false
}
