package cmdflags

import (
	"reflect"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// stdlibParsers holds the parser behind each scalar value type whose parse
// error pflag v1.1.0 replaces with a sentence of its own, keyed by the value's
// Type(). Each parser is the call pflag's own Set makes, with the same base and
// bit size, so a value that passes it is one the wrapped Set accepts.
var stdlibParsers = map[string]func(string) error{
	"bool": func(s string) error {
		_, err := strconv.ParseBool(s)
		return err
	},
	"duration": func(s string) error {
		_, err := time.ParseDuration(s)
		return err
	},
	"int":     parseInt(0),
	"int8":    parseInt(8),
	"int16":   parseInt(16),
	"int32":   parseInt(32),
	"int64":   parseInt(64),
	"uint":    parseUint(0),
	"uint8":   parseUint(8),
	"uint16":  parseUint(16),
	"uint32":  parseUint(32),
	"uint64":  parseUint(64),
	"float32": parseFloat(32),
	"float64": parseFloat(64),
}

func parseInt(bitSize int) func(string) error {
	return func(s string) error {
		_, err := strconv.ParseInt(s, 0, bitSize)
		return err
	}
}

func parseUint(bitSize int) func(string) error {
	return func(s string) error {
		_, err := strconv.ParseUint(s, 0, bitSize)
		return err
	}
}

func parseFloat(bitSize int) func(string) error {
	return func(s string) error {
		_, err := strconv.ParseFloat(s, bitSize)
		return err
	}
}

// pflagPackage is the import path of the package that declares pflag's own
// value types. Only those are replaced: a value a Ptah package declares keeps
// its own Set and its own wording, even where its Type() reads "bool".
var pflagPackage = reflect.TypeFor[pflag.Flag]().PkgPath()

// InstallStdlibValues gives every flag in the command tree rooted at root the
// value parse errors and the boolean help shape of the pinned Atlas CE binary.
// It visits each command's local and persistent flags and applies
// [InstallStdlibFlagValues] to them.
//
// Call it once the tree is complete. A flag added afterwards keeps pflag's own
// value; cobra adds --help and --version to a command only when it executes
// it, so a help renderer applies [InstallStdlibFlagValues] to the set it is
// about to print.
func InstallStdlibValues(root *cobra.Command) {
	InstallStdlibFlagValues(root.Flags())
	InstallStdlibFlagValues(root.PersistentFlags())
	for _, child := range root.Commands() {
		InstallStdlibValues(child)
	}
}

// InstallStdlibFlagValues replaces the value of each boolean, duration,
// integer, unsigned and float flag pflag declared in flags with one that
// refuses an unparsable argument with the standard library's error and renders
// a boolean flag in help as `--name` alone.
//
// pflag v1.1.0 refuses such an argument with its own sentence, `must be a
// duration like "30s" or "5m"` or `must be true or false`, and prints a boolean
// flag as `--name[=true|false]`. The pinned Atlas CE binary prints `time:
// invalid duration "soon"` and `strconv.ParseBool: parsing "maybe": invalid
// syntax` after the same `invalid argument "..." for "--name" flag:` prefix,
// and `--name` alone. Ptah's diagnostics, help and pages quote that shape on
// both binaries.
//
// The replacement keeps the variable the flag was declared with, its type name
// and its default. It does not report itself as a boolean flag through
// IsBoolFlag, which is what selects the `[=true|false]` suffix; the flag's
// NoOptDefVal, set when it was declared, is what lets `--name` stand without a
// value. A flag already replaced, and a value no pflag constructor made, are
// left as they are, so calling this twice changes nothing.
func InstallStdlibFlagValues(flags *pflag.FlagSet) {
	flags.VisitAll(func(flag *pflag.Flag) {
		if !declaredByPflag(flag.Value) {
			return
		}
		parse, ok := stdlibParsers[flag.Value.Type()]
		if !ok {
			return
		}
		flag.Value = &stdlibValue{Value: flag.Value, parse: parse}
	})
}

// FlagUsages applies [InstallStdlibFlagValues] to flags and returns pflag's
// rendering of them. A help renderer that prints a flag set itself calls it
// rather than [pflag.FlagSet.FlagUsages], so the --help line cobra added to the
// command at execution prints as `-h, --help` too.
func FlagUsages(flags *pflag.FlagSet) string {
	InstallStdlibFlagValues(flags)
	return flags.FlagUsages()
}

func declaredByPflag(value pflag.Value) bool {
	typ := reflect.TypeOf(value)
	if typ == nil {
		return false
	}
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return typ.PkgPath() == pflagPackage
}

// stdlibValue embeds the pflag value it replaces. Embedding the [pflag.Value]
// interface promotes String, Set and Type and nothing else, which is what
// hides the wrapped value's IsBoolFlag from pflag's help renderer.
type stdlibValue struct {
	pflag.Value
	parse func(string) error
}

// Set refuses s with the standard library's error before the wrapped value can
// refuse it with pflag's, and otherwise hands s to the wrapped value.
func (v *stdlibValue) Set(s string) error {
	if err := v.parse(s); err != nil {
		return err
	}
	return v.Value.Set(s)
}
