package atlas

import (
	"encoding/csv"
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/pflag"

	"ptah.run/internal/atlasargs"
	"ptah.run/internal/schemascope"
)

const (
	atlasFileFlagName        = "file"
	atlasFileFlagShorthand   = "f"
	atlasFromFlagName        = "from"
	atlasFromFlagShorthand   = "f"
	atlasSchemaFlagName      = "schema"
	atlasSchemaFlagShorthand = "s"
)

// registerAtlasSchemaFlag declares -s/--schema as a string slice, the type the
// pinned community binary declares: each value is one line of comma-separated
// fields, so `"a,b"` names one schema, `"one"` names one, and `a"b` is refused
// while the flags are parsed. The list a project file sets goes through the same
// grammar in [effectiveAtlasSchemas], and the packages below this one split a
// name no further.
//
// A value that names no schema is refused too: the empty value, as in
// `--schema=`, and a value whose every field is empty once trimmed, such as
// `""`, `,` or `" "`. Below this layer [schemascope.UniqueNames] drops a blank
// name, so the selection would be empty, and an empty selection is the whole
// realm: `schema apply` would plan a DROP for every schema the desired schema
// leaves out. `--schema "$SCHEMA"` with an empty variable, or the quoted
// `"\"${schema}\""`, is the usual source of such a value, so refusing it is
// deliberate and stricter than the community binary. That binary reads the
// empty value as no selection and plans those DROPs too, and reads `""` as a
// schema with an empty name, which selects nothing.
//
// The default text is emptied after the wrap. pflag decides whether to print
// `(default [])` from the concrete type of the value, and the wrapper is not
// the string slice it recognizes. Nothing parses the text back: every reset of
// the flag replaces the names through [pflag.SliceValue].
func registerAtlasSchemaFlag(flags *pflag.FlagSet, target *[]string, usage string) {
	flags.StringSliceVarP(target, atlasSchemaFlagName, atlasSchemaFlagShorthand, nil, usage)
	flag := flags.Lookup(atlasSchemaFlagName)
	slice, _ := flag.Value.(pflag.SliceValue) // a string slice flag's value is one
	flag.Value = atlasSchemaValue{Value: flag.Value, slice: slice}
	flag.DefValue = ""
}

// errEmptySchemaValue is the reason a -s/--schema value with no schema name in
// any of its fields is refused; pflag puts it after the value and the flag.
var errEmptySchemaValue = errors.New("every field is empty, so the value names no schema")

// atlasSchemaValue is the string slice behind -s/--schema with the refusal of a
// value that names no schema; see [registerAtlasSchemaFlag]. It is a
// [pflag.SliceValue] as the string slice is, because the reset between two runs
// of one command tree empties a slice through Replace and would otherwise set
// the flag's default text as a value.
type atlasSchemaValue struct {
	pflag.Value
	slice pflag.SliceValue
}

// Set refuses an empty value and a value whose fields are all blank, and
// otherwise stores it as the string slice does. The fields are read with
// encoding/csv, the reader the string slice uses, so a value it cannot read is
// left to it and refused in its words.
func (v atlasSchemaValue) Set(raw string) error {
	if raw == "" {
		return errEmptySchemaValue
	}
	fields, err := csv.NewReader(strings.NewReader(raw)).Read()
	if err == nil && len(schemascope.UniqueNames(fields)) == 0 {
		return errEmptySchemaValue
	}
	return v.Value.Set(raw)
}

// Append adds one schema name as the string slice does.
func (v atlasSchemaValue) Append(value string) error { return v.slice.Append(value) }

// Replace replaces the names as the string slice does.
func (v atlasSchemaValue) Replace(values []string) error { return v.slice.Replace(values) }

// GetSlice returns the names as the string slice does.
func (v atlasSchemaValue) GetSlice() []string { return v.slice.GetSlice() }

// atlasForwardedSchemaFlag gives a -s/--schema flag that a verb forwards to a
// native command the grammar [registerAtlasSchemaFlag] declares: each value is
// read as one line of comma-separated fields, so `"one"` names one, `a"b` is
// refused in the same words, and a value that names no schema is refused. The
// native flag takes the names as one comma-separated list, so a name that holds
// a comma cannot reach it and is refused rather than split into two names.
//
// A native flag with another name has its own variable, PTAH_SCHEMAS beside
// PTAH_SCHEMA on `migrate checkpoint`. The flag reads it as a value of its own
// through [atlasargs.Flag.NativeEnv], and [atlasNativeEnvTarget] keeps the
// target from reading it again, so that variable takes the same grammar.
func atlasForwardedSchemaFlag(flag atlasargs.Flag) atlasargs.Flag {
	flag.MapValue = forwardAtlasSchemaValue
	flag.NativeEnv = flag.NativeName != ""
	return flag
}

// forwardAtlasSchemaValue reads one -s/--schema value and returns its names in
// the comma-separated form the native flag reads.
func forwardAtlasSchemaValue(value string) (string, error) {
	names, err := readAtlasSchemaList([]string{value})
	if err != nil {
		return "", err
	}
	for _, name := range names {
		if strings.Contains(name, ",") {
			return "", fmt.Errorf("schema %q holds a comma, and this command passes its schema names on as one comma-separated list", name)
		}
	}
	return strings.Join(names, ","), nil
}
