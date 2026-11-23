package atlas

import (
	"encoding/csv"
	"fmt"
	"strings"

	"github.com/spf13/pflag"

	"ptah.run/internal/atlasargs"
	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/envbinding"
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
// `""`, `,` or `" "`. Such a value adds no name, so the selection would be
// empty, and an empty selection is the whole realm: `schema apply` would plan
// a DROP for every schema the desired schema leaves out. `--schema "$SCHEMA"`
// with an empty variable, or the quoted `"\"${schema}\""`, is the usual
// source of such a value, so refusing it is deliberate and stricter than the
// community binary. That binary reads the empty value as no selection and
// plans those DROPs too, and reads `""` as a schema with an empty name, which
// selects nothing.
//
// The default surface trims the space around each name here, so it reads
// `--schema "one, two"` as `one` and `two`. That is deliberate. Below this
// layer a schema name keeps its bytes, so the trim is this flag's and no
// other's. The community binary keeps the space: ` two` is a schema of its
// own, and a run naming it describes that schema or nothing. The strict policy
// reads the value as that binary does and keeps the space, so `-s ' a'`
// selects the schema ` a`. A field that is blank beside a name is dropped, not
// refused: on either binary it selects nothing.
//
// The default text is emptied after the wrap. pflag decides whether to print
// `(default [])` from the concrete type of the value, and the wrapper is not
// the string slice it recognizes. Nothing parses the text back: every reset of
// the flag replaces the names through [pflag.SliceValue].
func registerAtlasSchemaFlag(flags *pflag.FlagSet, target *[]string, usage string, policy atlascompatpolicy.Policy) {
	flags.StringSliceVarP(target, atlasSchemaFlagName, atlasSchemaFlagShorthand, nil, usage)
	flag := flags.Lookup(atlasSchemaFlagName)
	slice, _ := flag.Value.(pflag.SliceValue) // a string slice flag's value is one
	flag.Value = atlasSchemaValue{Value: flag.Value, slice: slice, strict: policy.IsStrictCE()}
	flag.DefValue = ""
	if err := cmdflags.RefuseEmptyEnv(flags, atlasSchemaFlagName, atlasEmptySchemaRule); err != nil {
		panic(err) // registered above
	}
}

// schemaValueError is a refusal of a -s/--schema value that is Ptah's own
// rather than the community binary's. pflag puts it after the value and the
// flag; for an `env { schemas }` element, [atlasConfiguredSchemaError] names
// the project file and the env instead, because nobody typed the flag.
type schemaValueError struct {
	reason string
}

// Error returns the reason.
func (e *schemaValueError) Error() string { return e.reason }

// atlasEmptySchemaRule is what PTAH_SCHEMA refuses: a value that names no
// schema once it is split on commas and trimmed, as an empty -s value is
// refused. Read as unset, it would select every schema, and `schema apply`
// would plan a DROP for each one the desired schema leaves out.
var atlasEmptySchemaRule = envbinding.EmptyRule{Omitted: "select every schema", List: true}

// errEmptySchemaValue is the reason a -s/--schema value with no schema name in
// any of its fields is refused.
var errEmptySchemaValue = &schemaValueError{reason: "every field is empty, so the value names no schema"}

// commaSchemaNameError is the refusal of a name that holds a comma on a verb
// that passes its names on as one comma-separated list; see
// [atlasForwardedSchemaFlag].
func commaSchemaNameError(name string) error {
	return &schemaValueError{reason: fmt.Sprintf(
		"schema %q holds a comma, and this command passes its schema names on as one comma-separated list", name,
	)}
}

// atlasSchemaValue is the string slice behind -s/--schema with the refusal of a
// value that names no schema; see [registerAtlasSchemaFlag]. It is a
// [pflag.SliceValue] as the string slice is, because the reset between two runs
// of one command tree empties a slice through Replace and would otherwise set
// the flag's default text as a value.
type atlasSchemaValue struct {
	pflag.Value
	slice pflag.SliceValue
	// strict keeps the white space around a name, as the community binary
	// does.
	strict bool
}

// Set refuses an empty value and a value whose fields are all blank, and
// otherwise adds the value's names without its blank fields: each trimmed on
// the default surface, and as written under the strict policy. The fields are
// read with encoding/csv, the reader the string slice uses, so a value it
// cannot read is left to it and refused in its words.
func (v atlasSchemaValue) Set(raw string) error {
	if raw == "" {
		return errEmptySchemaValue
	}
	fields, err := csv.NewReader(strings.NewReader(raw)).Read()
	if err != nil {
		return v.Value.Set(raw)
	}
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		name := strings.TrimSpace(field)
		if name == "" {
			continue
		}
		if v.strict {
			name = field
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return errEmptySchemaValue
	}
	for _, name := range names {
		if err := v.slice.Append(name); err != nil {
			return err
		}
	}
	return nil
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
	flag.EmptyEnv = atlasEmptySchemaRule
	return flag
}

// atlasDownStopFlag is a `migrate down` flag that names where the rollback
// stops, --to-version or --to-tag, mapped onto the native nativeName. Its
// variable refuses an empty or blank value, as PTAH_TO_VERSION does on
// `migrate apply`: read as unset, it reverts the last applied migration, which
// is not the stop the pipeline meant to set.
func atlasDownStopFlag(name, usage, nativeName string) atlasargs.Flag {
	flag := atlasargs.NativeString(name, "", usage, nativeName)
	flag.EmptyEnv = envbinding.EmptyRule{Omitted: "revert the last applied migration"}
	return flag
}

// atlasRevisionsSchemaFlag is the --revisions-schema of a verb that forwards
// to a native command, mapped onto the native --migrations-schema. Its
// variable refuses an empty value, as on the verbs that read the revision
// table themselves: read as unset, it names the default schema's revision
// table instead of the one the operator meant; see
// [dbcli.RefuseEmptyHistoryEnv].
func atlasRevisionsSchemaFlag(usage string) atlasargs.Flag {
	flag := atlasargs.NativeString("revisions-schema", "", usage, dbcli.MigrationsSchemaFlagName)
	flag.EmptyEnv = envbinding.EmptyRule{Omitted: dbcli.OmittedRevisionsSchema}
	return flag
}

// atlasCheckpointSchemaFlag is the -s/--schema of `migrate checkpoint`. Left
// out, it forwards no --schemas, and the native checkpoint reads what its
// --schemas reads when empty: the connection default schema rather than every
// schema. The refusal of a variable naming no schema says that instead of what
// [atlasEmptySchemaRule] says.
func atlasCheckpointSchemaFlag() atlasargs.Flag {
	flag := atlasForwardedSchemaFlag(atlasargs.NativeStringArray("schema", "s", "Schema names the checkpoint covers", "schemas"))
	flag.EmptyEnv = envbinding.EmptyRule{Omitted: dbcli.ConnectionDefaultSchemas, List: true}
	return flag
}

// forwardAtlasSchemaValue reads one -s/--schema value and returns its names in
// the comma-separated form the native flag reads, a name holding a quote
// written as a quoted field; see [dbcli.JoinSchemas].
func forwardAtlasSchemaValue(value string) (string, error) {
	names, err := readAtlasSchemaList([]string{value}, atlascompatpolicy.Full())
	if err != nil {
		return "", err
	}
	for _, name := range names {
		if strings.Contains(name, ",") {
			return "", commaSchemaNameError(name)
		}
	}
	return dbcli.JoinSchemas(names), nil
}
