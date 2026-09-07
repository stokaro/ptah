package atlas

import (
	"strings"

	"github.com/spf13/pflag"
)

// atlasVarValue is the `--var` flag value of the Atlas-compatible surface: a
// repeatable `name=value` input variable for the selected atlas.hcl
// environment.
//
// It exists for ONE reason -- the help line. The conformance `cli-surface` tier
// asserts that ptah-compat's flags render as the pinned Atlas community binary
// v1.3.0 renders them, and pflag names a flag's placeholder from its value's
// Type(). Registered as a plain string array the line read
// `--var stringArray   input variables (default [])`; that binary prints
// (measured 2026-08-08, `atlas schema apply --help`):
//
//	--var <name>=<value>   input variables (default [])
//
// It deliberately does NOT validate. A pflag value whose Set refuses would sit
// on that binary's own step, and was measured and rejected in #1307: the
// SYNTAX of --var is checked by [validateAtlasVarFlagValue], from the tree-wide
// gate [validateAtlasVarFlagsOnCommand] and from [extractAtlasProjectArgs], and
// that check knows things this one could not -- the value is CSV, so
// `--var a=1,b` is refused naming `b`, an empty value fails with `EOF` and an
// unbalanced quote fails with the reader's own parse error. Splitting the rule
// across a Set here and a validator there would be two rules that have to
// agree. This one only renders and collects.
//
// It implements [pflag.SliceValue] because [resetAtlasProjectFlags] clears a
// repeatable flag through that interface and falls back to `Set(DefValue)` for
// a value that does not implement it -- which for this flag would append the
// literal `[]` instead of clearing, so `--var` would leak and grow across
// Execute calls on a reused root.
type atlasVarValue struct {
	values *[]string
}

// newAtlasVarValue binds a --var flag value to the slice that collects it.
func newAtlasVarValue(values *[]string) *atlasVarValue {
	return &atlasVarValue{values: values}
}

// String renders the collected values the way pflag renders a string array, so
// the registered default reads `[]` and the help line carries `(default [])`.
func (v *atlasVarValue) String() string {
	if v.values == nil || len(*v.values) == 0 {
		return "[]"
	}
	return "[" + strings.Join(*v.values, ",") + "]"
}

// Set appends one value verbatim. The syntax rule lives in
// [validateAtlasVarFlagValue]; see the type comment for why it is not here.
func (v *atlasVarValue) Set(raw string) error {
	*v.values = append(*v.values, raw)
	return nil
}

// Type names the value placeholder shown in help output, which is the whole
// point of this type.
func (v *atlasVarValue) Type() string {
	return "<name>=<value>"
}

// GetSlice returns the collected assignments.
func (v *atlasVarValue) GetSlice() []string {
	if v.values == nil {
		return nil
	}
	return *v.values
}

// Replace sets the collected assignments, and is what lets the per-execution
// flag reset clear this flag rather than append to it.
func (v *atlasVarValue) Replace(raw []string) error {
	*v.values = raw
	return nil
}

// Append adds one assignment.
func (v *atlasVarValue) Append(raw string) error {
	return v.Set(raw)
}

var (
	_ pflag.Value      = (*atlasVarValue)(nil)
	_ pflag.SliceValue = (*atlasVarValue)(nil)
)

// atlasVarFlagRawValues reads the values collected for --var off the flag's own
// value.
//
// It cannot go through pflag's GetStringArray: that gates on
// `flag.Value.Type() == "stringArray"` (pflag flag.go, getFlagType), and this
// surface registers --var as [atlasVarValue] so the help line matches the
// pinned binary. A flag registered elsewhere as a plain string array still
// works, through the fallback.
func atlasVarFlagRawValues(flag *pflag.Flag) ([]string, error) {
	if value, ok := flag.Value.(*atlasVarValue); ok {
		return value.GetSlice(), nil
	}
	if value, ok := flag.Value.(pflag.SliceValue); ok {
		return value.GetSlice(), nil
	}
	return nil, nil
}
