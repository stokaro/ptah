package envbool_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/envbool"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
)

// probeEnvVar is a name no production code declares, so these rows measure the
// grammar and nothing else.
const probeEnvVar = "PTAH_ENVBOOL_PROBE"

// TestResolveAcceptsEveryParseBoolSpelling pins the accepted grammar, which is
// exactly [strconv.ParseBool]'s and deliberately no narrower.
//
// Narrowing it would be a capability removal: `PTAH_ATLAS_INSPECT_ALL_BLOCKS=t`
// works today and has to keep working, and an operator who learned one spelling
// on one variable must not find it refused on the next.
func TestResolveAcceptsEveryParseBoolSpelling(t *testing.T) {
	tests := []struct {
		name string
		env  func(testing.TB)
		want bool
	}{
		{name: "1", env: envbooltest.Set(probeEnvVar, "1"), want: true},
		{name: "t", env: envbooltest.Set(probeEnvVar, "t"), want: true},
		{name: "T", env: envbooltest.Set(probeEnvVar, "T"), want: true},
		{name: "true", env: envbooltest.Set(probeEnvVar, "true"), want: true},
		{name: "True", env: envbooltest.Set(probeEnvVar, "True"), want: true},
		{name: "TRUE", env: envbooltest.Set(probeEnvVar, "TRUE"), want: true},
		{name: "0", env: envbooltest.Set(probeEnvVar, "0"), want: false},
		{name: "f", env: envbooltest.Set(probeEnvVar, "f"), want: false},
		{name: "F", env: envbooltest.Set(probeEnvVar, "F"), want: false},
		{name: "false", env: envbooltest.Set(probeEnvVar, "false"), want: false},
		{name: "False", env: envbooltest.Set(probeEnvVar, "False"), want: false},
		{name: "FALSE", env: envbooltest.Set(probeEnvVar, "FALSE"), want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.env(t)
			variable := envbool.New(probeEnvVar, false, envbool.Gated)

			got, err := variable.Resolve()

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestResolveDistinguishesAbsentFromEmpty is the state split this whole change
// exists for.
//
// `os.Getenv` answers "" for both, which is how a typo became a default. Absence
// selects the declared default -- both of them, so a future variable defaulting
// to true is covered by the same rows -- while an exported empty value is a
// configuration error.
func TestResolveDistinguishesAbsentFromEmpty(t *testing.T) {
	tests := []struct {
		name        string
		env         func(testing.TB)
		defaultVal  bool
		want        bool
		wantErr     bool
		wantMessage string
	}{
		{
			name:       "absent selects a false default",
			env:        envbooltest.Unset(probeEnvVar),
			defaultVal: false,
			want:       false,
		},
		{
			name:       "absent selects a true default",
			env:        envbooltest.Unset(probeEnvVar),
			defaultVal: true,
			want:       true,
		},
		{
			name:        "an exported empty value is refused",
			env:         envbooltest.Set(probeEnvVar, ""),
			defaultVal:  false,
			wantErr:     true,
			wantMessage: `invalid boolean value "" for PTAH_ENVBOOL_PROBE`,
		},
		{
			name:        "an exported empty value is refused against a true default too",
			env:         envbooltest.Set(probeEnvVar, ""),
			defaultVal:  true,
			wantErr:     true,
			wantMessage: `invalid boolean value "" for PTAH_ENVBOOL_PROBE`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.env(t)
			variable := envbool.New(probeEnvVar, test.defaultVal, envbool.Gated)

			got, err := variable.Resolve()

			c.Assert(err != nil, qt.Equals, test.wantErr)
			c.Assert(got, qt.Equals, test.want)
			c.Assert(errMessage(err), qt.Equals, test.wantMessage)
		})
	}
}

// TestResolveRefusesEveryInvalidValue covers the spellings an operator actually
// types, including the whitespace and quoting mistakes.
//
// Nothing is trimmed on purpose. `PTAH_X=" true"` in a YAML manifest is a
// quoting mistake, and accepting it would hide the mistake on every OTHER
// variable in the same file that the quoting also broke.
func TestResolveRefusesEveryInvalidValue(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		wantMessage string
	}{
		{name: "yes", value: "yes", wantMessage: `invalid boolean value "yes" for PTAH_ENVBOOL_PROBE`},
		{name: "no", value: "no", wantMessage: `invalid boolean value "no" for PTAH_ENVBOOL_PROBE`},
		{name: "on", value: "on", wantMessage: `invalid boolean value "on" for PTAH_ENVBOOL_PROBE`},
		{name: "maybe", value: "maybe", wantMessage: `invalid boolean value "maybe" for PTAH_ENVBOOL_PROBE`},
		{name: "tru", value: "tru", wantMessage: `invalid boolean value "tru" for PTAH_ENVBOOL_PROBE`},
		{name: "2", value: "2", wantMessage: `invalid boolean value "2" for PTAH_ENVBOOL_PROBE`},
		{name: "leading space", value: " true", wantMessage: `invalid boolean value " true" for PTAH_ENVBOOL_PROBE`},
		{name: "trailing space", value: "false ", wantMessage: `invalid boolean value "false " for PTAH_ENVBOOL_PROBE`},
		{name: "quoted", value: `"true"`, wantMessage: `invalid boolean value "\"true\"" for PTAH_ENVBOOL_PROBE`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(probeEnvVar, test.value)
			variable := envbool.New(probeEnvVar, false, envbool.Gated)

			got, err := variable.Resolve()

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, test.wantMessage)
			c.Assert(got, qt.Equals, false)
		})
	}
}

// TestErrorNamesBothTheVariableAndTheRawValue keeps the diagnostic answerable.
//
// The name alone does not tell an operator which of several exported values is
// the bad one when the shell that exported it is not in front of them, and the
// value alone does not tell them where to look. Both halves are asserted
// separately so dropping either one reddens this test on its own.
func TestErrorNamesBothTheVariableAndTheRawValue(t *testing.T) {
	c := qt.New(t)

	_, err := envbool.Parse("PTAH_POSTGRES_INSPECT_ALL_ROLES", "maybe")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "PTAH_POSTGRES_INSPECT_ALL_ROLES")
	c.Assert(err.Error(), qt.Contains, `"maybe"`)
	c.Assert(err.Error(), qt.Equals, `invalid boolean value "maybe" for PTAH_POSTGRES_INSPECT_ALL_ROLES`)
}

// TestNewCarriesTheDeclaredClassification pins the third half of a declaration.
//
// The classification is what strict Atlas Community Edition compatibility
// derives its refusals from, so a Var that answered the wrong class would be a
// variable validated as something it is not. The rows read it straight back off
// the declaration, which is the only place it is ever written.
func TestNewCarriesTheDeclaredClassification(t *testing.T) {
	tests := []struct {
		name  string
		class envbool.Class
		want  string
	}{
		{name: "unclassified", class: envbool.Unclassified, want: "unclassified"},
		{name: "gated", class: envbool.Gated, want: "gated"},
		{name: "retained", class: envbool.Retained, want: "retained"},
		{name: "selector", class: envbool.Selector, want: "selector"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			variable := envbool.New(probeEnvVar, false, test.class)

			c.Assert(variable.Class(), qt.Equals, test.class)
			c.Assert(variable.Class().String(), qt.Equals, test.want)
		})
	}
}

// TestTheZeroClassIsUnclassified pins the fail-closed default.
//
// A declaration that states nothing gets Go's zero value, and strict mode
// refuses an enabled value for it. If the zero value were [envbool.Retained]
// instead, the variable nobody classified would be the one strict mode honored
// in silence -- the exact shape of the defect the classification exists to
// close.
func TestTheZeroClassIsUnclassified(t *testing.T) {
	c := qt.New(t)

	var zero envbool.Class

	c.Assert(zero, qt.Equals, envbool.Unclassified)
	c.Assert(envbool.New(probeEnvVar, false, zero).Class(), qt.Equals, envbool.Unclassified)
}

// errMessage renders an error for comparison against a table row without a
// branch in the test body.
func errMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
