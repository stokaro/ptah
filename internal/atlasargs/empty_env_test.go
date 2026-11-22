package atlasargs_test

import (
	"os"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasargs"
	"ptah.run/internal/envbinding"
)

// refusingSchemaFlags is the `migrate checkpoint` --schema flag reading the
// variable of its native --schemas, declared to refuse a value of either
// variable that names no schema.
func refusingSchemaFlags() []atlasargs.Flag {
	flags := nativeEnvSchemaFlags()
	flags[0].EmptyEnv = envbinding.EmptyRule{Omitted: "select every schema", List: true}
	return flags
}

// environment is the state of the variables one row runs under: a key is set
// to its value, and a variable the map does not name is unset.
type environment map[string]string

// applyEnvironment unsets both schema variables and sets what env names.
func applyEnvironment(c *qt.C, env environment) {
	for _, name := range []string{"PTAH_SCHEMA", "PTAH_SCHEMAS"} {
		c.Setenv(name, "")
		c.Assert(os.Unsetenv(name), qt.IsNil)
	}
	for name, value := range env {
		c.Setenv(name, value)
	}
}

func TestMap_EmptyEnv_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		env     environment
		wantErr string
	}{
		{
			name:    "the flag's own variable",
			env:     environment{"PTAH_SCHEMA": ""},
			wantErr: `PTAH_SCHEMA is set but empty: give it a value, or unset it and leave --schema out to select every schema`,
		},
		{
			name:    "the native variable",
			env:     environment{"PTAH_SCHEMAS": ""},
			wantErr: `PTAH_SCHEMAS is set but empty: give it a value, or unset it and leave --schemas out to select every schema`,
		},
		{
			name:    "the flag's own variable, before a native value",
			env:     environment{"PTAH_SCHEMA": "", "PTAH_SCHEMAS": "one"},
			wantErr: `PTAH_SCHEMA is set but empty: give it a value, or unset it and leave --schema out to select every schema`,
		},
		{
			name:    "the flag's own variable, a space",
			env:     environment{"PTAH_SCHEMA": " "},
			wantErr: `PTAH_SCHEMA is set to " ", which names nothing: give it a value, or unset it and leave --schema out to select every schema`,
		},
		{
			name:    "the flag's own variable, a comma",
			env:     environment{"PTAH_SCHEMA": ","},
			wantErr: `PTAH_SCHEMA is set to ",", which names nothing: give it a value, or unset it and leave --schema out to select every schema`,
		},
		{
			name:    "the flag's own variable, blank fields",
			env:     environment{"PTAH_SCHEMA": " , "},
			wantErr: `PTAH_SCHEMA is set to " , ", which names nothing: give it a value, or unset it and leave --schema out to select every schema`,
		},
		{
			name:    "the native variable, blank fields",
			env:     environment{"PTAH_SCHEMAS": " , "},
			wantErr: `PTAH_SCHEMAS is set to " , ", which names nothing: give it a value, or unset it and leave --schemas out to select every schema`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			applyEnvironment(c, test.env)

			got, err := atlasargs.Map("migrate", "checkpoint", refusingSchemaFlags(), nil)

			c.Assert(err, qt.ErrorIs, envbinding.ErrEmptyValue)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(got, qt.IsNil)
		})
	}
}

func TestMap_EmptyEnv_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		flags []atlasargs.Flag
		env   environment
		args  []string
		want  []string
	}{
		{
			name:  "the flag's own variable is forwarded",
			flags: refusingSchemaFlags(),
			env:   environment{"PTAH_SCHEMA": "own"},
			want:  []string{"--schemas=mapped(own)"},
		},
		{
			name:  "the native variable is forwarded",
			flags: refusingSchemaFlags(),
			env:   environment{"PTAH_SCHEMAS": "one"},
			want:  []string{"--schemas=mapped(one)"},
		},
		{
			name:  "neither variable forwards nothing",
			flags: refusingSchemaFlags(),
			want:  make([]string, 0),
		},
		{
			name:  "the command line wins without reading an empty variable",
			flags: refusingSchemaFlags(),
			env:   environment{"PTAH_SCHEMA": "", "PTAH_SCHEMAS": ""},
			args:  []string{"--schema", "cli"},
			want:  []string{"--schemas=mapped(cli)"},
		},
		// The control for the refusal: without the declaration an empty
		// variable keeps reading as unset.
		{
			name:  "an undeclared flag reads an empty variable as unset",
			flags: nativeEnvSchemaFlags(),
			env:   environment{"PTAH_SCHEMA": "", "PTAH_SCHEMAS": ""},
			want:  make([]string, 0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			applyEnvironment(c, test.env)

			got, err := atlasargs.Map("migrate", "checkpoint", test.flags, test.args)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
