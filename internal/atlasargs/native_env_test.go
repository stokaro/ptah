package atlasargs_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasargs"
)

// nativeEnvSchemaFlags is the `migrate checkpoint` --schema flag reading the
// variable of its native --schemas, through a mapping that marks every value it
// saw.
func nativeEnvSchemaFlags() []atlasargs.Flag {
	flag := atlasargs.NativeStringArray("schema", "s", "Schema names", "schemas")
	flag.NativeEnv = true
	flag.MapValue = func(value string) (string, error) { return "mapped(" + value + ")", nil }
	return []atlasargs.Flag{flag}
}

// TestMap_NativeEnvReadsTheNativeVariable pins that a flag marked NativeEnv
// takes the native flag's variable as a value of its own, mapped, when its own
// variable is empty, and that its own variable and the command line both
// outrank it.
func TestMap_NativeEnvReadsTheNativeVariable(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		schemas string
		args    []string
		want    []string
	}{
		{name: "the native variable alone", schemas: "one", want: []string{"--schemas=mapped(one)"}},
		{name: "the flag's own variable first", schema: "own", schemas: "one", want: []string{"--schemas=mapped(own)"}},
		{name: "the command line first", schemas: "one", args: []string{"--schema", "cli"}, want: []string{"--schemas=mapped(cli)"}},
		{name: "neither variable", want: make([]string, 0)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv("PTAH_SCHEMA", test.schema)
			c.Setenv("PTAH_SCHEMAS", test.schemas)

			got, err := atlasargs.Map("migrate", "checkpoint", nativeEnvSchemaFlags(), test.args)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestMap_NativeVariableLeftToTheTargetByDefault is the control: without the
// mark the adapter does not read the native flag's variable, so it is left to
// the forwarded target, which reads it without the mapping.
func TestMap_NativeVariableLeftToTheTargetByDefault(t *testing.T) {
	c := qt.New(t)
	c.Setenv("PTAH_SCHEMA", "")
	c.Setenv("PTAH_SCHEMAS", "one")

	got, err := atlasargs.Map("migrate", "checkpoint", checkpointSchemaFlags(), nil)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.HasLen, 0)
}
