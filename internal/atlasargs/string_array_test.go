package atlasargs_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasargs"
)

// checkpointSchemaFlags is the repeatable-flag half of `migrate checkpoint`:
// Atlas declares `-s, --schema strings`, Ptah takes one comma-separated
// --schemas.
func checkpointSchemaFlags() []atlasargs.Flag {
	return []atlasargs.Flag{
		atlasargs.NativeStringArray("schema", "s", "Schema names", "schemas"),
		atlasargs.NativeString("dev-url", "", "Dev database URL", "shadow-db"),
	}
}

// TestMap_RepeatableFlagJoinsEveryOccurrence is the test that exists because of
// how this can fail silently. A repeatable Atlas flag forwarded occurrence by
// occurrence lands on a native flag that holds one string, so the last value
// wins and every earlier one disappears without an error — a checkpoint that
// covers one schema when the caller named three.
//
// Each row therefore asserts the WHOLE mapped argument list, not just that the
// last value survived.
func TestMap_RepeatableFlagJoinsEveryOccurrence(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "repeated long form",
			args: []string{"--schema", "public", "--schema", "billing"},
			want: []string{"--schemas=public,billing"},
		},
		{
			name: "repeated inline form",
			args: []string{"--schema=public", "--schema=billing"},
			want: []string{"--schemas=public,billing"},
		},
		{
			name: "shorthand form",
			args: []string{"-s", "public", "-s", "billing"},
			want: []string{"--schemas=public,billing"},
		},
		{
			name: "one comma-separated value passes through whole",
			args: []string{"--schema", "public,billing"},
			want: []string{"--schemas=public,billing"},
		},
		{
			name: "a single occurrence still maps",
			args: []string{"--schema", "public"},
			want: []string{"--schemas=public"},
		},
		{
			name: "absent means absent, not an empty list",
			args: []string{"--dev-url", "sqlite://dev.db"},
			want: []string{"--shadow-db", "sqlite://dev.db"},
		},
		{
			name: "occurrences are collected past other flags",
			args: []string{"--schema", "public", "--dev-url=sqlite://dev.db", "--schema", "billing"},
			want: []string{"--shadow-db=sqlite://dev.db", "--schemas=public,billing"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv("PTAH_SCHEMA", "")
			c.Setenv("PTAH_SCHEMAS", "")
			c.Setenv("PTAH_DEV_URL", "")
			c.Setenv("PTAH_SHADOW_DB", "")

			got, err := atlasargs.Map("migrate", "checkpoint", checkpointSchemaFlags(), test.args)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
