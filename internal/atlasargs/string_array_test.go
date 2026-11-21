package atlasargs_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasargs"
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

// TestMap_RepeatableFlagStaysBeforeTheSeparator holds the joined value in
// front of a `--` separator, past which the native command would read it as a
// positional argument.
func TestMap_RepeatableFlagStaysBeforeTheSeparator(t *testing.T) {
	c := qt.New(t)
	c.Setenv("PTAH_SCHEMA", "")

	got, err := atlasargs.Map("migrate", "checkpoint", checkpointSchemaFlags(), []string{"--schema", "public", "--", "tag"})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{"--schemas=public", "--", "tag"})
}

// scanURLFlags is a repeatable Atlas flag whose native flag is repeatable too,
// as `security scan -u` forwards to `db vulnerabilities --db-url`.
func scanURLFlags() []atlasargs.Flag {
	url := atlasargs.NativeStringArray("url", "u", "Database URL", "db-url")
	url.ForwardEach = true
	return []atlasargs.Flag{url, atlasargs.String("format", "", "Format")}
}

// TestMap_ForwardEachForwardsEveryOccurrence is the counterpart of the join:
// against a native flag that is itself repeatable, a joined value is one value,
// so two URLs reach the scan as one URL that names neither. Each occurrence is
// forwarded as written, in order, and before a separator.
func TestMap_ForwardEachForwardsEveryOccurrence(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{name: "repeated long form", args: []string{"--url", "sqlite://a.db", "--url", "sqlite://b.db"},
			want: []string{"--db-url=sqlite://a.db", "--db-url=sqlite://b.db"}},
		{name: "repeated inline form", args: []string{"--url=sqlite://a.db", "--url=sqlite://b.db"},
			want: []string{"--db-url=sqlite://a.db", "--db-url=sqlite://b.db"}},
		{name: "shorthand form", args: []string{"-u", "sqlite://a.db", "-u", "sqlite://b.db"},
			want: []string{"--db-url=sqlite://a.db", "--db-url=sqlite://b.db"}},
		{name: "a value holding a comma stays one value", args: []string{"-u", "postgres://h1,h2/app"},
			want: []string{"--db-url=postgres://h1,h2/app"}},
		{name: "an empty value is forwarded for the native flag to judge", args: []string{"-u", "", "-u", "sqlite://b.db"},
			want: []string{"--db-url=", "--db-url=sqlite://b.db"}},
		{name: "occurrences are collected past other flags", args: []string{"-u", "sqlite://a.db", "--format", "x", "-u", "sqlite://b.db"},
			want: []string{"--format", "x", "--db-url=sqlite://a.db", "--db-url=sqlite://b.db"}},
		{name: "occurrences stay before the separator", args: []string{"-u", "sqlite://a.db", "--", "--format", "x"},
			want: []string{"--db-url=sqlite://a.db", "--", "--format", "x"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv("PTAH_URL", "")
			c.Setenv("PTAH_FORMAT", "")

			got, err := atlasargs.Map("security", "scan", scanURLFlags(), test.args)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestMap_RepeatableFlagWithoutValue_FailurePath refuses a repeatable flag
// that ends the arguments with no value, in pflag's words and with the flag
// as the caller spelled it. Forwarded bare, it would be refused by the native
// command under the native name, which this surface does not accept.
func TestMap_RepeatableFlagWithoutValue_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "shorthand", args: []string{"-u"}, want: `flag needs an argument: 'u' in -u`},
		{name: "long form", args: []string{"--format", "x", "--url"}, want: `flag needs an argument: --url`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv("PTAH_URL", "")
			c.Setenv("PTAH_FORMAT", "")

			got, err := atlasargs.Map("security", "scan", scanURLFlags(), test.args)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got, qt.IsNil)
		})
	}
}
