package atlasargs_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasargs"
)

// TestParseArg_HappyPath reads each flag argument form pflag accepts. An
// attached shorthand value is read only for a shorthand that takes a value,
// and `-x=` gives such a flag the value "=", as pflag gives it.
func TestParseArg_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		arg        string
		takesValue bool
		want       atlasargs.Arg
		spelling   string
	}{
		{name: "a long name", arg: "--url", want: atlasargs.Arg{Name: "url"}, spelling: "--url"},
		{name: "a long name with a value", arg: "--url=sqlite://a.db",
			want: atlasargs.Arg{Name: "url", Value: "sqlite://a.db", HasValue: true}, spelling: "--url"},
		{name: "a long name with an empty value", arg: "--url=",
			want: atlasargs.Arg{Name: "url", HasValue: true}, spelling: "--url"},
		{name: "a long name with = in the value", arg: "--url=a=b",
			want: atlasargs.Arg{Name: "url", Value: "a=b", HasValue: true}, spelling: "--url"},
		{name: "a shorthand", arg: "-u", takesValue: true, want: atlasargs.Arg{Name: "u", Shorthand: true}, spelling: "-u"},
		{name: "a boolean shorthand", arg: "-w", want: atlasargs.Arg{Name: "w", Shorthand: true}, spelling: "-w"},
		{name: "a shorthand with = and a value", arg: "-u=sqlite://a.db", takesValue: true,
			want: atlasargs.Arg{Name: "u", Shorthand: true, Value: "sqlite://a.db", HasValue: true}, spelling: "-u"},
		{name: "a boolean shorthand with = and a value", arg: "-w=false",
			want: atlasargs.Arg{Name: "w", Shorthand: true, Value: "false", HasValue: true}, spelling: "-w"},
		{name: "an attached shorthand value", arg: "-usqlite://a.db", takesValue: true,
			want: atlasargs.Arg{Name: "u", Shorthand: true, Value: "sqlite://a.db", HasValue: true}, spelling: "-u"},
		{name: "a shorthand with = alone", arg: "-u=", takesValue: true,
			want: atlasargs.Arg{Name: "u", Shorthand: true, Value: "=", HasValue: true}, spelling: "-u"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := atlasargs.ParseArg(tt.arg, func(string) bool { return tt.takesValue })

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, tt.want)
			c.Assert(got.Spelling(), qt.Equals, tt.spelling)
		})
	}
}

// TestParseArg_FailurePath reports false for what is no single flag argument:
// a positional, `-`, `--`, a group of boolean shorthands, and `-x=` for a
// boolean x, which pflag reads as x followed by an unknown shorthand `=`.
func TestParseArg_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		arg  string
	}{
		{name: "empty", arg: ""},
		{name: "a positional", arg: "migrations"},
		{name: "a single dash", arg: "-"},
		{name: "the separator", arg: "--"},
		{name: "a group of boolean shorthands", arg: "-wq"},
		{name: "a boolean shorthand with = alone", arg: "-w="},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := atlasargs.ParseArg(tt.arg, func(string) bool { return false })

			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, atlasargs.Arg{})
		})
	}
}

// presenceFlags is a value flag with a shorthand, a value flag without one,
// a boolean, and two booleans with a shorthand.
func presenceFlags() []atlasargs.Flag {
	return []atlasargs.Flag{
		atlasargs.String("url", "u", "the database URL"),
		atlasargs.String("format", "", "the output template"),
		atlasargs.Bool("dry-run", "", "print without applying"),
		atlasargs.Bool("latest", "l", "print only the latest"),
		atlasargs.Bool("short", "s", "print only the version"),
	}
}

// TestFlagPresent_HappyPath finds --url in each form pflag reads, and leaves
// it out where pflag would not read it as --url: after the separator, as the
// value of the flag before it, and as --u, which names no flag.
func TestFlagPresent_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "the long name", args: []string{"--url", "sqlite://a.db"}, want: true},
		{name: "the long name with a value", args: []string{"--url=sqlite://a.db"}, want: true},
		{name: "the shorthand", args: []string{"-u", "sqlite://a.db"}, want: true},
		{name: "the shorthand with = and a value", args: []string{"-u=sqlite://a.db"}, want: true},
		{name: "an attached shorthand value", args: []string{"-usqlite://a.db"}, want: true},
		{name: "after a boolean", args: []string{"--dry-run", "--url", "sqlite://a.db"}, want: true},
		{name: "after a value given with =", args: []string{"--format=x", "--url", "sqlite://a.db"}, want: true},
		{name: "absent", args: []string{"--format", "x"}, want: false},
		{name: "after the separator", args: []string{"--", "--url", "sqlite://a.db"}, want: false},
		{name: "as the value of the flag before it", args: []string{"--format", "--url"}, want: false},
		{name: "as the value of a shorthand before it", args: []string{"-u", "--format"}, want: true},
		{name: "a long spelling of the shorthand", args: []string{"--u", "sqlite://a.db"}, want: false},
		{name: "inside a value", args: []string{"--format", "-usqlite://a.db"}, want: false},
		{name: "attached to a boolean group", args: []string{"-lusqlite://a.db"}, want: true},
		{name: "last in a boolean group", args: []string{"-lu", "sqlite://a.db"}, want: true},
		{name: "a group inside a value", args: []string{"--format", "-lusqlite://a.db"}, want: false},
		{name: "after a group ending in a value flag", args: []string{"-lu", "--url"}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(atlasargs.FlagPresent(presenceFlags(), tt.args, "url"), qt.Equals, tt.want)
		})
	}
}

// TestExpandShorthands_HappyPath splits a group of shorthands as pflag reads
// it: one letter for each boolean, and the rest of the argument for the first
// letter that is not a boolean. An argument that is not a group, and one that
// is the value of the flag before it, stays as it is.
func TestExpandShorthands_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{name: "two booleans", args: []string{"-ls"}, want: []string{"-l", "-s"}},
		{name: "two booleans the other way", args: []string{"-sl"}, want: []string{"-s", "-l"}},
		{name: "a boolean and an attached value", args: []string{"-lusqlite://a.db"}, want: []string{"-l", "-usqlite://a.db"}},
		{name: "a boolean and a value flag",
			args: []string{"-lu", "sqlite://a.db"}, want: []string{"-l", "-u", "sqlite://a.db"}},
		{name: "a group as the value of the group before it", args: []string{"-lu", "-ls"}, want: []string{"-l", "-u", "-ls"}},
		{name: "a group as the value of a long flag", args: []string{"--format", "-ls"}, want: []string{"--format", "-ls"}},
		{name: "a boolean with = and a value", args: []string{"-l=false"}, want: []string{"-l=false"}},
		{name: "a boolean with = alone", args: []string{"-l="}, want: []string{"-l", "-="}},
		{name: "a letter no flag declares after a boolean", args: []string{"-lx"}, want: []string{"-l", "-x"}},
		{name: "a letter no flag declares first", args: []string{"-xl"}, want: []string{"-xl"}},
		{name: "an attached value", args: []string{"-usqlite://a.db"}, want: []string{"-usqlite://a.db"}},
		{name: "a long flag", args: []string{"--latest"}, want: []string{"--latest"}},
		{name: "a positional", args: []string{"migrations"}, want: []string{"migrations"}},
		{name: "after the separator", args: []string{"-l", "--", "-ls"}, want: []string{"-l", "--", "-ls"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(atlasargs.ExpandShorthands(presenceFlags(), tt.args), qt.DeepEquals, tt.want)
		})
	}
}
