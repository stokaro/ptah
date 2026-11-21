package atlasargs_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasargs"
	"ptah.run/internal/envbool/envbooltest"
)

// clearDownEnvironment unsets the variables the migrate down flags read, so
// the run's environment cannot add an argument.
func clearDownEnvironment(c *qt.C) {
	for _, name := range []string{"PTAH_URL", "PTAH_DIR", "PTAH_TO_VERSION", "PTAH_REVISIONS_SCHEMA", "PTAH_LOCK_TIMEOUT"} {
		c.Setenv(name, "")
	}
}

// TestMap_HappyPathShorthandValueForms maps every form pflag reads for a
// shorthand that takes a value to the native flag. `-u=` gives -u the value
// "=", as pflag gives it, and the native command decides what that means.
func TestMap_HappyPathShorthandValueForms(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{name: "separate", args: []string{"-u", "sqlite://a.db"}, want: []string{"--db-url", "sqlite://a.db"}},
		{name: "after =", args: []string{"-u=sqlite://a.db"}, want: []string{"--db-url=sqlite://a.db"}},
		{name: "attached", args: []string{"-usqlite://a.db"}, want: []string{"--db-url=sqlite://a.db"}},
		{name: "= alone", args: []string{"-u="}, want: []string{"--db-url=="}},
		{name: "attached before a long flag", args: []string{"-usqlite://a.db", "--to-version", "2"},
			want: []string{"--db-url=sqlite://a.db", "--target", "2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			clearDownEnvironment(c)

			got, err := atlasargs.Map("migrate", "down", migrateDownFlags(), tt.args)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

// TestMap_HappyPathShorthandGroup maps a group of shorthands to the native
// flag of each shorthand in it, as pflag reads the group: forwarded whole, the
// native command reads the letters as its own shorthands, which it may not
// declare.
func TestMap_HappyPathShorthandGroup(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{name: "two booleans", args: []string{"-ls"}, want: []string{"--latest", "--short"}},
		{name: "a boolean and an attached value", args: []string{"-lusqlite://a.db"},
			want: []string{"--latest", "--url=sqlite://a.db"}},
		{name: "a group as a value", args: []string{"--url", "-ls"}, want: []string{"--url", "-ls"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			for _, name := range []string{"PTAH_URL", "PTAH_LATEST", "PTAH_SHORT", "PTAH_FORMAT", "PTAH_DRY_RUN"} {
				envbooltest.Unset(name)(t)
			}

			got, err := atlasargs.Map("migrate", "ls", presenceFlags(), tt.args)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

// TestMap_FailurePathFlagWithoutValue refuses every flag that takes a value
// and ends the arguments without one, in pflag's words and spelled as the
// caller typed it. Forwarded bare, the flag would reach the native command,
// which would name its own flag, --db-url for -u. A flag this surface does not
// implement is refused under its own name first.
func TestMap_FailurePathFlagWithoutValue(t *testing.T) {
	tests := []struct {
		name  string
		flags []atlasargs.Flag
		args  []string
		want  string
	}{
		{name: "a shorthand", flags: migrateDownFlags(), args: []string{"-u"}, want: `flag needs an argument: 'u' in -u`},
		{name: "a string flag", flags: migrateDownFlags(), args: []string{"-u", "sqlite://a.db", "--to-version"},
			want: `flag needs an argument: --to-version`},
		{name: "a directory flag", flags: migrateDownFlags(), args: []string{"--dir"}, want: `flag needs an argument: --dir`},
		{name: "an unsupported string flag", flags: migrateDownFlags(), args: []string{"--dev-url"},
			want: `atlas migrate down accepts --dev-url, but Ptah does not implement its behavior yet`},
		{name: "a flag whose default the run adds", flags: migrateHashFlags(), args: []string{"--dir-format"},
			want: `flag needs an argument: --dir-format`},
		{name: "a flag before a default", flags: migrateHashFlags(), args: []string{"--dir"}, want: `flag needs an argument: --dir`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			clearDownEnvironment(c)
			c.Setenv("PTAH_DIR_FORMAT", "")

			got, err := atlasargs.Map("migrate", "down", tt.flags, tt.args)

			c.Assert(err, qt.ErrorMatches, tt.want)
			c.Assert(got, qt.IsNil)
		})
	}
}

// TestMap_HappyPathSynthesizedArgumentsGoFirst puts the arguments the
// environment and the defaults add before the caller's. A flag the caller
// wrote last keeps ending the arguments, and nothing lands after `--`, where
// the native command would read it as a positional.
func TestMap_HappyPathSynthesizedArgumentsGoFirst(t *testing.T) {
	tests := []struct {
		name string
		env  string
		args []string
		want []string
	}{
		{name: "a default before the separator", args: []string{"--dir=file://m", "--", "x"},
			want: []string{"--dir-format=atlas", "--dir=m", "--", "x"}},
		{name: "an environment value before the separator", env: "ptah", args: []string{"--", "x"},
			want: []string{"--dir-format=ptah", "--", "x"}},
		{name: "a flag after the separator is not the caller's", args: []string{"--", "--dir-format", "ptah"},
			want: []string{"--dir-format=atlas", "--", "--dir-format", "ptah"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv("PTAH_DIR", "")
			c.Setenv("PTAH_DIR_FORMAT", tt.env)

			got, err := atlasargs.Map("migrate", "hash", migrateHashFlags(), tt.args)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}
