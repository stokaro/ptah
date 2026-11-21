package cmdflags_test

import (
	"errors"
	"io"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/pflag"

	"ptah.run/internal/cli/internal/cmdflags"
)

// parseOnlyOriginals declares the flags a ParseOnly copy is made from: one of
// each value kind the copy treats differently.
func parseOnlyOriginals() (local, inherited *pflag.FlagSet) {
	local = pflag.NewFlagSet("local", pflag.ContinueOnError)
	local.StringP("dir", "d", "file://migrations", "")
	local.BoolP("verbose", "v", false, "")
	local.Uint("count", 0, "")
	local.StringSlice("list", nil, "")
	local.Var(&checkedValue{}, "pair", "")
	local.BoolP("help", "h", false, "")
	inherited = pflag.NewFlagSet("inherited", pflag.ContinueOnError)
	inherited.StringP("config", "c", "file://atlas.hcl", "")
	return local, inherited
}

// checkedValue is a flag value whose refusal lives in CheckArg rather than in Set,
// as the Atlas-compatible --var does. Set counts its calls so a test can tell
// that a parse-only read never reached it.
type checkedValue struct {
	sets int
}

var errNoSeparator = errors.New("want name=value")

func (v *checkedValue) String() string { return "" }

func (v *checkedValue) Type() string { return "pair" }

func (v *checkedValue) Set(string) error {
	v.sets++
	return nil
}

func (v *checkedValue) CheckArg(arg string) error {
	if arg == "bad" {
		return errNoSeparator
	}
	return nil
}

// refusingValue refuses every argument in Set and offers no CheckArg, so only
// Set could say what it refuses.
type refusingValue struct {
	sets int
}

func (v *refusingValue) String() string { return "" }

func (v *refusingValue) Type() string { return "custom" }

func (v *refusingValue) Set(string) error {
	v.sets++
	return errors.New("refused")
}

func TestParseOnly_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		wantHelp    bool
		wantDir     string
		wantVerbose bool
		wantArgs    []string
	}{
		{
			name:     "a help flag",
			args:     []string{"-h"},
			wantHelp: true,
			wantDir:  "file://migrations",
			wantArgs: make([]string, 0),
		},
		{
			name:        "a shorthand group splits per boolean",
			args:        []string{"-vh"},
			wantHelp:    true,
			wantDir:     "file://migrations",
			wantVerbose: true,
			wantArgs:    make([]string, 0),
		},
		{
			name:     "a value flag takes the next argument even when it reads as help",
			args:     []string{"--dir", "-h"},
			wantDir:  "-h",
			wantArgs: make([]string, 0),
		},
		{
			name:     "an attached shorthand value",
			args:     []string{"-dfile://x"},
			wantDir:  "file://x",
			wantArgs: make([]string, 0),
		},
		{
			name:     "a help after the terminator is a positional",
			args:     []string{"--", "-h"},
			wantDir:  "file://migrations",
			wantArgs: []string{"-h"},
		},
		{
			name:     "an explicit false",
			args:     []string{"--help=false"},
			wantDir:  "file://migrations",
			wantArgs: make([]string, 0),
		},
		{
			name:     "an inherited flag and a positional",
			args:     []string{"-c", "file://other.hcl", "name", "-h"},
			wantHelp: true,
			wantDir:  "file://migrations",
			wantArgs: []string{"name"},
		},
		{
			name:     "well-formed values of a checked value and a string slice",
			args:     []string{"--pair=good", "--list", "a,b", "-h"},
			wantHelp: true,
			wantDir:  "file://migrations",
			wantArgs: make([]string, 0),
		},
		{
			name:     "an empty string slice",
			args:     []string{"--list=", "-h"},
			wantHelp: true,
			wantDir:  "file://migrations",
			wantArgs: make([]string, 0),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			local, inherited := parseOnlyOriginals()
			copied := cmdflags.ParseOnly(local, inherited)

			err := copied.Parse(tt.args)

			c.Assert(err, qt.IsNil)
			help, err := copied.GetBool("help")
			c.Assert(err, qt.IsNil)
			c.Assert(help, qt.Equals, tt.wantHelp)
			dir, err := copied.GetString("dir")
			c.Assert(err, qt.IsNil)
			c.Assert(dir, qt.Equals, tt.wantDir)
			verbose, err := copied.GetBool("verbose")
			c.Assert(err, qt.IsNil)
			c.Assert(verbose, qt.Equals, tt.wantVerbose)
			c.Assert(copied.Args(), qt.DeepEquals, tt.wantArgs)
			assertUnwritten(c, local, inherited)
		})
	}
}

func TestParseOnly_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "an unknown shorthand after help",
			args:    []string{"-h", "-x"},
			wantErr: `unknown shorthand flag: 'x' in -x`,
		},
		{
			name:    "an unknown shorthand in a group with help",
			args:    []string{"-hx"},
			wantErr: `unknown shorthand flag: 'x' in -x`,
		},
		{
			name:    "an unknown long flag before help",
			args:    []string{"--nonexistent", "--help"},
			wantErr: `unknown flag: --nonexistent`,
		},
		{
			name:    "a value flag with no value",
			args:    []string{"-h", "--dir"},
			wantErr: `flag needs an argument: --dir`,
		},
		{
			name:    "an inherited shorthand with no value",
			args:    []string{"-h", "-c"},
			wantErr: `flag needs an argument: 'c' in -c`,
		},
		{
			name:    "a malformed boolean, in the standard library's words",
			args:    []string{"--help=maybe"},
			wantErr: `invalid argument "maybe" for "-h, --help" flag: strconv.ParseBool: parsing "maybe": invalid syntax`,
		},
		{
			name:    "a malformed unsigned number",
			args:    []string{"-h", "--count=abc"},
			wantErr: `invalid argument "abc" for "--count" flag: strconv.ParseUint: parsing "abc": invalid syntax`,
		},
		{
			name:    "a value its CheckArg refuses",
			args:    []string{"-h", "--pair", "bad"},
			wantErr: `invalid argument "bad" for "--pair" flag: want name=value`,
		},
		{
			name:    "a string slice that is not comma-separated fields",
			args:    []string{"-h", "--list", `a"b`},
			wantErr: `invalid argument "a\"b" for "--list" flag: parse error on line 1, column 2: bare " in non-quoted-field`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			local, inherited := parseOnlyOriginals()
			copied := cmdflags.ParseOnly(local, inherited)

			err := copied.Parse(tt.args)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(tt.wantErr))
			assertUnwritten(c, local, inherited)
		})
	}
}

// TestParseOnly_CopiesAReplacedValuesRefusal covers an original whose value
// InstallStdlibFlagValues has already replaced: the copy keeps that value's
// standard-library refusal rather than accepting everything.
func TestParseOnly_CopiesAReplacedValuesRefusal(t *testing.T) {
	c := qt.New(t)
	local := pflag.NewFlagSet("local", pflag.ContinueOnError)
	local.Bool("dry-run", false, "")
	cmdflags.InstallStdlibFlagValues(local)

	err := cmdflags.ParseOnly(local).Parse([]string{"--dry-run=maybe"})

	c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(
		`invalid argument "maybe" for "--dry-run" flag: strconv.ParseBool: parsing "maybe": invalid syntax`,
	))
	value, err := local.GetBool("dry-run")
	c.Assert(err, qt.IsNil)
	c.Assert(value, qt.IsFalse)
}

// TestParseOnly_NeverCallsTheOriginalsSet covers a value whose refusal only
// its own Set knows: the copy accepts the argument and the original's Set is
// never called, so nothing about the original changes.
func TestParseOnly_NeverCallsTheOriginalsSet(t *testing.T) {
	c := qt.New(t)
	refusing := &refusingValue{}
	checked := &checkedValue{}
	local := pflag.NewFlagSet("local", pflag.ContinueOnError)
	local.Var(refusing, "custom", "")
	local.Var(checked, "pair", "")

	err := cmdflags.ParseOnly(local).Parse([]string{"--custom", "anything", "--pair", "a=b"})

	c.Assert(err, qt.IsNil)
	c.Assert(refusing.sets, qt.Equals, 0)
	c.Assert(checked.sets, qt.Equals, 0)
	c.Assert(local.Lookup("custom").Changed, qt.IsFalse)
	c.Assert(local.Lookup("pair").Changed, qt.IsFalse)
}

// TestParseOnly_KeepsTheFirstDeclaration covers a name two sets declare: the
// copy keeps the first, as cobra keeps a command's own flag over a parent's
// persistent flag of the same name.
func TestParseOnly_KeepsTheFirstDeclaration(t *testing.T) {
	c := qt.New(t)
	local := pflag.NewFlagSet("local", pflag.ContinueOnError)
	local.Bool("env", false, "")
	inherited := pflag.NewFlagSet("inherited", pflag.ContinueOnError)
	inherited.String("env", "", "")

	copied := cmdflags.ParseOnly(local, inherited)

	c.Assert(copied.Lookup("env").Value.Type(), qt.Equals, "bool")
	c.Assert(copied.Parse([]string{"--env", "positional"}), qt.IsNil)
	c.Assert(copied.Args(), qt.DeepEquals, []string{"positional"})
}

// assertUnwritten asserts that no original flag was given a value.
func assertUnwritten(c *qt.C, sets ...*pflag.FlagSet) {
	c.Helper()
	for _, set := range sets {
		set.VisitAll(func(flag *pflag.Flag) {
			c.Assert(flag.Changed, qt.IsFalse, qt.Commentf("--%s", flag.Name))
			c.Assert(flag.Value.String(), qt.Equals, flag.DefValue, qt.Commentf("--%s", flag.Name))
		})
	}
}

// TestParseOnly_WritesNothing covers a set that declares no help flag, which
// pflag answers with ErrHelp after printing its usage to the set's output.
func TestParseOnly_WritesNothing(t *testing.T) {
	c := qt.New(t)
	original := pflag.NewFlagSet("original", pflag.ContinueOnError)
	original.Bool("verbose", false, "")
	copied := cmdflags.ParseOnly(original)

	err := copied.Parse([]string{"--help"})

	c.Assert(err, qt.ErrorIs, pflag.ErrHelp)
	c.Assert(copied.Output(), qt.Equals, io.Discard)
}
