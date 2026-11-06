package cmdflags_test

import (
	"errors"
	"regexp"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"ptah.run/internal/cli/internal/cmdflags"
)

func TestInstallStdlibFlagValues_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		declare   func(*pflag.FlagSet)
		value     string
		want      string
		wantValue string
	}{
		{
			name:      "bool",
			declare:   func(fs *pflag.FlagSet) { fs.Bool("flag", false, "") },
			value:     "maybe",
			want:      `invalid argument "maybe" for "--flag" flag: strconv.ParseBool: parsing "maybe": invalid syntax`,
			wantValue: "false",
		},
		{
			name:      "duration, a word",
			declare:   func(fs *pflag.FlagSet) { fs.Duration("flag", 10*time.Second, "") },
			value:     "soon",
			want:      `invalid argument "soon" for "--flag" flag: time: invalid duration "soon"`,
			wantValue: "10s",
		},
		{
			name:      "duration, an empty value",
			declare:   func(fs *pflag.FlagSet) { fs.Duration("flag", 10*time.Second, "") },
			value:     "",
			want:      `invalid argument "" for "--flag" flag: time: invalid duration ""`,
			wantValue: "10s",
		},
		{
			name:      "duration, a number with no unit",
			declare:   func(fs *pflag.FlagSet) { fs.Duration("flag", 10*time.Second, "") },
			value:     "1",
			want:      `invalid argument "1" for "--flag" flag: time: missing unit in duration "1"`,
			wantValue: "10s",
		},
		{
			name:      "int",
			declare:   func(fs *pflag.FlagSet) { fs.Int("flag", 0, "") },
			value:     "x",
			want:      `invalid argument "x" for "--flag" flag: strconv.ParseInt: parsing "x": invalid syntax`,
			wantValue: "0",
		},
		{
			name:      "int8, out of range",
			declare:   func(fs *pflag.FlagSet) { fs.Int8("flag", 0, "") },
			value:     "300",
			want:      `invalid argument "300" for "--flag" flag: strconv.ParseInt: parsing "300": value out of range`,
			wantValue: "0",
		},
		{
			name:      "int16",
			declare:   func(fs *pflag.FlagSet) { fs.Int16("flag", 0, "") },
			value:     "x",
			want:      `invalid argument "x" for "--flag" flag: strconv.ParseInt: parsing "x": invalid syntax`,
			wantValue: "0",
		},
		{
			name:      "int32",
			declare:   func(fs *pflag.FlagSet) { fs.Int32("flag", 0, "") },
			value:     "x",
			want:      `invalid argument "x" for "--flag" flag: strconv.ParseInt: parsing "x": invalid syntax`,
			wantValue: "0",
		},
		{
			name:      "int64",
			declare:   func(fs *pflag.FlagSet) { fs.Int64("flag", 0, "") },
			value:     "x",
			want:      `invalid argument "x" for "--flag" flag: strconv.ParseInt: parsing "x": invalid syntax`,
			wantValue: "0",
		},
		{
			name:      "uint, a negative number",
			declare:   func(fs *pflag.FlagSet) { fs.Uint("flag", 0, "") },
			value:     "-1",
			want:      `invalid argument "-1" for "--flag" flag: strconv.ParseUint: parsing "-1": invalid syntax`,
			wantValue: "0",
		},
		{
			name:      "uint8, out of range",
			declare:   func(fs *pflag.FlagSet) { fs.Uint8("flag", 0, "") },
			value:     "256",
			want:      `invalid argument "256" for "--flag" flag: strconv.ParseUint: parsing "256": value out of range`,
			wantValue: "0",
		},
		{
			name:      "uint16",
			declare:   func(fs *pflag.FlagSet) { fs.Uint16("flag", 0, "") },
			value:     "x",
			want:      `invalid argument "x" for "--flag" flag: strconv.ParseUint: parsing "x": invalid syntax`,
			wantValue: "0",
		},
		{
			name:      "uint32",
			declare:   func(fs *pflag.FlagSet) { fs.Uint32("flag", 0, "") },
			value:     "x",
			want:      `invalid argument "x" for "--flag" flag: strconv.ParseUint: parsing "x": invalid syntax`,
			wantValue: "0",
		},
		{
			name:      "uint64",
			declare:   func(fs *pflag.FlagSet) { fs.Uint64("flag", 0, "") },
			value:     "x",
			want:      `invalid argument "x" for "--flag" flag: strconv.ParseUint: parsing "x": invalid syntax`,
			wantValue: "0",
		},
		{
			name:      "float32",
			declare:   func(fs *pflag.FlagSet) { fs.Float32("flag", 0, "") },
			value:     "x",
			want:      `invalid argument "x" for "--flag" flag: strconv.ParseFloat: parsing "x": invalid syntax`,
			wantValue: "0",
		},
		{
			name:      "float64",
			declare:   func(fs *pflag.FlagSet) { fs.Float64("flag", 0, "") },
			value:     "x",
			want:      `invalid argument "x" for "--flag" flag: strconv.ParseFloat: parsing "x": invalid syntax`,
			wantValue: "0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
			tt.declare(fs)

			cmdflags.InstallStdlibFlagValues(fs)
			err := fs.Parse([]string{"--flag=" + tt.value})

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(tt.want))
			c.Assert(fs.Lookup("flag").Value.String(), qt.Equals, tt.wantValue)
		})
	}
}

// TestInstallStdlibFlagValues_PflagWordingWithoutInstall is the control for
// the table above: the same arguments, parsed by pflag's own values, answer in
// pflag's sentences. When it fails, pflag passes the standard library's error
// through again and the replacement may be unnecessary.
func TestInstallStdlibFlagValues_PflagWordingWithoutInstall(t *testing.T) {
	tests := []struct {
		name    string
		declare func(*pflag.FlagSet)
		value   string
		want    string
	}{
		{
			name:    "bool",
			declare: func(fs *pflag.FlagSet) { fs.Bool("flag", false, "") },
			value:   "maybe",
			want:    `invalid argument "maybe" for "--flag" flag: must be true or false`,
		},
		{
			name:    "duration",
			declare: func(fs *pflag.FlagSet) { fs.Duration("flag", 0, "") },
			value:   "soon",
			want:    `invalid argument "soon" for "--flag" flag: must be a duration like "30s" or "5m"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
			tt.declare(fs)

			err := fs.Parse([]string{"--flag=" + tt.value})

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(tt.want))
		})
	}
}

func TestInstallStdlibFlagValues_HappyPath(t *testing.T) {
	c := qt.New(t)
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	dryRun := fs.Bool("dry-run", false, "")
	strict := fs.Bool("strict", true, "")
	wait := fs.Duration("lock-timeout", 10*time.Second, "")
	limit := fs.Int("limit", 0, "")
	amount := fs.Uint("amount", 0, "")
	ratio := fs.Float64("ratio", 0, "")

	cmdflags.InstallStdlibFlagValues(fs)
	err := fs.Parse([]string{
		"--dry-run", "--strict=false", "--lock-timeout", "5m", "--limit", "0x10", "--amount", "7", "--ratio", "0.5",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(*dryRun, qt.IsTrue)
	c.Assert(*strict, qt.IsFalse)
	c.Assert(*wait, qt.Equals, 5*time.Minute)
	c.Assert(*limit, qt.Equals, 16)
	c.Assert(*amount, qt.Equals, uint(7))
	c.Assert(*ratio, qt.Equals, 0.5)
}

func TestInstallStdlibFlagValues_KeepsTypedAccessors(t *testing.T) {
	c := qt.New(t)
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	fs.Bool("dry-run", false, "")
	fs.Duration("lock-timeout", 0, "")

	cmdflags.InstallStdlibFlagValues(fs)
	c.Assert(fs.Parse([]string{"--dry-run", "--lock-timeout=2s"}), qt.IsNil)
	dryRun, dryRunErr := fs.GetBool("dry-run")
	wait, waitErr := fs.GetDuration("lock-timeout")

	c.Assert(dryRunErr, qt.IsNil)
	c.Assert(dryRun, qt.IsTrue)
	c.Assert(waitErr, qt.IsNil)
	c.Assert(wait, qt.Equals, 2*time.Second)
}

// TestFlagUsages_RendersBooleanFlagsAsAtlasCEDoes pins the help lines the
// pinned Atlas CE binary v1.3.0 prints for `atlas schema apply --help`: a
// boolean flag reads `--dry-run`, with no `[=true|false]` after it. The
// default-true line is the same renderer's answer for a flag Atlas CE does not
// declare on that verb.
func TestFlagUsages_RendersBooleanFlagsAsAtlasCEDoes(t *testing.T) {
	c := qt.New(t)
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	fs.Bool("dry-run", false, "print SQL without executing it")
	fs.BoolP("help", "h", false, "help for apply")
	fs.Duration("lock-timeout", 10*time.Second, "set how long to wait for the database lock")
	fs.Bool("strict", true, "refuse unknown keys")

	got := cmdflags.FlagUsages(fs)

	c.Assert(got, qt.Equals, ""+
		"      --dry-run                 print SQL without executing it\n"+
		"  -h, --help                    help for apply\n"+
		"      --lock-timeout duration   set how long to wait for the database lock (default 10s)\n"+
		"      --strict                  refuse unknown keys (default true)\n")
}

func TestInstallStdlibFlagValues_IsIdempotent(t *testing.T) {
	c := qt.New(t)
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	fs.Duration("lock-timeout", 0, "")

	cmdflags.InstallStdlibFlagValues(fs)
	first := fs.Lookup("lock-timeout").Value
	cmdflags.InstallStdlibFlagValues(fs)

	c.Assert(fs.Lookup("lock-timeout").Value, qt.Equals, first)
}

// modeValue is a value no pflag constructor made whose Type() reads "bool". It
// accepts "auto", which strconv.ParseBool refuses, so a replacement applied to
// it would refuse an argument the flag was declared to take.
type modeValue struct{ mode string }

func (v *modeValue) String() string { return v.mode }

func (v *modeValue) Set(s string) error {
	if s != "auto" && s != "true" && s != "false" {
		return errors.New("want auto, true or false")
	}
	v.mode = s
	return nil
}

func (v *modeValue) Type() string { return "bool" }

func TestInstallStdlibFlagValues_LeavesOtherValues(t *testing.T) {
	t.Run("an argument the value accepts", func(t *testing.T) {
		c := qt.New(t)
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		mode := &modeValue{mode: "false"}
		fs.Var(mode, "mode", "")

		cmdflags.InstallStdlibFlagValues(fs)
		err := fs.Parse([]string{"--mode=auto"})

		c.Assert(err, qt.IsNil)
		c.Assert(mode.mode, qt.Equals, "auto")
		c.Assert(fs.Lookup("mode").Value, qt.Equals, pflag.Value(mode))
	})
	t.Run("an argument the value refuses", func(t *testing.T) {
		c := qt.New(t)
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		fs.Var(&modeValue{mode: "false"}, "mode", "")

		cmdflags.InstallStdlibFlagValues(fs)
		err := fs.Parse([]string{"--mode=maybe"})

		c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(`invalid argument "maybe" for "--mode" flag: want auto, true or false`))
	})
}

func TestInstallStdlibValues_ReachesEveryCommand(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "a child's local flag",
			args: []string{"child", "--dry-run=maybe"},
			want: `invalid argument "maybe" for "--dry-run" flag: strconv.ParseBool: parsing "maybe": invalid syntax`,
		},
		{
			name: "a persistent flag the child inherits",
			args: []string{"child", "--lock-timeout=soon"},
			want: `invalid argument "soon" for "--lock-timeout" flag: time: invalid duration "soon"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			root := &cobra.Command{Use: "root", SilenceErrors: true, SilenceUsage: true}
			root.PersistentFlags().Duration("lock-timeout", 0, "")
			child := &cobra.Command{Use: "child", RunE: func(*cobra.Command, []string) error { return nil }}
			child.Flags().Bool("dry-run", false, "")
			root.AddCommand(child)

			cmdflags.InstallStdlibValues(root)
			root.SetArgs(tt.args)
			err := root.Execute()

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(tt.want))
		})
	}
}
