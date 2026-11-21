package atlas_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
)

// The rows below pin how a forwarded verb reads --help beside its other
// arguments. The pinned community binary is a cobra program that parses its
// flags itself: it parses every flag first, and only then reads --help. Each
// stderr below was measured on the pinned community binary v1.4.0 in an empty
// directory, standard output and standard error captured apart and the exit
// status read from an unpiped run, and is byte-identical here.

// TestCompatHelp_RefusesBeforeHelp_FailurePath covers arguments that ask for
// help and carry something the pinned binary refuses: an unknown flag, a value
// flag with no value, a value its flag refuses. The refusal wins wherever it
// stands, so nothing is printed on standard output.
func TestCompatHelp_RefusesBeforeHelp_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{
			name:       "an unknown shorthand after help",
			args:       []string{"migrate", "hash", "-h", "-x"},
			wantStderr: "Error: unknown shorthand flag: 'x' in -x\n",
		},
		{
			name:       "an unknown shorthand before help",
			args:       []string{"migrate", "hash", "-x", "-h"},
			wantStderr: "Error: unknown shorthand flag: 'x' in -x\n",
		},
		{
			name:       "an unknown shorthand grouped with help",
			args:       []string{"migrate", "hash", "-hx"},
			wantStderr: "Error: unknown shorthand flag: 'x' in -x\n",
		},
		{
			name:       "an unknown shorthand grouped with help on validate",
			args:       []string{"migrate", "validate", "-hx"},
			wantStderr: "Error: unknown shorthand flag: 'x' in -x\n",
		},
		{
			name:       "an unknown shorthand after help on new",
			args:       []string{"migrate", "new", "-h", "-x"},
			wantStderr: "Error: unknown shorthand flag: 'x' in -x\n",
		},
		{
			name:       "an unknown long flag after help",
			args:       []string{"migrate", "checkpoint", "--help", "--nonexistent"},
			wantStderr: "Error: unknown flag: --nonexistent\n",
		},
		{
			name:       "an unknown long flag before help",
			args:       []string{"schema", "test", "--nonexistent", "--help"},
			wantStderr: "Error: unknown flag: --nonexistent\n",
		},
		{
			name:       "a native flag the surface refuses",
			args:       []string{"migrate", "down", "-h", "--amount=abc"},
			wantStderr: "Error: unknown flag: --amount\n",
		},
		{
			name:       "a value flag with no value",
			args:       []string{"migrate", "hash", "-h", "--dir"},
			wantStderr: "Error: flag needs an argument: --dir\n",
		},
		{
			name:       "an inherited value flag with no value",
			args:       []string{"migrate", "hash", "-h", "--env"},
			wantStderr: "Error: flag needs an argument: --env\n",
		},
		{
			name:       "an inherited shorthand with no value",
			args:       []string{"migrate", "hash", "-h", "-c"},
			wantStderr: "Error: flag needs an argument: 'c' in -c\n",
		},
		{
			name:       "a --var with no separator",
			args:       []string{"migrate", "hash", "-h", "--var", "bad"},
			wantStderr: `Error: invalid argument "bad" for "--var" flag: variables must be format as key=value, got: "bad"` + "\n",
		},
		{
			name:       "a --var whose second field has no separator, before help",
			args:       []string{"migrate", "hash", "--var", "a=1,b", "-h"},
			wantStderr: `Error: invalid argument "a=1,b" for "--var" flag: variables must be format as key=value, got: "b"` + "\n",
		},
		{
			name:       "a malformed --help value on a verb that resolves its directory first",
			args:       []string{"migrate", "hash", "--help=x"},
			wantStderr: `Error: invalid argument "x" for "-h, --help" flag: strconv.ParseBool: parsing "x": invalid syntax` + "\n",
		},
		{
			name:       "a malformed --help value on a forwarded verb",
			args:       []string{"migrate", "down", "--help=x"},
			wantStderr: `Error: invalid argument "x" for "-h, --help" flag: strconv.ParseBool: parsing "x": invalid syntax` + "\n",
		},
		{
			name:       "help as the value of --dir",
			args:       []string{"migrate", "hash", "--dir", "-h"},
			wantStderr: "Error: missing scheme for dir url. Did you mean \"file://-h\"? \n",
		},
		{
			name:       "help as the value of --dir on new",
			args:       []string{"migrate", "new", "--dir", "-h"},
			wantStderr: "Error: missing scheme for dir url. Did you mean \"file://-h\"? \n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			t.Chdir(t.TempDir())

			run := runTree(atlas.NewCompatCommand("atlas"), tt.args...)

			c.Assert(run.code, qt.Equals, 1)
			c.Assert(run.stderr, qt.Equals, tt.wantStderr)
			c.Assert(run.stdout, qt.Equals, "")
		})
	}
}

// TestCompatHelp_PrintsHelp_HappyPath covers arguments that ask for help and
// carry nothing the pinned binary refuses. A positional, another flag's value
// and an inherited flag each leave help in charge, as they do there.
func TestCompatHelp_PrintsHelp_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "the shorthand",
			args: []string{"migrate", "hash", "-h"},
			want: "Forwards to `ptah migrations hash`.",
		},
		{
			name: "an explicit true",
			args: []string{"migrate", "hash", "--help=true"},
			want: "Forwards to `ptah migrations hash`.",
		},
		{
			name: "help with a positional",
			args: []string{"migrate", "hash", "-h", "foo"},
			want: "Forwards to `ptah migrations hash`.",
		},
		{
			name: "help with an attached shorthand value",
			args: []string{"migrate", "hash", "-h", "-cfoo"},
			want: "Forwards to `ptah migrations hash`.",
		},
		{
			name: "help after an inherited flag and its value",
			args: []string{"migrate", "hash", "--env", "dev", "-h"},
			want: "Forwards to `ptah migrations hash`.",
		},
		{
			name: "help with a well-formed --var",
			args: []string{"migrate", "hash", "-h", "--var", "a=b"},
			want: "Forwards to `ptah migrations hash`.",
		},
		{
			// --latest is a boolean of this verb, so `-lh` is `-l -h`, as for
			// any cobra command. The pinned binary has no `migrate ls`.
			name: "help grouped with a boolean",
			args: []string{"migrate", "ls", "-lh"},
			want: "Forwards to `ptah migrations ls`.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			t.Chdir(t.TempDir())

			run := runTree(atlas.NewCompatCommand("atlas"), tt.args...)

			c.Assert(run.code, qt.Equals, 0)
			c.Assert(run.stdout, qt.Contains, tt.want)
			c.Assert(run.stderr, qt.Equals, "")
		})
	}
}

// TestCompatHelp_ExplicitFalseRunsTheVerb covers `--help=false` and
// `-h=false`, which ask for no help: the verb runs and reports the missing
// migration directory, as the pinned binary does in an empty directory (in its
// own words, `sql/migrate: stat migrations: no such file or directory`).
func TestCompatHelp_ExplicitFalseRunsTheVerb(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "the long form", args: []string{"migrate", "hash", "--help=false"}},
		{name: "the shorthand", args: []string{"migrate", "hash", "-h=false"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			t.Chdir(t.TempDir())

			run := runTree(atlas.NewCompatCommand("atlas"), tt.args...)

			c.Assert(run.code, qt.Equals, 1)
			c.Assert(run.stderr, qt.Equals,
				"Error: migrations directory migrations: stat migrations: no such file or directory\n")
			c.Assert(run.stdout, qt.Equals, "")
		})
	}
}

// TestCompatHelp_AValueNamedHelpKeepsTheVerbsChecks covers `migrate down
// --dir -h`, where -h is the value of --dir and asks for no help. The verb's
// own checks run, so a malformed PTAH_ATLAS_REPEATABLE_MIGRATIONS is refused
// by name before anything is forwarded.
func TestCompatHelp_AValueNamedHelpKeepsTheVerbsChecks(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	t.Setenv("PTAH_ATLAS_REPEATABLE_MIGRATIONS", "maybe")

	run := runTree(atlas.NewCompatCommand("atlas"), "migrate", "down", "--dir", "-h")

	c.Assert(run.code, qt.Equals, 1)
	c.Assert(run.stderr, qt.Equals, `Error: invalid boolean value "maybe" for PTAH_ATLAS_REPEATABLE_MIGRATIONS`+"\n")
	c.Assert(run.stdout, qt.Equals, "")
}
