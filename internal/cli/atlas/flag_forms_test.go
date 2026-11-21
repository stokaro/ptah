package atlas_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/envbool/envbooltest"
)

// clearURLEnvironment unsets the variables that could name a database or a
// directory for the commands below, so each run reads only its arguments.
func clearURLEnvironment(t *testing.T) {
	for _, name := range []string{"PTAH_URL", "PTAH_DB_URL", "PTAH_DEV_URL", "PTAH_DIR", "PTAH_DIR_FORMAT", "PTAH_FORMAT"} {
		envbooltest.Unset(name)(t)
	}
}

// TestCompatShorthandValue_ReadsAsTheSeparateForm runs each command with -u
// and its value attached, `-usqlite://x.db`, and after =, `-u=sqlite://x.db`,
// and with the value as the next argument, and a group of boolean shorthands
// beside the same shorthands written apart. pflag reads each set as the same
// flags and values, as the community binary does, so each run answers as the
// separate form does. The separate form is the control: it reaches the
// command rather than a flag error.
func TestCompatShorthandValue_ReadsAsTheSeparateForm(t *testing.T) {
	tests := []struct {
		name     string
		command  []string
		separate []string
		forms    [][]string
	}{
		{name: "security scan", command: []string{"security", "scan"},
			separate: []string{"-u", "sqlite://x.db"}, forms: [][]string{{"-usqlite://x.db"}, {"-u=sqlite://x.db"}}},
		{name: "schema inspect", command: []string{"schema", "inspect"},
			separate: []string{"-u", "sqlite://x.db"}, forms: [][]string{{"-usqlite://x.db"}, {"-u=sqlite://x.db"}}},
		{name: "migrate status", command: []string{"migrate", "status"},
			separate: []string{"-u", "sqlite://x.db"}, forms: [][]string{{"-usqlite://x.db"}, {"-u=sqlite://x.db"}}},
		{name: "migrate down", command: []string{"migrate", "down"},
			separate: []string{"-u", "sqlite://x.db"}, forms: [][]string{{"-usqlite://x.db"}, {"-u=sqlite://x.db"}}},
		// -l and -s are booleans, so pflag reads -ls as -l -s.
		{name: "migrate ls", command: []string{"migrate", "ls"},
			separate: []string{"-l", "-s"}, forms: [][]string{{"-ls"}, {"-sl"}}},
		{name: "migrate ls help", command: []string{"migrate", "ls"},
			separate: []string{"-l", "-h"}, forms: [][]string{{"-lh"}, {"-hl"}}},
		// schema test takes a positional, so this row also reads the splitter
		// that sets the positional apart from the flags.
		{name: "schema test", command: []string{"schema", "test"},
			separate: []string{"-u", "sqlite://x.db"}, forms: [][]string{{"-usqlite://x.db"}, {"-u=sqlite://x.db"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			t.Chdir(t.TempDir())
			envbooltest.Set(securityScanVar, "1")(t)
			clearURLEnvironment(t)
			want := runTree(atlas.NewCompatCommand("atlas"), append(tt.command, tt.separate...)...)
			c.Assert(want.stderr, qt.Not(qt.Matches), `(?s).*(unknown shorthand|required flag|flag needs an argument).*`)

			for _, form := range tt.forms {
				got := runTree(atlas.NewCompatCommand("atlas"), append(tt.command, form...)...)

				c.Assert(got, qt.Equals, want, qt.Commentf("%q", form))
			}
		})
	}
}

// TestCompatFlagWithoutValue_FailurePath refuses a flag that takes a value
// and ends the arguments without one, naming the flag as the caller typed it,
// as the community binary does. Forwarded bare, the flag reaches the native
// command, which names its own flag, --db-url for -u and --shadow-db for
// --dev-url, or takes an argument the surface adds as the value.
func TestCompatFlagWithoutValue_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "migrate down -u", args: []string{"migrate", "down", "-u"},
			wantStderr: "Error: flag needs an argument: 'u' in -u\n"},
		{name: "migrate down --dev-url", args: []string{"migrate", "down", "-u", "sqlite://x.db", "--dev-url"},
			wantStderr: "Error: flag needs an argument: --dev-url\n"},
		{name: "migrate hash --dir before its default format", args: []string{"migrate", "hash", "--dir"},
			wantStderr: "Error: flag needs an argument: --dir\n"},
		{name: "security scan --format before the database", args: []string{"security", "scan", "--url", "sqlite://x.db", "--format"},
			wantStderr: "Error: flag needs an argument: --format\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			t.Chdir(t.TempDir())
			envbooltest.Set(securityScanVar, "1")(t)
			clearURLEnvironment(t)

			got := runTree(atlas.NewCompatCommand("atlas"), tt.args...)

			c.Assert(got, qt.Equals, compatRun{stderr: tt.wantStderr, code: 1})
		})
	}
}
