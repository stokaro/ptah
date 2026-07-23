package root

// White-box testing required: executeWithRecovery defines Ptah's top-level CLI
// panic recovery and ordinary-error exit-code mapping. That process-exit
// boundary cannot be observed through NewRootCommand without invoking os.Exit.

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
)

func TestNewRootCommand_AtlasLookingRootPathsStayRejected(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "migrate apply",
			args: []string{"migrate", "apply"},
			want: `unknown command "migrate"`,
		},
		{
			name: "schema inspect",
			args: []string{"schema", "inspect"},
			want: `unexpected positional arguments ["inspect"]`,
		},
		{
			name: "schema viz",
			args: []string{"schema", "viz"},
			want: `unexpected positional arguments ["viz"]`,
		},
		{
			name: "db inspect",
			args: []string{"db", "inspect"},
			want: `unexpected positional arguments ["inspect"]`,
		},
		{
			name: "migrations apply",
			args: []string{"migrations", "apply"},
			want: `unexpected positional arguments ["apply"]`,
		},
		{
			name: "migrations diff",
			args: []string{"migrations", "diff"},
			want: `unexpected positional arguments ["diff"]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			_, stderr, err := executeRoot(tt.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stderr, qt.Contains, tt.want)
		})
	}
}

func TestExecuteWithRecovery_ConvertsCommandPanicToError(t *testing.T) {
	c := qt.New(t)

	var stderr bytes.Buffer
	cmd := &cobra.Command{
		Use: "panic",
		RunE: func(_ *cobra.Command, _ []string) error {
			panic("bad annotation")
		},
	}
	cmd.SetErr(&stderr)

	err := executeWithRecovery(cmd)

	c.Assert(err, qt.ErrorMatches, "internal error: bad annotation")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr.String(), qt.Contains, "error: internal error: bad annotation")
}

func TestZZZRootUnknownSubcommandExits2WithoutUsage(t *testing.T) {
	c := qt.New(t)

	_, stderr, err := executeRoot("bogus-subcommand")

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, `unknown command "bogus-subcommand"`)
	c.Assert(stderr, qt.Not(qt.Contains), "Usage:")
}

func TestZZZRootCommandErrorsExit2(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "compare unreachable database",
			args: []string{
				"schema",
				"compare",
				"--root-dir", filepath.Join("..", "..", "stubs"),
				"--db-url", "postgres://u:p@127.0.0.1:1/db?sslmode=disable",
				"--connect-timeout", "1ms",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			_, stderr, err := executeRoot(tt.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stderr, qt.Contains, "error connecting to database")
			c.Assert(stderr, qt.Not(qt.Contains), "Usage:")
		})
	}
}

func TestZZZRootUsageErrorsExit2WithoutUsage(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "schema export", args: []string{"schema", "export", "--bogus-flag"}},
		{name: "schema render", args: []string{"schema", "render", "--bogus-flag"}},
		{name: "schema compare", args: []string{"schema", "compare", "--bogus-flag"}},
		{name: "schema drift", args: []string{"schema", "drift", "--bogus-flag"}},
		{name: "db read", args: []string{"db", "read", "--bogus-flag"}},
		{name: "db drop-all", args: []string{"db", "drop-all", "--bogus-flag"}},
		{name: "migrations plan", args: []string{"migrations", "plan", "--bogus-flag"}},
		{name: "migrations generate", args: []string{"migrations", "generate", "--bogus-flag"}},
		{name: "migrations create", args: []string{"migrations", "create", "--bogus-flag"}},
		{name: "migrations up", args: []string{"migrations", "up", "--bogus-flag"}},
		{name: "migrations down", args: []string{"migrations", "down", "--bogus-flag"}},
		{name: "migrations status", args: []string{"migrations", "status", "--bogus-flag"}},
		{name: "migrations baseline", args: []string{"migrations", "baseline", "--bogus-flag"}},
		{name: "migrations repair", args: []string{"migrations", "repair", "--bogus-flag"}},
		{name: "migrations hash", args: []string{"migrations", "hash", "--bogus-flag"}},
		{name: "migrations validate", args: []string{"migrations", "validate", "--bogus-flag"}},
		{name: "migrations lint", args: []string{"migrations", "lint", "--bogus-flag"}},
		{name: "seed", args: []string{"seed", "--bogus-flag"}},
		{name: "sql lint", args: []string{"sql", "lint", "--bogus-flag"}},
		{name: "viz", args: []string{"viz", "--bogus-flag"}},
		{name: "atlas version", args: []string{"atlas", "version", "--bogus-flag"}},
		{name: "version", args: []string{"version", "--bogus-flag"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			_, stderr, err := executeRoot(tt.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stderr, qt.Contains, "error: unknown flag: --bogus-flag")
			c.Assert(stderr, qt.Not(qt.Contains), "Usage:")
		})
	}
}

func TestNewRootCommand_UngroupedRootCommandSpellingsAreNotRegistered(t *testing.T) {
	tests := [][]string{
		{"generate"},
		{"read-db"},
		{"compare"},
		{"drift"},
		{"lint"},
		{"migrate"},
		{"migrate-up"},
		{"migrate-down"},
		{"migrate-status"},
		{"migrate-baseline"},
		{"migrate-repair"},
		{"migrate-hash"},
		{"migrate-validate"},
		{"drop-all"},
	}

	for _, args := range tests {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			c := qt.New(t)

			_, stderr, err := executeRoot(args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stderr, qt.Contains, `unknown command "`+args[0]+`"`)
			c.Assert(stderr, qt.Not(qt.Contains), "Usage:")
		})
	}
}

func executeRoot(args ...string) (stdout, stderr string, err error) {
	cmd := NewRootCommand()
	var out bytes.Buffer
	var errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = executeWithRecovery(cmd)
	return out.String(), errOut.String(), err
}
