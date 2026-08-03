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

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
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
			// schema inspect and schema apply became native verbs with #850;
			// schema clean remains an Atlas-only spelling (native: db drop-all).
			name: "schema clean",
			args: []string{"schema", "clean"},
			want: `unexpected positional arguments ["clean"]`,
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

// TestNativeDiagnosticsKeepTheNativePrefix pins the native half of the
// stokaro/ptah#1019 rule: a process-level diagnostic on the native ptah surface
// is prefixed "error: " at exit 2, while the same failure on the compat surface
// is prefixed "Error: " at exit 1 (pinned by
// TestCompatBinaryCommandFailuresExit1 in cmd/ptah-compat).
//
// The assertion is byte-exact rather than a substring on purpose. cmdutil's
// printers are shared by both surfaces, so the cheap way to make the compat
// pins green is to edit the literal inside them — which would silently rewrite
// every native diagnostic in the binary. That shortcut turns this test red;
// resolving the prefix from the command tree keeps both green.
func TestNativeDiagnosticsKeepTheNativePrefix(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{
			name:       "unknown flag",
			args:       []string{"version", "--bogus-flag"},
			wantStderr: "error: unknown flag: --bogus-flag\n",
		},
		{
			name:       "unknown command",
			args:       []string{"definitely-not-a-command"},
			wantStderr: "error: unknown command \"definitely-not-a-command\" for \"ptah\"\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, stderr, err := executeRoot(tt.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, tt.wantStderr)
		})
	}
}

// TestExecuteWithRecovery_ConvertsCommandPanicToError covers the recovered
// panic at the process boundary, which the exit-code documentation names as a
// member of the process-level diagnostic class.
//
// It has two rows because one is not a discriminator. With only the default
// root, reverting the prefix lookup to a hardcoded "error: " leaves the whole
// suite green -- this is the one printer in the documented class that no other
// gate watches, so a later simplification back to the literal would silently
// reintroduce the split the policy exists to close.
func TestExecuteWithRecovery_ConvertsCommandPanicToError(t *testing.T) {
	tests := []struct {
		name string
		// declare wires the surface's prefix policy. The native row leaves it
		// alone rather than passing an empty prefix, because
		// [cmdutil.SetErrorPrefixPolicy] rejects that by design.
		declare    func(*cobra.Command)
		wantStderr string
	}{
		{
			name:       "native default",
			declare:    func(*cobra.Command) {},
			wantStderr: "error: internal error: bad annotation",
		},
		{
			name:       "surface declaring its own prefix",
			declare:    func(cmd *cobra.Command) { cmdutil.SetErrorPrefixPolicy(cmd, "Error") },
			wantStderr: "Error: internal error: bad annotation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			var stderr bytes.Buffer
			cmd := &cobra.Command{
				Use: "panic",
				RunE: func(_ *cobra.Command, _ []string) error {
					panic("bad annotation")
				},
			}
			cmd.SetErr(&stderr)
			tt.declare(cmd)

			err := executeWithRecovery(cmd)

			c.Assert(err, qt.ErrorMatches, "internal error: bad annotation")
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stderr.String(), qt.Contains, tt.wantStderr)
		})
	}
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

// TestZZZAtlasNamespaceNotRegistered pins the removal of the ptah atlas
// command tree (#850): Atlas-compatible spellings live only in the separate
// ptah-compat binary, so any atlas invocation is a plain unknown command with
// the native exit-code contract.
func TestZZZAtlasNamespaceNotRegistered(t *testing.T) {
	tests := [][]string{
		{"atlas"},
		{"atlas", "version"},
		{"atlas", "migrate", "apply"},
		{"atlas", "schema", "inspect"},
	}

	for _, args := range tests {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			c := qt.New(t)

			_, stderr, err := executeRoot(args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stderr, qt.Contains, `unknown command "atlas" for "ptah"`)
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
