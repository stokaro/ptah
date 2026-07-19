package root

import (
	"bytes"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
)

func TestNewRootCommand_UsesPtahBranding(t *testing.T) {
	c := qt.New(t)

	cmd := NewRootCommand()

	c.Assert(cmd.Use, qt.Equals, "ptah")
	c.Assert(cmd.Short, qt.Contains, "Ptah")
	c.Assert(cmd.Version, qt.Not(qt.Equals), "")
}

func TestNewRootCommand_HelpAdvertisesPtahEnvVars(t *testing.T) {
	c := qt.New(t)
	cmd := NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate-up", "--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Usage:")
	c.Assert(out.String(), qt.Contains, "[env: PTAH_DB_URL]")
	c.Assert(out.String(), qt.Not(qt.Contains), "PACKAGE_"+"MIGRATOR")
}

func TestNewRootCommand_VersionSubcommandPrintsBuildInfo(t *testing.T) {
	c := qt.New(t)
	cmd := NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"version"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Version: ")
	c.Assert(out.String(), qt.Contains, "Commit: ")
	c.Assert(out.String(), qt.Contains, "Date: ")
	c.Assert(out.String(), qt.Contains, "Go: ")
	c.Assert(out.String(), qt.Contains, "Platform: ")
}

func TestNewRootCommand_SchemaExportSubcommandIsRegistered(t *testing.T) {
	c := qt.New(t)
	cmd := NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "export", "--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Export one schema source format to another")
	c.Assert(out.String(), qt.Contains, "--cleanup-go-annotations")
}

func TestNewRootCommand_VersionFlagWorks(t *testing.T) {
	c := qt.New(t)
	cmd := NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--version"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "ptah version")
}

func TestNewRootCommand_PTAHDBURLFeedsCommandFlag(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_DB_URL", "postgres://user:pass@127.0.0.1:1/db?sslmode=disable")
	cmd := NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate-status", "--migrations-dir", filepath.ToSlash(t.TempDir())})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "database URL is required")
	c.Assert(err.Error(), qt.Contains, "error connecting to database")
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
		{name: "generate", args: []string{"generate", "--bogus-flag"}},
		{name: "read-db", args: []string{"read-db", "--bogus-flag"}},
		{name: "schema export", args: []string{"schema", "export", "--bogus-flag"}},
		{name: "compare", args: []string{"compare", "--bogus-flag"}},
		{name: "drift", args: []string{"drift", "--bogus-flag"}},
		{name: "migrate", args: []string{"migrate", "--bogus-flag"}},
		{name: "migrate generate", args: []string{"migrate", "generate", "--bogus-flag"}},
		{name: "migrate new", args: []string{"migrate", "new", "--bogus-flag"}},
		{name: "migrate-baseline", args: []string{"migrate-baseline", "--bogus-flag"}},
		{name: "migrate-up", args: []string{"migrate-up", "--bogus-flag"}},
		{name: "migrate-down", args: []string{"migrate-down", "--bogus-flag"}},
		{name: "migrate-repair", args: []string{"migrate-repair", "--bogus-flag"}},
		{name: "migrate-status", args: []string{"migrate-status", "--bogus-flag"}},
		{name: "migrate-hash", args: []string{"migrate-hash", "--bogus-flag"}},
		{name: "migrate-validate", args: []string{"migrate-validate", "--bogus-flag"}},
		{name: "seed", args: []string{"seed", "--bogus-flag"}},
		{name: "drop-all", args: []string{"drop-all", "--bogus-flag"}},
		{name: "lint", args: []string{"lint", "--bogus-flag"}},
		{name: "sql lint", args: []string{"sql", "lint", "--bogus-flag"}},
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
