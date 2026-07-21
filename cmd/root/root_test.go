package root

import (
	"bytes"
	"path/filepath"
	"strings"
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
	cmd.SetArgs([]string{"migrations", "up", "--help"})

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

func TestNewRootCommand_NativeCommandTreeIsRegistered(t *testing.T) {
	c := qt.New(t)

	cmd := NewRootCommand()
	for _, path := range [][]string{
		{"schema", "render"},
		{"schema", "compare"},
		{"schema", "drift"},
		{"introspect"},
		{"db", "read"},
		{"db", "drop-all"},
		{"migrations", "plan"},
		{"migrations", "generate"},
		{"migrations", "create"},
		{"migrations", "up"},
		{"migrations", "down"},
		{"migrations", "status"},
		{"migrations", "baseline"},
		{"migrations", "repair"},
		{"migrations", "hash"},
		{"migrations", "validate"},
		{"migrations", "lint"},
		{"viz"},
	} {
		found, _, err := cmd.Find(path)
		c.Assert(err, qt.IsNil)
		c.Assert(found.CommandPath(), qt.Equals, "ptah "+strings.Join(path, " "))
	}
}

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
	cmd.SetArgs([]string{"migrations", "status", "--migrations-dir", filepath.ToSlash(t.TempDir())})

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
