package root

import (
	"bytes"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
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
