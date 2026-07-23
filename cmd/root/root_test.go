package root_test

import (
	"bytes"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/root"
)

func TestNewRootCommand_UsesPtahBranding(t *testing.T) {
	c := qt.New(t)

	cmd := root.NewRootCommand()

	c.Assert(cmd.Use, qt.Equals, "ptah")
	c.Assert(cmd.Short, qt.Contains, "Ptah")
	c.Assert(cmd.Version, qt.Not(qt.Equals), "")
}

func TestNewRootCommand_HelpAdvertisesPtahEnvVars(t *testing.T) {
	c := qt.New(t)
	cmd := root.NewRootCommand()
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
	cmd := root.NewRootCommand()
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
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "export", "--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Export a Ptah schema to another format")
	c.Assert(out.String(), qt.Contains, "--cleanup-go-annotations")
}

func TestNewRootCommand_NativeCommandTreeIsRegistered(t *testing.T) {
	c := qt.New(t)

	cmd := root.NewRootCommand()
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

func TestNewRootCommand_VersionFlagWorks(t *testing.T) {
	c := qt.New(t)
	cmd := root.NewRootCommand()
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
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrations", "status", "--migrations-dir", filepath.ToSlash(t.TempDir())})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "database URL is required")
	c.Assert(err.Error(), qt.Contains, "error connecting to database")
}

func TestNewRootCommand_PTAHAutoApproveDoesNotBypassDropAllConfirmation(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_AUTO_APPROVE", "true")

	dbURL := (&url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "ptah.db")}).String()
	out, _, err := captureRootStdIO(c, "no\n", "db", "drop-all", "--db-url", dbURL)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "Type 'DELETE EVERYTHING'")
	c.Assert(out, qt.Contains, "Operation canceled.")
	c.Assert(out, qt.Not(qt.Contains), "Auto-approval enabled")
}

func executeRootCommand(args ...string) (stdout, stderr string, err error) {
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	var errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

func captureRootStdIO(c *qt.C, input string, args ...string) (stdout, stderr string, err error) {
	c.Helper()

	oldStdin := os.Stdin
	oldStdout := os.Stdout
	defer func() {
		os.Stdin = oldStdin
		os.Stdout = oldStdout
	}()

	inR, inW, err := os.Pipe()
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(inR.Close(), qt.IsNil) }()
	_, err = inW.WriteString(input)
	c.Assert(err, qt.IsNil)
	c.Assert(inW.Close(), qt.IsNil)

	outR, outW, err := os.Pipe()
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(outR.Close(), qt.IsNil) }()

	os.Stdin = inR
	os.Stdout = outW
	stdout, stderr, err = executeRootCommand(args...)
	c.Assert(outW.Close(), qt.IsNil)

	outBytes, readErr := io.ReadAll(outR)
	c.Assert(readErr, qt.IsNil)
	return stdout + string(outBytes), stderr, err
}
