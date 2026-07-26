package schemasource_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/schemasource"
)

// helperModes maps a mode name to the behavior the re-executed test binary
// performs when it stands in for an external schema program.
var helperModes = map[string]func(){
	"sql": func() {
		fmt.Fprint(os.Stdout, "CREATE TABLE widgets (\n  id INTEGER PRIMARY KEY,\n  name TEXT NOT NULL\n);\n")
		os.Exit(0)
	},
	"badsql": func() {
		fmt.Fprint(os.Stdout, "CREATE TABLE widgets (id INTEGER\n")
		os.Exit(0)
	},
	"fail": func() {
		fmt.Fprintln(os.Stderr, "loader blew up")
		os.Exit(3)
	},
	"sleep": func() {
		time.Sleep(30 * time.Second)
		os.Exit(0)
	},
	"orphan": func() {
		// Spawn a grandchild that inherits stdout and outlives this process, so
		// the stdout pipe stays open after the direct child exits.
		child := exec.Command(os.Args[0], "-test.run=TestHelperProcess") //nolint:gosec // test fixture re-executing this test binary
		child.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1", "SCHEMASOURCE_HELPER_MODE=sleep")
		child.Stdout = os.Stdout
		_ = child.Start()
		os.Exit(0)
	},
}

// runHelperProcess is executed when this test binary is re-run as the external
// schema program by the tests below. It is not itself a test. Keeping the
// dispatch here leaves TestHelperProcess free of control flow.
func runHelperProcess() {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	emit, ok := helperModes[os.Getenv("SCHEMASOURCE_HELPER_MODE")]
	if !ok {
		fmt.Fprintln(os.Stderr, "unknown helper mode")
		os.Exit(1)
	}
	emit()
}

// TestHelperProcess is not a real test; the tests below re-execute this binary
// with -test.run=TestHelperProcess to act as an external schema program.
func TestHelperProcess(t *testing.T) {
	runHelperProcess()
}

func helperArgs() []string {
	return []string{os.Args[0], "-test.run=TestHelperProcess"}
}

func helperEnv(mode string) []string {
	return []string{"GO_WANT_HELPER_PROCESS=1", "SCHEMASOURCE_HELPER_MODE=" + mode}
}

func TestRun_ParsesSQLStdout(t *testing.T) {
	c := qt.New(t)

	db, err := schemasource.Run(context.Background(), schemasource.Command{
		Args: helperArgs(),
		Env:  helperEnv("sql"),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "widgets")
	c.Assert(db.Fields, qt.HasLen, 2)
}

func TestRun_SurfacesStderrOnFailure(t *testing.T) {
	c := qt.New(t)

	_, err := schemasource.Run(context.Background(), schemasource.Command{
		Args: helperArgs(),
		Env:  helperEnv("fail"),
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "loader blew up")
}

func TestRun_TimesOut(t *testing.T) {
	c := qt.New(t)

	_, err := schemasource.Run(context.Background(), schemasource.Command{
		Args:    helperArgs(),
		Env:     helperEnv("sleep"),
		Timeout: 200 * time.Millisecond,
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "timed out")
}

func TestRun_ReportsParseError(t *testing.T) {
	c := qt.New(t)

	_, err := schemasource.Run(context.Background(), schemasource.Command{
		Args: helperArgs(),
		Env:  helperEnv("badsql"),
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "parse schema command")
}

func TestRun_RejectsEmptyCommand(t *testing.T) {
	c := qt.New(t)

	_, err := schemasource.Run(context.Background(), schemasource.Command{})

	c.Assert(err, qt.ErrorMatches, "schema command is empty")
}

func TestRun_RejectsUnsupportedFormat(t *testing.T) {
	c := qt.New(t)

	_, err := schemasource.Run(context.Background(), schemasource.Command{
		Args:   helperArgs(),
		Env:    helperEnv("sql"),
		Format: "hcl",
	})

	c.Assert(err, qt.ErrorMatches, `unsupported schema command format "hcl": only "sql" is supported`)
}

func TestParseCommandLine_SplitsOnWhitespace(t *testing.T) {
	c := qt.New(t)

	c.Assert(schemasource.ParseCommandLine("go run ./loader"), qt.DeepEquals, []string{"go", "run", "./loader"})
	c.Assert(schemasource.ParseCommandLine("   "), qt.HasLen, 0)
}

func TestCommandsFromCLI_BuildsCommand(t *testing.T) {
	c := qt.New(t)

	commands := schemasource.CommandsFromCLI("go run ./loader", "sql", "postgres")

	c.Assert(commands, qt.HasLen, 1)
	c.Assert(commands[0].Args, qt.DeepEquals, []string{"go", "run", "./loader"})
	c.Assert(commands[0].Format, qt.Equals, "sql")
	c.Assert(commands[0].Dialect, qt.Equals, "postgres")
}

func TestCommandsFromCLI_ReturnsNilWhenEmpty(t *testing.T) {
	c := qt.New(t)

	c.Assert(schemasource.CommandsFromCLI("", "sql", ""), qt.IsNil)
	c.Assert(schemasource.CommandsFromCLI("   ", "sql", ""), qt.IsNil)
}

func TestCommandsFromConfig_UsesArgsVerbatim(t *testing.T) {
	c := qt.New(t)

	commands := schemasource.CommandsFromConfig(
		[]string{"go", "run", "./loader", "--path", "./models with spaces"},
		"sql",
		"./project",
		[]string{"FOO=bar"},
	)

	c.Assert(commands, qt.HasLen, 1)
	// The explicit argument list is used verbatim — no whitespace splitting — so
	// an argument containing spaces survives intact.
	c.Assert(commands[0].Args, qt.DeepEquals, []string{"go", "run", "./loader", "--path", "./models with spaces"})
	c.Assert(commands[0].Format, qt.Equals, "sql")
	c.Assert(commands[0].Dir, qt.Equals, "./project")
	c.Assert(commands[0].Env, qt.DeepEquals, []string{"FOO=bar"})
}

func TestCommandsFromConfig_ReturnsNilWhenEmpty(t *testing.T) {
	c := qt.New(t)

	c.Assert(schemasource.CommandsFromConfig(nil, "sql", "", nil), qt.IsNil)
	c.Assert(schemasource.CommandsFromConfig([]string{}, "sql", "", nil), qt.IsNil)
}
