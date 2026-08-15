package schemasource_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemasource"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// helperModes maps a mode name to the behavior the re-executed test binary
// performs when it stands in for an external schema program.
var helperModes = map[string]func(){
	"sql": func() {
		emitWidgetSQL()
		exitHelper(0)
	},
	"roundtrip-sql": func() {
		emitRoundTripSQL()
		exitHelper(0)
	},
	"yaml": func() {
		fmt.Fprint(os.Stdout, "tables:\n  widgets:\n    columns:\n      id: {type: INTEGER, primary: true}\n      name: {type: TEXT, not_null: true}\n")
		exitHelper(0)
	},
	"hcl": func() {
		fmt.Fprint(os.Stdout, "table \"widgets\" {\n  column \"id\" {\n    type = int\n  }\n  column \"name\" {\n    type = text\n    null = false\n  }\n  primary_key {\n    columns = [column.id]\n  }\n}\n")
		exitHelper(0)
	},
	"badsql": func() {
		fmt.Fprint(os.Stdout, "CREATE TABLE widgets (id INTEGER\n")
		exitHelper(0)
	},
	"secret-badsql": func() {
		fmt.Fprint(os.Stdout, "CREATE SECRET object;\n")
		exitHelper(0)
	},
	"empty": func() {
		exitHelper(0)
	},
	"whitespace": func() {
		fmt.Fprint(os.Stdout, " \n\t")
		exitHelper(0)
	},
	"fail": func() {
		fmt.Fprintln(os.Stderr, "loader blew up")
		exitHelper(3)
	},
	"secret-fail": func() {
		fmt.Fprintf(os.Stderr, "token=%s \x1b[31mfailed\r\n", os.Getenv("SCHEMA_TOKEN"))
		exitHelper(3)
	},
	"large-stderr-fail": func() {
		fmt.Fprintln(os.Stderr, "EARLY_DIAGNOSTIC")
		fmt.Fprint(os.Stderr, strings.Repeat("x", 70<<10))
		fmt.Fprintln(os.Stderr, "FINAL_DIAGNOSTIC")
		exitHelper(3)
	},
	"sleep": func() {
		time.Sleep(30 * time.Second)
		exitHelper(0)
	},
	"pwd": func() {
		currentDir, _ := os.Getwd()
		if currentDir != os.Getenv("SCHEMASOURCE_EXPECTED_PWD") ||
			os.Getenv("PWD") != os.Getenv("SCHEMASOURCE_EXPECTED_PWD") {
			fmt.Fprintf(os.Stderr, "cwd=%s PWD=%s", currentDir, os.Getenv("PWD"))
			exitHelper(4)
		}
		fmt.Fprint(os.Stdout, "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n")
		exitHelper(0)
	},
	"orphan": func() {
		startSurvivor(os.Stdout, os.Stderr)
		exitHelper(0)
	},
	"orphan-detached": func() {
		startSurvivor(nil, nil)
		exitHelper(0)
	},
	"orphan-detached-sql": func() {
		startSurvivor(nil, nil)
		emitWidgetSQL()
		exitHelper(0)
	},
	"survivor": func() {
		_ = os.WriteFile(
			os.Getenv("SCHEMASOURCE_STARTED_FILE"),
			[]byte("started"),
			0o600,
		)
		time.Sleep(750 * time.Millisecond)
		_ = os.WriteFile(
			os.Getenv("SCHEMASOURCE_SURVIVOR_FILE"),
			[]byte("survived"),
			0o600,
		)
		exitHelper(0)
	},
}

func exitHelper(code int) {
	os.Exit(code) //revive:disable-line:deep-exit subprocess fixture must terminate before the Go test runner writes to stdout
}

func emitWidgetSQL() {
	fmt.Fprint(os.Stdout, "CREATE TABLE widgets (\n  id INTEGER PRIMARY KEY,\n  name TEXT NOT NULL\n);\n")
}

func emitRoundTripSQL() {
	fmt.Fprint(os.Stdout, `CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY,
  "email" TEXT NOT NULL
);
CREATE UNIQUE INDEX "idx_users_email" ON "users" ("email");
CREATE TABLE "posts" (
  "id" INTEGER PRIMARY KEY,
  "user_id" INTEGER NOT NULL,
  "title" TEXT NOT NULL,
  CONSTRAINT "fk_posts_user" FOREIGN KEY ("user_id") REFERENCES "users" ("id") ON DELETE CASCADE
);
CREATE INDEX "idx_posts_user_id" ON "posts" ("user_id");
`)
}

func startSurvivor(stdout, stderr *os.File) {
	child := exec.Command(os.Args[0], "-test.run=TestHelperProcess") // #nosec -- test fixture re-executing this test binary
	child.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1", "SCHEMASOURCE_HELPER_MODE=survivor")
	child.Stdout = stdout
	child.Stderr = stderr
	if err := child.Start(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		exitHelper(5)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat( // #nosec -- test fixture path supplied by the parent test process
			os.Getenv("SCHEMASOURCE_STARTED_FILE"),
		); err == nil {
			return
		}
		if time.Now().After(deadline) {
			fmt.Fprintln(os.Stderr, "survivor did not start")
			exitHelper(6)
		}
		time.Sleep(10 * time.Millisecond)
	}
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
		exitHelper(1)
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
	return []string{
		"GO_WANT_HELPER_PROCESS=1",
		"SCHEMASOURCE_HELPER_MODE=" + mode,
		"GORACE=atexit_sleep_ms=0",
	}
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

func TestRun_SQLiteRoundTripConverges(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()

	desired, err := schemasource.Run(ctx, schemasource.Command{
		Args:    helperArgs(),
		Env:     helperEnv("roundtrip-sql"),
		Dialect: "sqlite",
	})
	c.Assert(err, qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+t.TempDir()+"/roundtrip.db")
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	statements, err := renderer.GetOrderedCreateStatements(desired, "sqlite")
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, err = conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}

	live, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	diff := schemadiff.CompareWithDialect(desired, live, "sqlite")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %#v", diff))
}

func TestRun_ParsesYAMLStdout(t *testing.T) {
	c := qt.New(t)

	db, err := schemasource.Run(context.Background(), schemasource.Command{
		Args:   helperArgs(),
		Env:    helperEnv("yaml"),
		Format: "yaml",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "widgets")
	c.Assert(db.Fields, qt.HasLen, 2)
}

func TestRun_ParsesHCLStdout(t *testing.T) {
	c := qt.New(t)

	db, err := schemasource.Run(context.Background(), schemasource.Command{
		Args:   helperArgs(),
		Env:    helperEnv("hcl"),
		Format: "hcl",
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

	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
	c.Assert(err.Error(), qt.Contains, "timed out")
}

func TestRun_PreservesCallerCancellation(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	_, err := schemasource.Run(ctx, schemasource.Command{
		Args: helperArgs(),
		Env:  helperEnv("sleep"),
	})

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(err.Error(), qt.Contains, "canceled")
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

func TestRun_SanitizesParseError(t *testing.T) {
	c := qt.New(t)

	_, err := schemasource.Run(context.Background(), schemasource.Command{
		Args: helperArgs(),
		Env: append(
			helperEnv("secret-badsql"),
			"SCHEMA_TOKEN=SECRET",
		),
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "SECRET")
	c.Assert(err.Error(), qt.Contains, "unsupported CREATE target: redacted")
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
		Format: "json",
	})

	c.Assert(err, qt.ErrorMatches, `unsupported schema command format "json": expected sql, hcl, or yaml`)
}

func TestRun_RejectsEmptyOutput(t *testing.T) {
	c := qt.New(t)

	_, err := schemasource.Run(context.Background(), schemasource.Command{
		Args: helperArgs(),
		Env:  helperEnv("empty"),
	})

	c.Assert(err, qt.ErrorMatches, `schema command ".*" produced empty output`)
}

func TestRun_RejectsWhitespaceOnlyOutput(t *testing.T) {
	c := qt.New(t)

	_, err := schemasource.Run(context.Background(), schemasource.Command{
		Args: helperArgs(),
		Env:  helperEnv("whitespace"),
	})

	c.Assert(err, qt.ErrorMatches, `schema command ".*" produced empty output`)
}

func TestRun_AllowsEmbedderWorkingDirectoryOutsideCurrentDirectory(t *testing.T) {
	c := qt.New(t)

	db, err := schemasource.Run(context.Background(), schemasource.Command{
		Args: helperArgs(),
		Dir:  "..",
		Env:  helperEnv("sql"),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
}

func TestRun_UpdatesPWDForWorkingDirectory(t *testing.T) {
	c := qt.New(t)
	workingDir := t.TempDir()

	db, err := schemasource.Run(context.Background(), schemasource.Command{
		Args: helperArgs(),
		Dir:  workingDir,
		Env: append(
			helperEnv("pwd"),
			"SCHEMASOURCE_EXPECTED_PWD="+workingDir,
		),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
}

func TestRun_RejectsPATHOverride(t *testing.T) {
	c := qt.New(t)

	_, err := schemasource.Run(context.Background(), schemasource.Command{
		Args: helperArgs(),
		Env:  []string{"PATH=/custom/bin"},
	})

	c.Assert(err, qt.ErrorMatches, "schema command environment must not override PATH; use an explicit executable path")
}

func TestRun_RejectsPWDOverride(t *testing.T) {
	c := qt.New(t)

	_, err := schemasource.Run(context.Background(), schemasource.Command{
		Args: helperArgs(),
		Env:  []string{"PWD=/tmp/incorrect"},
	})

	c.Assert(err, qt.ErrorMatches, "schema command environment must not override PWD; use the command working directory")
}

func TestRun_RedactsAndSanitizesStderr(t *testing.T) {
	c := qt.New(t)

	_, err := schemasource.Run(context.Background(), schemasource.Command{
		Args: helperArgs(),
		Env: append(
			helperEnv("secret-fail"),
			"SCHEMA_TOKEN=top-secret",
		),
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "top-secret")
	c.Assert(err.Error(), qt.Contains, "token=redacted")
	c.Assert(err.Error(), qt.Contains, `\x1b[31mfailed\x0d`)
}

func TestRun_ReportsActualStderrTail(t *testing.T) {
	c := qt.New(t)

	_, err := schemasource.Run(context.Background(), schemasource.Command{
		Args: helperArgs(),
		Env:  helperEnv("large-stderr-fail"),
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "FINAL_DIAGNOSTIC")
	c.Assert(err.Error(), qt.Not(qt.Contains), "EARLY_DIAGNOSTIC")
}
