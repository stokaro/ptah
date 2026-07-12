package onlineddl

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema/types"
)

// fakeConn satisfies Conn without a database. QueryRowContext must never be
// reached: tests stub the executor's rowCount seam instead.
type fakeConn struct {
	info types.DBInfo
}

func (f fakeConn) Info() types.DBInfo { return f.info }

func (f fakeConn) QueryRowContext(_ context.Context, _ string, _ ...any) *sql.Row {
	panic("QueryRowContext must not be called in unit tests")
}

func mysqlConn() fakeConn {
	//nolint:gosec // fixture URL with a made-up password, not a credential
	return fakeConn{info: types.DBInfo{Dialect: "mysql", URL: "mysql://app:secret@db.internal:3307/shop"}}
}

// testExecutor returns an executor with recording seams: *ran captures the
// invocation, lookPathErr simulates a missing binary, rows/rowsErr stub the
// row-count estimate.
type invocation struct {
	binary string
	args   []string
}

func testExecutor(cfg Config, ran *[]invocation, lookPathErr error, rows int64, rowsErr error) *Executor {
	e := New(cfg)
	e.run = func(_ context.Context, binary string, args []string) error {
		*ran = append(*ran, invocation{binary: binary, args: args})
		return nil
	}
	e.lookPath = func(file string) (string, error) {
		if lookPathErr != nil {
			return "", lookPathErr
		}
		return "/usr/local/bin/" + file, nil
	}
	e.rowCount = func(_ context.Context, _ Conn, _, _ string) (int64, error) {
		return rows, rowsErr
	}
	return e
}

func TestExecuteStatement_DirectiveRoutesThroughGhost(t *testing.T) {
	c := qt.New(t)

	var ran []invocation
	e := testExecutor(Config{}, &ran, nil, 0, nil)

	handled, err := e.executeStatement(context.Background(), mysqlConn(),
		"ALTER TABLE users ADD COLUMN bio TEXT",
		map[string]string{DirectiveTool: ToolGhost})

	c.Assert(err, qt.IsNil)
	c.Assert(handled, qt.IsTrue)
	c.Assert(ran, qt.HasLen, 1)
	c.Assert(ran[0].binary, qt.Equals, "gh-ost")
	c.Assert(ran[0].args, qt.DeepEquals, []string{
		"--host=db.internal",
		"--port=3307",
		"--user=app",
		"--database=shop",
		"--table=users",
		"--alter=ADD COLUMN bio TEXT",
		"--password=secret",
		"--execute",
	})
}

func TestExecuteStatement_GhostAppendsConfigArgsBeforeExecute(t *testing.T) {
	c := qt.New(t)

	var ran []invocation
	e := testExecutor(Config{Args: []string{"--allow-on-master", "--max-load=Threads_running=25"}}, &ran, nil, 0, nil)

	handled, err := e.executeStatement(context.Background(), mysqlConn(),
		"ALTER TABLE users ADD COLUMN bio TEXT",
		map[string]string{DirectiveTool: ToolGhost})

	c.Assert(err, qt.IsNil)
	c.Assert(handled, qt.IsTrue)
	c.Assert(ran[0].args, qt.DeepEquals, []string{
		"--host=db.internal",
		"--port=3307",
		"--user=app",
		"--database=shop",
		"--table=users",
		"--alter=ADD COLUMN bio TEXT",
		"--password=secret",
		"--allow-on-master",
		"--max-load=Threads_running=25",
		"--execute",
	})
}

func TestExecuteStatement_DirectiveWinsOverThreshold(t *testing.T) {
	c := qt.New(t)

	// Config would route through pt-osc on a huge table, but the migration's
	// explicit ghost directive must win, and the row count must not even be
	// consulted.
	var ran []invocation
	e := testExecutor(Config{Tool: ToolPTOSC, ThresholdRows: 10}, &ran, nil, 0, nil)
	e.rowCount = func(context.Context, Conn, string, string) (int64, error) {
		c.Fatal("row count must not be consulted when a directive is present")
		return 0, nil
	}

	handled, err := e.executeStatement(context.Background(), mysqlConn(),
		"ALTER TABLE users ADD COLUMN bio TEXT",
		map[string]string{DirectiveTool: ToolGhost})

	c.Assert(err, qt.IsNil)
	c.Assert(handled, qt.IsTrue)
	c.Assert(ran, qt.HasLen, 1)
	c.Assert(ran[0].binary, qt.Equals, "gh-ost")
}

func TestExecuteStatement_PTOSCRejectsCommaInIdentifier(t *testing.T) {
	c := qt.New(t)

	var ran []invocation
	e := testExecutor(Config{}, &ran, nil, 0, nil)

	// A backtick-quoted table name with a comma would smuggle extra DSN keys
	// into pt-osc's comma-delimited --execute spec; refuse instead.
	_, err := e.executeStatement(context.Background(), mysqlConn(),
		"ALTER TABLE `evil,u=attacker,F=/tmp/x.cnf` ADD COLUMN a INT",
		map[string]string{DirectiveTool: ToolPTOSC})

	c.Assert(err, qt.ErrorMatches, ".*cannot receive a table containing a comma.*")
	c.Assert(ran, qt.HasLen, 0)
}

func TestExecuteStatement_PTOSCRejectsCommaInPassword(t *testing.T) {
	c := qt.New(t)

	var ran []invocation
	e := testExecutor(Config{}, &ran, nil, 0, nil)
	conn := fakeConn{info: types.DBInfo{Dialect: "mysql", URL: "mysql://app:se,cret@db:3306/shop"}}

	_, err := e.executeStatement(context.Background(), conn,
		"ALTER TABLE users ADD COLUMN a INT",
		map[string]string{DirectiveTool: ToolPTOSC})

	c.Assert(err, qt.ErrorMatches, ".*cannot receive a password containing a comma.*")
	c.Assert(ran, qt.HasLen, 0)
}

func TestValidateDirectives(t *testing.T) {
	c := qt.New(t)

	e := New(Config{})
	c.Assert(e.ValidateDirectives(nil), qt.IsNil)
	c.Assert(e.ValidateDirectives(map[string]string{DirectiveTool: ToolGhost}), qt.IsNil)
	c.Assert(e.ValidateDirectives(map[string]string{DirectiveTool: ToolPTOSC}), qt.IsNil)
	c.Assert(e.ValidateDirectives(map[string]string{DirectiveTool: DirectiveNone}), qt.IsNil)
	c.Assert(e.ValidateDirectives(map[string]string{"unrelated": "x"}), qt.IsNil)
	c.Assert(e.ValidateDirectives(map[string]string{DirectiveTool: "goste"}),
		qt.ErrorMatches, `unknown online_ddl_tool directive value "goste".*`)
}

func TestExecuteStatement_FallbackPathsWarn(t *testing.T) {
	c := qt.New(t)

	// Missing binary warns then falls through.
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	var ran []invocation
	e := testExecutor(Config{}, &ran, errors.New("not found"), 0, nil).WithLogger(logger)
	handled, err := e.executeStatement(context.Background(), mysqlConn(),
		"ALTER TABLE users ADD COLUMN a INT", map[string]string{DirectiveTool: ToolGhost})
	c.Assert(err, qt.IsNil)
	c.Assert(handled, qt.IsFalse)
	c.Assert(buf.String(), qt.Contains, "unavailable on PATH")
	c.Assert(buf.String(), qt.Contains, "not found", qt.Commentf("the underlying lookPath error is surfaced"))

	// Row-count estimate failure warns then falls through.
	buf.Reset()
	e = testExecutor(Config{Tool: ToolGhost, ThresholdRows: 1}, &ran, nil, 0, errors.New("boom")).WithLogger(logger)
	handled, err = e.executeStatement(context.Background(), mysqlConn(),
		"ALTER TABLE users ADD COLUMN a INT", nil)
	c.Assert(err, qt.IsNil)
	c.Assert(handled, qt.IsFalse)
	c.Assert(buf.String(), qt.Contains, "row-count check failed")
}

func TestExecuteStatement_DirectiveRoutesThroughPTOSC(t *testing.T) {
	c := qt.New(t)

	var ran []invocation
	e := testExecutor(Config{Args: []string{"--max-lag=5"}}, &ran, nil, 0, nil)

	handled, err := e.executeStatement(context.Background(), mysqlConn(),
		"ALTER TABLE users MODIFY COLUMN bio VARCHAR(500)",
		map[string]string{DirectiveTool: ToolPTOSC})

	c.Assert(err, qt.IsNil)
	c.Assert(handled, qt.IsTrue)
	c.Assert(ran, qt.HasLen, 1)
	c.Assert(ran[0].binary, qt.Equals, "pt-online-schema-change")
	c.Assert(ran[0].args, qt.DeepEquals, []string{
		"--alter", "MODIFY COLUMN bio VARCHAR(500)",
		"--max-lag=5",
		"--execute", "h=db.internal,P=3307,u=app,D=shop,t=users,p=secret",
	})
}

func TestExecuteStatement_DirectiveNoneOptsOutOfAutoRouting(t *testing.T) {
	c := qt.New(t)

	var ran []invocation
	e := testExecutor(Config{Tool: ToolGhost, ThresholdRows: 10}, &ran, nil, 1_000_000, nil)

	handled, err := e.executeStatement(context.Background(), mysqlConn(),
		"ALTER TABLE users ADD COLUMN bio TEXT",
		map[string]string{DirectiveTool: DirectiveNone})

	c.Assert(err, qt.IsNil)
	c.Assert(handled, qt.IsFalse)
	c.Assert(ran, qt.HasLen, 0)
}

func TestExecuteStatement_UnknownDirectiveValueIsAnError(t *testing.T) {
	c := qt.New(t)

	var ran []invocation
	e := testExecutor(Config{}, &ran, nil, 0, nil)

	_, err := e.executeStatement(context.Background(), mysqlConn(),
		"ALTER TABLE users ADD COLUMN bio TEXT",
		map[string]string{DirectiveTool: "goose"})

	c.Assert(err, qt.ErrorMatches, `unknown online_ddl_tool directive value "goose".*`)
	c.Assert(ran, qt.HasLen, 0)
}

func TestExecuteStatement_ThresholdAutoRouting(t *testing.T) {
	c := qt.New(t)

	cfg := Config{Tool: ToolGhost, ThresholdRows: 1000}

	// At/above the threshold: routed.
	var ran []invocation
	e := testExecutor(cfg, &ran, nil, 1000, nil)
	handled, err := e.executeStatement(context.Background(), mysqlConn(),
		"ALTER TABLE users ADD COLUMN bio TEXT", nil)
	c.Assert(err, qt.IsNil)
	c.Assert(handled, qt.IsTrue)
	c.Assert(ran, qt.HasLen, 1)

	// Below the threshold: plain ALTER.
	ran = nil
	e = testExecutor(cfg, &ran, nil, 999, nil)
	handled, err = e.executeStatement(context.Background(), mysqlConn(),
		"ALTER TABLE users ADD COLUMN bio TEXT", nil)
	c.Assert(err, qt.IsNil)
	c.Assert(handled, qt.IsFalse)
	c.Assert(ran, qt.HasLen, 0)

	// Row-count estimate broken: fail open to the plain ALTER.
	ran = nil
	e = testExecutor(cfg, &ran, nil, 0, errors.New("information_schema unavailable"))
	handled, err = e.executeStatement(context.Background(), mysqlConn(),
		"ALTER TABLE users ADD COLUMN bio TEXT", nil)
	c.Assert(err, qt.IsNil)
	c.Assert(handled, qt.IsFalse)
	c.Assert(ran, qt.HasLen, 0)
}

func TestExecuteStatement_NonAlterAndNonMySQLPassThrough(t *testing.T) {
	c := qt.New(t)

	var ran []invocation
	e := testExecutor(Config{Tool: ToolGhost, ThresholdRows: 1}, &ran, nil, 1_000_000, nil)

	// Non-ALTER statements never route.
	handled, err := e.executeStatement(context.Background(), mysqlConn(),
		"CREATE TABLE t (id INT)", nil)
	c.Assert(err, qt.IsNil)
	c.Assert(handled, qt.IsFalse)

	// Non-MySQL dialects never route, even with an explicit directive.
	pg := fakeConn{info: types.DBInfo{Dialect: "postgres", URL: "postgres://app@db/shop"}}
	handled, err = e.executeStatement(context.Background(), pg,
		"ALTER TABLE users ADD COLUMN bio TEXT",
		map[string]string{DirectiveTool: ToolGhost})
	c.Assert(err, qt.IsNil)
	c.Assert(handled, qt.IsFalse)

	c.Assert(ran, qt.HasLen, 0)
}

func TestExecuteStatement_MissingBinaryFallsBackToPlainAlter(t *testing.T) {
	c := qt.New(t)

	var ran []invocation
	e := testExecutor(Config{}, &ran, errors.New("executable file not found in $PATH"), 0, nil)

	handled, err := e.executeStatement(context.Background(), mysqlConn(),
		"ALTER TABLE users ADD COLUMN bio TEXT",
		map[string]string{DirectiveTool: ToolGhost})

	c.Assert(err, qt.IsNil)
	c.Assert(handled, qt.IsFalse, qt.Commentf("the migrator must execute the plain ALTER instead"))
	c.Assert(ran, qt.HasLen, 0)
}

func TestExecuteStatement_SchemaQualifiedTableOverridesDatabase(t *testing.T) {
	c := qt.New(t)

	var ran []invocation
	e := testExecutor(Config{}, &ran, nil, 0, nil)

	handled, err := e.executeStatement(context.Background(), mysqlConn(),
		"ALTER TABLE analytics.events ADD COLUMN a INT",
		map[string]string{DirectiveTool: ToolGhost})

	c.Assert(err, qt.IsNil)
	c.Assert(handled, qt.IsTrue)
	c.Assert(ran[0].args, qt.Contains, "--database=analytics")
	c.Assert(ran[0].args, qt.Contains, "--table=events")
}

func TestExecuteStatement_ToolFailureAbortsMigration(t *testing.T) {
	c := qt.New(t)

	e := New(Config{})
	e.lookPath = func(string) (string, error) { return "/bin/gh-ost", nil }
	e.run = func(context.Context, string, []string) error { return errors.New("exit status 1") }

	_, err := e.executeStatement(context.Background(), mysqlConn(),
		"ALTER TABLE users ADD COLUMN bio TEXT",
		map[string]string{DirectiveTool: ToolGhost})

	c.Assert(err, qt.ErrorMatches, "online-DDL tool gh-ost failed for table users: exit status 1")
}

func TestExecuteStatement_DryRunHandlesWithoutRunning(t *testing.T) {
	c := qt.New(t)

	var ran []invocation
	e := testExecutor(Config{}, &ran, nil, 0, nil).WithDryRun(true)

	handled, err := e.executeStatement(context.Background(), mysqlConn(),
		"ALTER TABLE users ADD COLUMN bio TEXT",
		map[string]string{DirectiveTool: ToolGhost})

	c.Assert(err, qt.IsNil)
	c.Assert(handled, qt.IsTrue, qt.Commentf("dry-run must not fall through to the writer either"))
	c.Assert(ran, qt.HasLen, 0)
}

func TestExecuteStatement_EmptyPasswordOmitsPasswordArg(t *testing.T) {
	c := qt.New(t)

	var ran []invocation
	e := testExecutor(Config{}, &ran, nil, 0, nil)
	conn := fakeConn{info: types.DBInfo{Dialect: "mariadb", URL: "mariadb://root@localhost:3306/shop"}}

	handled, err := e.executeStatement(context.Background(), conn,
		"ALTER TABLE users ADD COLUMN bio TEXT",
		map[string]string{DirectiveTool: ToolGhost})

	c.Assert(err, qt.IsNil)
	c.Assert(handled, qt.IsTrue)
	for _, arg := range ran[0].args {
		c.Assert(strings.HasPrefix(arg, "--password"), qt.IsFalse)
	}
}

// TestExecuteStatement_RunsRealFakeBinary exercises the production LookPath
// and CommandRunner against a fake gh-ost on PATH that records its argv.
func TestExecuteStatement_RunsRealFakeBinary(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	argsFile := filepath.Join(dir, "args.txt")
	// Quote the redirection target: t.TempDir() paths can contain spaces.
	script := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$@\" > \"%s\"\n", argsFile)
	binPath := filepath.Join(dir, "gh-ost")
	c.Assert(os.WriteFile(binPath, []byte(script), 0o600), qt.IsNil)
	c.Assert(os.Chmod(binPath, 0o700), qt.IsNil)
	t.Setenv("PATH", dir)

	e := New(Config{})
	handled, err := e.executeStatement(context.Background(), mysqlConn(),
		"ALTER TABLE users ADD COLUMN bio TEXT",
		map[string]string{DirectiveTool: ToolGhost})

	c.Assert(err, qt.IsNil)
	c.Assert(handled, qt.IsTrue)

	recorded, err := os.ReadFile(argsFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(recorded), qt.Equals,
		"--host=db.internal\n--port=3307\n--user=app\n--database=shop\n--table=users\n--alter=ADD COLUMN bio TEXT\n--password=secret\n--execute\n")
}
