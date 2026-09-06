package schema

// White-box testing required: the injected session runner is the boundary
// between the command's pool-backed connection and the one physical session
// that owns the apply lock. Public SQLite commands exercise the no-op lock
// path, but cannot distinguish using the outer pool from using the callback
// session. These tests redirect that callback to a second database so any
// accidental outer-connection inspection or DDL becomes observable.

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/cmd/internal/dbcli"
	"ptah.run/cmd/internal/exitcode"
	"ptah.run/dbschema"
)

type supportedApplyLock struct{}

func (supportedApplyLock) Supported() bool { return true }

type redirectApplySession struct {
	target    *dbschema.DatabaseConnection
	root      *dbschema.DatabaseConnection
	session   *dbschema.DatabaseConnection
	terminal  error
	callCount int
}

func (r *redirectApplySession) run(
	ctx context.Context,
	root *dbschema.DatabaseConnection,
	_ string,
	_ time.Duration,
	use func(*dbschema.DatabaseConnection, schemaApplyLock) error,
) (runErr, releaseErr error) {
	r.root = root
	r.callCount++
	callbackErr := r.target.WithSession(ctx, func(session *dbschema.DatabaseConnection) error {
		r.session = session
		return use(session, supportedApplyLock{})
	})
	return errors.Join(callbackErr, r.terminal), nil
}

func TestSchemaApplyNormalPathUsesLockCallbackSession(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	rootPath := filepath.Join(dir, "outer.db")
	targetPath := filepath.Join(dir, "session.db")
	seedApplySessionSQLite(c, rootPath, "CREATE TABLE root_only (id INTEGER PRIMARY KEY);")
	seedApplySessionSQLite(c, targetPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	desiredPath := writeApplySessionFile(c, dir, "schema.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")
	target := openApplySessionSQLite(c, targetPath)
	redirect := &redirectApplySession{target: target}
	cmd, output := newApplySessionCommand()

	err := runSchemaApplyWithLockSession(cmd, schemaApplyOptions{
		dbURL:          "sqlite://" + rootPath,
		schemaFiles:    []string{desiredPath},
		autoApprove:    true,
		connectTimeout: dbcli.DefaultConnectTimeout.String(),
	}, redirect.run)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", output.String()))
	c.Assert(redirect.callCount, qt.Equals, 1)
	c.Assert(redirect.root, qt.Not(qt.Equals), redirect.session)
	c.Assert(output.String(), qt.Contains, "Schema apply completed successfully.")
	c.Assert(listApplySessionSQLiteTables(c, rootPath), qt.DeepEquals, []string{"root_only"})
	c.Assert(listApplySessionSQLiteTables(c, targetPath), qt.DeepEquals, []string{"orders", "users"})
}

func TestSchemaApplyPlanFilePathUsesLockCallbackSession(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	rootPath := filepath.Join(dir, "outer.db")
	targetPath := filepath.Join(dir, "session.db")
	planPath := filepath.Join(dir, "add-orders.plan.json")
	seedApplySessionSQLite(c, rootPath, "CREATE TABLE root_only (id INTEGER PRIMARY KEY);")
	seedApplySessionSQLite(c, targetPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	desiredPath := writeApplySessionFile(c, dir, "schema.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")
	planOutput, planErr := runApplySessionCommand(
		"plan",
		"--db-url", "sqlite://"+targetPath,
		"--schema-file", desiredPath,
		"--output", planPath,
	)
	c.Assert(planErr, qt.IsNil, qt.Commentf("%s", planOutput))
	target := openApplySessionSQLite(c, targetPath)
	redirect := &redirectApplySession{target: target}
	cmd, output := newApplySessionCommand()

	err := runSchemaApplyWithLockSession(cmd, schemaApplyOptions{
		dbURL:          "sqlite://" + rootPath,
		planPath:       planPath,
		autoApprove:    true,
		connectTimeout: dbcli.DefaultConnectTimeout.String(),
	}, redirect.run)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", output.String()))
	c.Assert(redirect.callCount, qt.Equals, 1)
	c.Assert(redirect.root, qt.Not(qt.Equals), redirect.session)
	c.Assert(output.String(), qt.Contains, "Schema apply completed successfully.")
	c.Assert(listApplySessionSQLiteTables(c, rootPath), qt.DeepEquals, []string{"root_only"})
	c.Assert(listApplySessionSQLiteTables(c, targetPath), qt.DeepEquals, []string{"orders", "users"})
}

func TestSchemaApplyAcquisitionFailureKeepsDiagnosticAndExitCode(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	rootPath := filepath.Join(dir, "outer.db")
	seedApplySessionSQLite(c, rootPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	desiredPath := writeApplySessionFile(c, dir, "schema.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")
	cmd, output := newApplySessionCommand()
	var callbackCount int
	failBeforeSession := func(
		ctx context.Context,
		_ *dbschema.DatabaseConnection,
		name string,
		timeout time.Duration,
		use func(*dbschema.DatabaseConnection, schemaApplyLock) error,
	) (runErr, releaseErr error) {
		return withSchemaApplyLockSession(ctx, nil, name, timeout, func(
			session *dbschema.DatabaseConnection,
			lock schemaApplyLock,
		) error {
			callbackCount++
			return use(session, lock)
		})
	}

	err := runSchemaApplyWithLockSession(cmd, schemaApplyOptions{
		dbURL:          "sqlite://" + rootPath,
		schemaFiles:    []string{desiredPath},
		autoApprove:    true,
		connectTimeout: dbcli.DefaultConnectTimeout.String(),
	}, failBeforeSession)

	c.Assert(err, qt.ErrorMatches,
		"acquire schema apply lock: schema apply locking requires database connection")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(callbackCount, qt.Equals, 0)
	c.Assert(output.String(), qt.Equals,
		"error: acquire schema apply lock: schema apply locking requires database connection\n")
}

func TestSchemaApplyNormalPathDoesNotReportSuccessAfterSessionLoss(t *testing.T) {
	testSchemaApplySessionLoss(t, keepApplySessionNormalPath)
}

func TestSchemaApplyPlanFilePathDoesNotReportSuccessAfterSessionLoss(t *testing.T) {
	testSchemaApplySessionLoss(t, configureApplySessionPlan)
}

type applySessionLossSetup func(
	*qt.C,
	string,
	string,
	string,
	*schemaApplyOptions,
)

func testSchemaApplySessionLoss(t *testing.T, setup applySessionLossSetup) {
	t.Helper()
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	rootPath := filepath.Join(dir, "outer.db")
	targetPath := filepath.Join(dir, "session.db")
	planPath := filepath.Join(dir, "add-orders.plan.json")
	seedApplySessionSQLite(c, rootPath, "CREATE TABLE root_only (id INTEGER PRIMARY KEY);")
	seedApplySessionSQLite(c, targetPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	desiredPath := writeApplySessionFile(c, dir, "schema.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")
	opts := schemaApplyOptions{
		dbURL:          "sqlite://" + rootPath,
		schemaFiles:    []string{desiredPath},
		autoApprove:    true,
		connectTimeout: dbcli.DefaultConnectTimeout.String(),
	}
	setup(c, targetPath, desiredPath, planPath, &opts)
	target := openApplySessionSQLite(c, targetPath)
	sessionFailure := errors.New("advisory lock session failed during release: lock release failed")
	redirect := &redirectApplySession{target: target, terminal: sessionFailure}
	cmd, output := newApplySessionCommand()

	err := runSchemaApplyWithLockSession(cmd, opts, redirect.run)

	c.Assert(err, qt.ErrorIs, sessionFailure)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(output.String(), qt.Not(qt.Contains), "Schema apply completed successfully.")
	c.Assert(strings.Count(output.String(), "error: "+sessionFailure.Error()+"\n"), qt.Equals, 1)
	c.Assert(listApplySessionSQLiteTables(c, rootPath), qt.DeepEquals, []string{"root_only"})
	c.Assert(listApplySessionSQLiteTables(c, targetPath), qt.DeepEquals, []string{"orders", "users"})
}

func keepApplySessionNormalPath(
	_ *qt.C,
	_, _, _ string,
	_ *schemaApplyOptions,
) {
}

func configureApplySessionPlan(
	c *qt.C,
	targetPath,
	desiredPath,
	planPath string,
	opts *schemaApplyOptions,
) {
	c.Helper()
	output, err := runApplySessionCommand(
		"plan",
		"--db-url", "sqlite://"+targetPath,
		"--schema-file", desiredPath,
		"--output", planPath,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	opts.schemaFiles = nil
	opts.planPath = planPath
}

func newApplySessionCommand() (*cobra.Command, *bytes.Buffer) {
	cmd := newSchemaApplyCommand()
	cmd.SetContext(context.Background())
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetIn(strings.NewReader(""))
	return cmd, &output
}

func runApplySessionCommand(args ...string) (string, error) {
	cmd := NewSchemaCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetIn(strings.NewReader(""))
	cmd.SetArgs(args)
	err := cmd.Execute()
	return output.String(), err
}

func openApplySessionSQLite(c *qt.C, path string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+path)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

func seedApplySessionSQLite(c *qt.C, path, schemaSQL string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+path)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), schemaSQL)
	c.Assert(err, qt.IsNil)
}

func listApplySessionSQLiteTables(c *qt.C, path string) []string {
	c.Helper()
	conn := openApplySessionSQLite(c, path)
	rows, err := conn.QueryContext(c.Context(),
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = rows.Close() })
	var tables []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		tables = append(tables, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return tables
}

func writeApplySessionFile(c *qt.C, dir, name, content string) string {
	c.Helper()
	path := filepath.Join(dir, name)
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	return path
}
