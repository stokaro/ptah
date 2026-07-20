package onlineddl

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/dbschema/types"
)

// DirectiveTool is the per-migration directive key selecting the online-DDL
// tool, written as `-- +ptah online_ddl_tool=ghost` in a migration file.
const DirectiveTool = "online_ddl_tool"

// DirectiveFallback is the per-migration directive key selecting the fallback
// policy, written as `-- +ptah online_ddl_fallback=error`.
const DirectiveFallback = "online_ddl_fallback"

// DirectiveNone is the DirectiveTool value that opts a migration out of
// automatic threshold routing.
const DirectiveNone = "none"

// Tool binary names on PATH.
const (
	ghostBinary = "gh-ost"
	ptoscBinary = "pt-online-schema-change"
)

// Conn is the slice of dbschema.DatabaseConnection the executor consumes,
// narrow so tests can fake it.
type Conn interface {
	Info() types.DBInfo
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// CommandRunner executes an external tool invocation.
type CommandRunner func(ctx context.Context, binary string, args []string) error

type toolInvocation struct {
	args    []string
	cleanup func() error
}

type toolRoute struct {
	tool     string
	fallback string
}

// Executor routes ALTER TABLE migration statements through an online-DDL
// tool. It implements migrator.StatementInterceptor.
type Executor struct {
	cfg    Config
	dryRun bool
	logger *slog.Logger

	// Swappable seams for tests.
	run      CommandRunner
	lookPath func(file string) (string, error)
	rowCount func(ctx context.Context, conn Conn, schema, table string) (int64, error)
}

// New creates an executor for the given configuration. A zero Config is
// valid: automatic routing stays off, per-migration directives keep working.
func New(cfg Config) *Executor {
	return &Executor{
		cfg:      cfg,
		logger:   slog.Default(),
		run:      runCommand,
		lookPath: exec.LookPath,
		rowCount: tableRows,
	}
}

// WithLogger returns a copy of the executor using the given logger.
func (e *Executor) WithLogger(logger *slog.Logger) *Executor {
	tmp := *e
	tmp.logger = logger
	return &tmp
}

// WithDryRun returns a copy of the executor that logs the tool invocation it
// would perform instead of running it (the statement still counts as
// handled, mirroring the writer's dry-run treatment of plain statements).
func (e *Executor) WithDryRun(dryRun bool) *Executor {
	tmp := *e
	tmp.dryRun = dryRun
	return &tmp
}

// ValidateDirectives implements migrator.StatementInterceptor: it rejects
// unknown online-DDL directive values before any statement runs, so a typo
// fails the migration cleanly instead of aborting midway (after earlier,
// implicitly committed DDL) when the first ALTER is reached.
func (e *Executor) ValidateDirectives(directives map[string]string) error {
	if tool, ok := directives[DirectiveTool]; ok {
		switch tool {
		case ToolGhost, ToolPTOSC, DirectiveNone:
		default:
			return fmt.Errorf("unknown %s directive value %q: expected %s, %s or %s",
				DirectiveTool, tool, ToolGhost, ToolPTOSC, DirectiveNone)
		}
	}
	if fallback, ok := directives[DirectiveFallback]; ok {
		if err := validateFallbackDirective(fallback); err != nil {
			return err
		}
	}
	return nil
}

// ExecuteStatement implements migrator.StatementInterceptor: it returns
// handled=true when the statement was routed through (or, in dry-run mode,
// attributed to) an online-DDL tool, and handled=false when the migrator
// should execute the statement itself — including every fallback path.
func (e *Executor) ExecuteStatement(ctx context.Context, conn *dbschema.DatabaseConnection, stmt string, directives map[string]string) (bool, error) {
	return e.executeStatement(ctx, conn, stmt, directives)
}

func (e *Executor) executeStatement(ctx context.Context, conn Conn, stmt string, directives map[string]string) (bool, error) {
	directiveTool, hasDirective := directives[DirectiveTool]
	if hasDirective && directiveTool == DirectiveNone {
		return false, nil
	}

	info := conn.Info()
	if info.Dialect != "mysql" && info.Dialect != "mariadb" {
		if hasDirective {
			e.logger.Warn("online_ddl_tool directive ignored: online-DDL tools support only the MySQL family",
				"dialect", info.Dialect)
		}
		return false, nil
	}

	target, ok := ParseAlterTable(stmt)
	if !ok {
		return false, nil
	}

	route, err := e.pickTool(ctx, conn, target, directives)
	if err != nil || route.tool == "" {
		return false, err
	}

	binary := binaryFor(route.tool)
	if _, err := e.lookPath(binary); err != nil {
		// Not just ErrNotFound: a non-executable binary or a permission
		// error also lands here, so surface the underlying cause.
		if route.fallback == FallbackError {
			return false, fmt.Errorf("online-DDL tool %s unavailable for table %s: %w; no ALTER TABLE was applied",
				binary, target.Table, err)
		}
		e.logger.Warn("online-DDL tool unavailable on PATH; falling back to a plain ALTER TABLE",
			"tool", route.tool, "binary", binary, "table", target.Table, "error", err)
		return false, nil
	}

	dsn, err := ParseDatabaseURL(info.URL)
	if err != nil {
		return false, fmt.Errorf("cannot build %s invocation: %w", binary, err)
	}
	if target.Schema != "" {
		dsn.Database = target.Schema
	}

	if e.dryRun {
		if err := validateToolInvocation(route.tool, dsn, target); err != nil {
			return false, fmt.Errorf("cannot build %s invocation: %w", binary, err)
		}
		e.logger.Info("DRY RUN: would run online-DDL tool",
			"tool", binary, "table", target.Table, "alter", target.Clause)
		return true, nil
	}

	invocation, err := buildArgs(route.tool, dsn, target, e.cfg.Args)
	if err != nil {
		return false, fmt.Errorf("cannot build %s invocation: %w", binary, err)
	}
	if invocation.cleanup != nil {
		defer func() {
			if err := invocation.cleanup(); err != nil {
				e.logger.Warn("failed to remove online-DDL credential file",
					"tool", binary, "table", target.Table, "error", err)
			}
		}()
	}
	e.logger.Info("Running online-DDL tool", "tool", binary, "table", target.Table, "alter", target.Clause)
	if err := e.run(ctx, binary, invocation.args); err != nil {
		return false, fmt.Errorf("online-DDL tool %s failed for table %s: %w", binary, target.Table, err)
	}
	e.logger.Info("Online-DDL tool finished", "tool", binary, "table", target.Table)
	return true, nil
}

// pickTool decides which tool (if any) handles the statement: an explicit
// directive wins; otherwise the configured tool applies when the table's
// estimated row count reaches the threshold. Empty tool means "not routed".
func (e *Executor) pickTool(ctx context.Context, conn Conn, target AlterTarget, directives map[string]string) (toolRoute, error) {
	if directiveTool, ok := directives[DirectiveTool]; ok {
		if directiveTool != ToolGhost && directiveTool != ToolPTOSC {
			return toolRoute{}, fmt.Errorf("unknown %s directive value %q: expected %s, %s or %s",
				DirectiveTool, directiveTool, ToolGhost, ToolPTOSC, DirectiveNone)
		}
		fallback, err := e.fallbackPolicy(directives, FallbackError)
		if err != nil {
			return toolRoute{}, err
		}
		return toolRoute{tool: directiveTool, fallback: fallback}, nil
	}

	if !e.cfg.Enabled() {
		return toolRoute{}, nil
	}
	fallback, err := e.fallbackPolicy(directives, FallbackPlain)
	if err != nil {
		return toolRoute{}, err
	}
	rows, err := e.rowCount(ctx, conn, target.Schema, target.Table)
	if err != nil {
		if fallback == FallbackError {
			return toolRoute{}, fmt.Errorf("online-DDL row-count check failed for table %s: %w; no ALTER TABLE was applied",
				target.Table, err)
		}
		e.logger.Warn("online-DDL row-count check failed; executing a plain ALTER TABLE",
			"table", target.Table, "error", err)
		return toolRoute{}, nil
	}
	if rows < e.cfg.ThresholdRows {
		return toolRoute{}, nil
	}
	e.logger.Info("Routing ALTER TABLE through the online-DDL tool: table exceeds the row threshold",
		"table", target.Table, "rows", rows, "threshold", e.cfg.ThresholdRows, "tool", e.cfg.Tool)
	return toolRoute{tool: e.cfg.Tool, fallback: fallback}, nil
}

func (e *Executor) fallbackPolicy(directives map[string]string, defaultPolicy string) (string, error) {
	if fallback, ok := directives[DirectiveFallback]; ok {
		if err := validateFallbackDirective(fallback); err != nil {
			return "", err
		}
		return fallback, nil
	}
	if e.cfg.Fallback != "" {
		if err := validateConfigFallback(e.cfg.Fallback); err != nil {
			return "", err
		}
		return e.cfg.Fallback, nil
	}
	return defaultPolicy, nil
}

func validateFallbackDirective(value string) error {
	switch value {
	case FallbackError, FallbackPlain:
		return nil
	default:
		return fmt.Errorf("unknown %s directive value %q: expected %s or %s",
			DirectiveFallback, value, FallbackError, FallbackPlain)
	}
}

func validateConfigFallback(value string) error {
	switch value {
	case FallbackError, FallbackPlain:
		return nil
	default:
		return fmt.Errorf("unknown online_ddl fallback %q: expected %s or %s", value, FallbackError, FallbackPlain)
	}
}

// binaryFor maps a canonical tool name to its binary on PATH.
func binaryFor(tool string) string {
	if tool == ToolPTOSC {
		return ptoscBinary
	}
	return ghostBinary
}

// buildArgs assembles the tool invocation. User-configured args go before
// the final --execute so they can never be overridden by it.
func buildArgs(tool string, dsn DSN, target AlterTarget, extra []string) (toolInvocation, error) {
	switch tool {
	case ToolPTOSC:
		return buildPTOSCArgs(dsn, target, extra)
	default: // ToolGhost
		return buildGhostArgs(dsn, target, extra)
	}
}

func buildGhostArgs(dsn DSN, target AlterTarget, extra []string) (toolInvocation, error) {
	// gh-ost takes each endpoint as its own --flag=value argv element, so
	// values are passed literally with no delimiter to escape.
	args := []string{
		"--host=" + dsn.Host,
		"--port=" + dsn.Port,
		"--user=" + dsn.User,
		"--database=" + dsn.Database,
		"--table=" + target.Table,
		"--alter=" + target.Clause,
	}
	cleanup, err := maybeAppendGhostCredential(&args, dsn, extra)
	if err != nil {
		return toolInvocation{}, err
	}
	args = append(args, extra...)
	return toolInvocation{args: append(args, "--execute"), cleanup: cleanup}, nil
}

func maybeAppendGhostCredential(args *[]string, dsn DSN, extra []string) (func() error, error) {
	if dsn.Password == "" || ghostArgsCarryCredentials(extra) {
		return nil, nil
	}
	path, cleanup, err := createMySQLDefaultsFile(dsn)
	if err != nil {
		return nil, err
	}
	*args = append(*args, "--conf="+path)
	return cleanup, nil
}

// buildPTOSCArgs assembles the pt-online-schema-change invocation. Its DSN is
// a single comma-delimited argv element (h=,P=,u=,D=,t=[,F=]) that Percona's
// DSN parser splits on literal commas with no un-escaping, so a comma in any
// endpoint would silently smuggle extra DSN keys (host redirect, F= defaults
// file). Rather than emit a corrupt or injectable DSN, refuse to build it.
func buildPTOSCArgs(dsn DSN, target AlterTarget, extra []string) (toolInvocation, error) {
	if err := validatePTOSCFields(dsn, target); err != nil {
		return toolInvocation{}, err
	}

	spec := fmt.Sprintf("h=%s,P=%s,u=%s,D=%s,t=%s", dsn.Host, dsn.Port, dsn.User, dsn.Database, target.Table)
	cleanup, err := maybeAppendPTOSCCredential(&spec, dsn, extra)
	if err != nil {
		return toolInvocation{}, err
	}
	args := []string{"--alter", target.Clause}
	args = append(args, extra...)
	return toolInvocation{args: append(args, "--execute", spec), cleanup: cleanup}, nil
}

func validateToolInvocation(tool string, dsn DSN, target AlterTarget) error {
	if tool != ToolPTOSC {
		return nil
	}
	return validatePTOSCFields(dsn, target)
}

func validatePTOSCFields(dsn DSN, target AlterTarget) error {
	fields := map[string]string{
		"host": dsn.Host, "port": dsn.Port, "user": dsn.User,
		"database": dsn.Database, "table": target.Table,
	}
	for name, value := range fields {
		if strings.Contains(value, ",") {
			return fmt.Errorf("pt-online-schema-change cannot receive a %s containing a comma (%q); "+
				"pass it via online_ddl.args (for example a --defaults-file) instead", name, value)
		}
	}
	return nil
}

func maybeAppendPTOSCCredential(spec *string, dsn DSN, extra []string) (func() error, error) {
	if dsn.Password == "" || ptoscArgsCarryCredentials(extra) {
		return nil, nil
	}
	path, cleanup, err := createMySQLDefaultsFile(dsn)
	if err != nil {
		return nil, err
	}
	if strings.Contains(path, ",") {
		if cleanupErr := cleanup(); cleanupErr != nil {
			return nil, fmt.Errorf("online-DDL credential file path contains a comma and cleanup failed: %w", cleanupErr)
		}
		return nil, fmt.Errorf("online-DDL credential file path contains a comma; set online_ddl.args with --defaults-file instead")
	}
	*spec += ",F=" + path
	return cleanup, nil
}

func ghostArgsCarryCredentials(args []string) bool {
	return hasFlagArg(args, "--conf") || hasFlagArg(args, "--password")
}

func ptoscArgsCarryCredentials(args []string) bool {
	if hasFlagArg(args, "--defaults-file") || hasFlagArg(args, "--defaults-extra-file") {
		return true
	}
	for _, arg := range args {
		if dsnArgContainsKey(arg, "p") || dsnArgContainsKey(arg, "F") {
			return true
		}
	}
	return false
}

func hasFlagArg(args []string, flag string) bool {
	for _, arg := range args {
		if arg == flag || strings.HasPrefix(arg, flag+"=") {
			return true
		}
	}
	return false
}

func dsnArgContainsKey(arg, key string) bool {
	for part := range strings.SplitSeq(arg, ",") {
		name, _, ok := strings.Cut(part, "=")
		if ok && name == key {
			return true
		}
	}
	return false
}

func createMySQLDefaultsFile(dsn DSN) (string, func() error, error) {
	content, err := mysqlDefaultsFileContent(dsn)
	if err != nil {
		return "", nil, err
	}
	file, err := os.CreateTemp("", "ptah-online-ddl-*.cnf")
	if err != nil {
		return "", nil, fmt.Errorf("create online-DDL credential file: %w", err)
	}
	path := file.Name()
	cleanup := func() error {
		return os.Remove(path)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = file.Close()
		_ = cleanup()
		return "", nil, fmt.Errorf("secure online-DDL credential file %s: %w", filepath.Base(path), err)
	}
	if _, err := file.WriteString(content); err != nil {
		_ = file.Close()
		_ = cleanup()
		return "", nil, fmt.Errorf("write online-DDL credential file %s: %w", filepath.Base(path), err)
	}
	if err := file.Close(); err != nil {
		_ = cleanup()
		return "", nil, fmt.Errorf("close online-DDL credential file %s: %w", filepath.Base(path), err)
	}
	return path, cleanup, nil
}

func mysqlDefaultsFileContent(dsn DSN) (string, error) {
	user, err := mysqlOptionValue(dsn.User)
	if err != nil {
		return "", fmt.Errorf("invalid MySQL user for online-DDL credential file: %w", err)
	}
	password, err := mysqlOptionValue(dsn.Password)
	if err != nil {
		return "", fmt.Errorf("invalid MySQL password for online-DDL credential file: %w", err)
	}
	return "[client]\nuser=" + user + "\npassword=" + password + "\n", nil
}

func mysqlOptionValue(value string) (string, error) {
	if strings.ContainsAny(value, "\x00\r\n") {
		return "", fmt.Errorf("value contains a control character")
	}
	escaped := strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(value)
	return `"` + escaped + `"`, nil
}

// runCommand is the production CommandRunner: it streams the tool's output
// to the migrator's stdout/stderr so progress is visible.
func runCommand(ctx context.Context, binary string, args []string) error {
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// tableRows estimates a table's row count from information_schema. The
// estimate is approximate on InnoDB, which is fine for a routing threshold.
func tableRows(ctx context.Context, conn Conn, schema, table string) (int64, error) {
	const query = "SELECT COALESCE(TABLE_ROWS, 0) FROM information_schema.TABLES " +
		"WHERE TABLE_SCHEMA = COALESCE(NULLIF(?, ''), DATABASE()) AND TABLE_NAME = ?"
	var rows int64
	if err := conn.QueryRowContext(ctx, query, schema, table).Scan(&rows); err != nil {
		return 0, err
	}
	return rows, nil
}
