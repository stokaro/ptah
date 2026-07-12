package onlineddl

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/dbschema/types"
)

// DirectiveTool is the per-migration directive key selecting the online-DDL
// tool, written as `-- +ptah online_ddl_tool=ghost` in a migration file.
const DirectiveTool = "online_ddl_tool"

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

// ValidateDirectives implements migrator.StatementInterceptor: it rejects an
// unknown online_ddl_tool directive value before any statement runs, so a
// typo fails the migration cleanly instead of aborting midway (after earlier,
// implicitly committed DDL) when the first ALTER is reached.
func (e *Executor) ValidateDirectives(directives map[string]string) error {
	tool, ok := directives[DirectiveTool]
	if !ok {
		return nil
	}
	switch tool {
	case ToolGhost, ToolPTOSC, DirectiveNone:
		return nil
	default:
		return fmt.Errorf("unknown %s directive value %q: expected %s, %s or %s",
			DirectiveTool, tool, ToolGhost, ToolPTOSC, DirectiveNone)
	}
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

	tool, err := e.pickTool(ctx, conn, target, directives)
	if err != nil || tool == "" {
		return false, err
	}

	binary := binaryFor(tool)
	if _, err := e.lookPath(binary); err != nil {
		// Not just ErrNotFound: a non-executable binary or a permission
		// error also lands here, so surface the underlying cause.
		e.logger.Warn("online-DDL tool unavailable on PATH; falling back to a plain ALTER TABLE",
			"tool", tool, "binary", binary, "table", target.Table, "error", err)
		return false, nil
	}

	dsn, err := ParseDatabaseURL(info.URL)
	if err != nil {
		return false, fmt.Errorf("cannot build %s invocation: %w", binary, err)
	}
	if target.Schema != "" {
		dsn.Database = target.Schema
	}

	args, err := buildArgs(tool, dsn, target, e.cfg.Args)
	if err != nil {
		return false, fmt.Errorf("cannot build %s invocation: %w", binary, err)
	}
	if e.dryRun {
		e.logger.Info("DRY RUN: would run online-DDL tool",
			"tool", binary, "table", target.Table, "alter", target.Clause)
		return true, nil
	}

	e.logger.Info("Running online-DDL tool", "tool", binary, "table", target.Table, "alter", target.Clause)
	if err := e.run(ctx, binary, args); err != nil {
		return false, fmt.Errorf("online-DDL tool %s failed for table %s: %w", binary, target.Table, err)
	}
	e.logger.Info("Online-DDL tool finished", "tool", binary, "table", target.Table)
	return true, nil
}

// pickTool decides which tool (if any) handles the statement: an explicit
// directive wins; otherwise the configured tool applies when the table's
// estimated row count reaches the threshold. Empty means "not routed".
func (e *Executor) pickTool(ctx context.Context, conn Conn, target AlterTarget, directives map[string]string) (string, error) {
	if directiveTool, ok := directives[DirectiveTool]; ok {
		if directiveTool != ToolGhost && directiveTool != ToolPTOSC {
			return "", fmt.Errorf("unknown %s directive value %q: expected %s, %s or %s",
				DirectiveTool, directiveTool, ToolGhost, ToolPTOSC, DirectiveNone)
		}
		return directiveTool, nil
	}

	if !e.cfg.Enabled() {
		return "", nil
	}
	rows, err := e.rowCount(ctx, conn, target.Schema, target.Table)
	if err != nil {
		// Fail open: the plain ALTER is the pre-feature behavior, and a
		// broken estimate must not block a migration.
		e.logger.Warn("online-DDL row-count check failed; executing a plain ALTER TABLE",
			"table", target.Table, "error", err)
		return "", nil
	}
	if rows < e.cfg.ThresholdRows {
		return "", nil
	}
	e.logger.Info("Routing ALTER TABLE through the online-DDL tool: table exceeds the row threshold",
		"table", target.Table, "rows", rows, "threshold", e.cfg.ThresholdRows, "tool", e.cfg.Tool)
	return e.cfg.Tool, nil
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
func buildArgs(tool string, dsn DSN, target AlterTarget, extra []string) ([]string, error) {
	switch tool {
	case ToolPTOSC:
		return buildPTOSCArgs(dsn, target, extra)
	default: // ToolGhost
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
		if dsn.Password != "" {
			args = append(args, "--password="+dsn.Password)
		}
		args = append(args, extra...)
		return append(args, "--execute"), nil
	}
}

// buildPTOSCArgs assembles the pt-online-schema-change invocation. Its DSN is
// a single comma-delimited argv element (h=,P=,u=,D=,t=[,p=]) that Percona's
// DSN parser splits on literal commas with no un-escaping, so a comma in any
// endpoint would silently smuggle extra DSN keys (host redirect, F= defaults
// file). Rather than emit a corrupt or injectable DSN, refuse to build it.
func buildPTOSCArgs(dsn DSN, target AlterTarget, extra []string) ([]string, error) {
	fields := map[string]string{
		"host": dsn.Host, "port": dsn.Port, "user": dsn.User,
		"database": dsn.Database, "table": target.Table, "password": dsn.Password,
	}
	for name, value := range fields {
		if strings.Contains(value, ",") {
			return nil, fmt.Errorf("pt-online-schema-change cannot receive a %s containing a comma (%q); "+
				"pass it via online_ddl.args (for example a --defaults-file) instead", name, value)
		}
	}

	spec := fmt.Sprintf("h=%s,P=%s,u=%s,D=%s,t=%s", dsn.Host, dsn.Port, dsn.User, dsn.Database, target.Table)
	if dsn.Password != "" {
		spec += ",p=" + dsn.Password
	}
	args := []string{"--alter", target.Clause}
	args = append(args, extra...)
	return append(args, "--execute", spec), nil
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
