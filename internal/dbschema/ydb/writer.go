package ydb

import (
	"context"
	"fmt"
	"log/slog"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Scheme_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/atlasretry"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/sqlrunner"
)

// Scheme is what the writer asks of YDB's scheme service: the entries of a
// directory, and the removal of an empty one. There is no SQL that drops a
// directory. Both take an absolute path.
type Scheme interface {
	ListDirectory(ctx context.Context, absolute string) ([]*Ydb_Scheme.Entry, error)
	RemoveDirectory(ctx context.Context, absolute string) error
}

// Writer applies schema changes to a YDB database.
//
// Each statement runs as a query of its own. A YDB query that carries several
// DDL statements is not atomic -- whatever ran before a failure stays applied
// -- and it compiles every statement against the schema as it stood before the
// query, so `ALTER TABLE t ADD COLUMN v` and `ALTER TABLE t ADD INDEX i ON (v)`
// fail as one query and succeed as two (measured on 26.2.1.14). Success is the
// driver's answer: YDB reports some successful statements with issue text that
// begins with `Error:`, and the driver returns no error for them.
//
// The transaction methods are no-ops. YDB refuses a scheme statement inside a
// transaction (`Scheme operations cannot be executed inside transaction`), so
// a transaction that claimed to hold DDL would promise an atomicity YDB does
// not provide.
type Writer struct {
	runner   sqlrunner.Runner
	scheme   Scheme
	database string
	dryRun   bool
	// pause waits before a retry, and is replaced in tests.
	pause func(context.Context, int) error
}

// NewWriter returns a writer that executes through runner and reaches the
// scheme service through driver.
func NewWriter(runner sqlrunner.Runner, driver *ydbsdk.Driver) *Writer {
	return NewWriterFromScheme(runner, grpcScheme{client: Ydb_Scheme_V1.NewSchemeServiceClient(ydbsdk.GRPCConn(driver))},
		driver.Name())
}

// NewWriterFromScheme returns a writer that executes through runner and asks
// scheme about database, an absolute path such as /local.
func NewWriterFromScheme(runner sqlrunner.Runner, scheme Scheme, database string) *Writer {
	return &Writer{
		runner:   runner,
		scheme:   scheme,
		database: "/" + strings.Trim(database, "/"),
		pause:    retryPause,
	}
}

// SetDryRun makes the writer log what it would execute instead.
func (w *Writer) SetDryRun(dryRun bool) { w.dryRun = dryRun }

// IsDryRun reports whether the writer only logs.
func (w *Writer) IsDryRun() bool { return w.dryRun }

// ExecuteSQL runs each statement of sqlExpr as a query of its own, in order,
// and stops at the first that fails. Arguments are passed to every statement,
// so a text with arguments holds one statement.
//
// A data statement -- INSERT, UPSERT, REPLACE, UPDATE or DELETE -- commits on
// its own and is run again when YDB aborts it for a conflicting transaction
// (`Transaction locks invalidated`), since an aborted statement changed
// nothing. A scheme statement is run once.
func (w *Writer) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	statements := sqlutil.SplitSQLStatementsForDialect(sqlExpr, platform.YDB)
	if len(statements) > 1 && len(args) > 0 {
		return fmt.Errorf("ydb: %d statements were given one argument list; pass one statement with its arguments",
			len(statements))
	}
	for _, statement := range statements {
		if err := w.execute(ctx, statement, args); err != nil {
			return err
		}
	}
	return nil
}

// execute runs one statement.
func (w *Writer) execute(ctx context.Context, statement string, args []any) error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would execute SQL", "sql", statement, "args", args)
		return nil
	}
	if w.runner == nil {
		return fmt.Errorf("no database connection")
	}
	attempts := 1
	if isDataStatement(statement) {
		attempts = maxDataAttempts
	}
	for attempt := range attempts {
		_, err := w.runner.ExecContext(ctx, statement, args...)
		if err == nil {
			return nil
		}
		if attempt == attempts-1 || !atlasretry.IsRetryable(err) {
			return fmt.Errorf("ydb: SQL execution failed: %w\nSQL: %s", err, statement)
		}
		if err := w.pause(ctx, attempt); err != nil {
			return err
		}
	}
	return nil
}

// maxDataAttempts bounds how often a data statement YDB aborted is run.
const maxDataAttempts = 5

// retryPause waits longer after each aborted attempt, and stops waiting when
// ctx ends.
func retryPause(ctx context.Context, attempt int) error {
	timer := time.NewTimer(time.Duration(attempt+1) * 50 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// dataStatements are the first keywords of a YQL statement that changes rows.
var dataStatements = []string{"INSERT", "UPSERT", "REPLACE", "UPDATE", "DELETE"}

// isDataStatement reports whether statement changes rows, by its first
// keyword, read with the YQL lexer so a leading comment is skipped.
func isDataStatement(statement string) bool {
	lexr := lexer.NewLexerWithOptions(statement, dialectlexer.Options(platform.YDB))
	for {
		token := lexr.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return false
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		case lexer.TokenIdentifier:
			return slices.Contains(dataStatements, strings.ToUpper(token.Value))
		default:
			return false
		}
	}
}

// BeginTransaction returns a transaction whose Commit and Rollback do nothing;
// see the Writer documentation.
func (w *Writer) BeginTransaction(context.Context) (catalog.SchemaTransaction, error) {
	if w.dryRun {
		slog.Info("[DRY RUN] Would begin transaction (a no-op for YDB DDL)")
	}
	return &transaction{writer: w}, nil
}

type transaction struct {
	writer *Writer
}

func (t *transaction) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	return t.writer.ExecuteSQL(ctx, sqlExpr, args...)
}

func (t *transaction) IsDryRun() bool { return t.writer.IsDryRun() }

// Commit is a no-op: every statement already took effect on its own.
func (t *transaction) Commit() error { return nil }

// Rollback is a no-op for the same reason. It reports no error, because the
// request is not wrong; YDB has nothing it could undo.
func (t *transaction) Rollback() error { return nil }

// DropAllTables drops every row table in the database and then removes each
// directory that dropping them left empty, deepest first.
//
// It drops what the schema reader describes and nothing else. A column table,
// a view, a topic and the other objects the reader records as not described
// stay, and so does the directory that holds one, so a cleanup planned from a
// read removes exactly what the plan listed. Dot-directories are never
// entered, and a directory that was empty before is left alone.
func (w *Writer) DropAllTables(ctx context.Context) error {
	if w.scheme == nil {
		return fmt.Errorf("no YDB scheme connection")
	}
	_, err := w.dropDirectory(ctx, "")
	return err
}

// dropDirectory drops the tables in the directory dir, relative to the
// database root, and the directories under it, and reports whether it dropped
// or removed anything there.
func (w *Writer) dropDirectory(ctx context.Context, dir string) (bool, error) {
	entries, err := w.scheme.ListDirectory(ctx, path.Join(w.database, dir))
	if err != nil {
		return false, err
	}
	slices.SortFunc(entries, func(a, b *Ydb_Scheme.Entry) int { return strings.Compare(a.GetName(), b.GetName()) })
	changed := false
	for _, entry := range entries {
		name := entry.GetName()
		switch {
		case entry.GetType() == Ydb_Scheme.Entry_TABLE:
			if err := w.ExecuteSQL(ctx, "DROP TABLE "+sqlident.Quote(platform.YDB, path.Join(dir, name))); err != nil {
				return changed, err
			}
			changed = true
		case entry.GetType() == Ydb_Scheme.Entry_DIRECTORY && !strings.HasPrefix(name, "."):
			child := path.Join(dir, name)
			childChanged, err := w.dropDirectory(ctx, child)
			if err != nil {
				return changed, err
			}
			if !childChanged {
				continue
			}
			changed = true
			if err := w.removeIfEmpty(ctx, child); err != nil {
				return changed, err
			}
		}
	}
	return changed, nil
}

// removeIfEmpty removes the directory dir when nothing is left in it.
func (w *Writer) removeIfEmpty(ctx context.Context, dir string) error {
	absolute := path.Join(w.database, dir)
	if w.dryRun {
		slog.Info("[DRY RUN] Would remove the directory if it is empty", "path", absolute)
		return nil
	}
	left, err := w.scheme.ListDirectory(ctx, absolute)
	if err != nil {
		return err
	}
	if len(left) > 0 {
		return nil
	}
	if err := w.scheme.RemoveDirectory(ctx, absolute); err != nil {
		return fmt.Errorf("ydb: remove directory %s: %w", absolute, err)
	}
	return nil
}

// grpcScheme answers through raw scheme service calls.
type grpcScheme struct {
	client Ydb_Scheme_V1.SchemeServiceClient
}

func (s grpcScheme) ListDirectory(ctx context.Context, absolute string) ([]*Ydb_Scheme.Entry, error) {
	return (&grpcSource{scheme: s.client}).ListDirectory(ctx, absolute)
}

func (s grpcScheme) RemoveDirectory(ctx context.Context, absolute string) error {
	response, err := s.client.RemoveDirectory(ctx, &Ydb_Scheme.RemoveDirectoryRequest{Path: absolute})
	if err != nil {
		return err
	}
	return operationStatus(response.GetOperation())
}
