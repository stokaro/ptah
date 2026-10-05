package ydb

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Operation_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Scheme_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/atlasretry"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/sqlrunner"
	"ptah.run/internal/ydbcoordination"
	"ptah.run/internal/ydbflags"
	"ptah.run/internal/ydburl"
	"ptah.run/internal/yqlquery"
)

// Scheme is what the writer asks of YDB's scheme service: the entries of a
// directory, the creation of a directory and the removal of an empty one, and
// the description of one path. There is no SQL that creates or drops a
// directory. Each takes an absolute path.
type Scheme interface {
	ListDirectory(ctx context.Context, absolute string) ([]*Ydb_Scheme.Entry, error)
	MakeDirectory(ctx context.Context, absolute string) error
	RemoveDirectory(ctx context.Context, absolute string) error
	DescribePath(ctx context.Context, absolute string) (*Ydb_Scheme.Entry, error)
}

// Writer applies schema changes to a YDB database.
//
// Each scheme statement runs as a query of its own. A YDB query that carries
// several DDL statements is not atomic -- whatever ran before a failure stays
// applied -- and it compiles every statement against the schema as it stood
// before the query, so `ALTER TABLE t ADD COLUMN v` and `ALTER TABLE t ADD
// INDEX i ON (v)` fail as one query and succeed as two (measured on
// 26.2.1.14). Data statements run together; see [Writer.ExecuteSQL]. Success is the
// driver's answer: YDB reports some successful statements with issue text that
// begins with `Error:`, and the driver returns no error for them.
//
// The transaction methods are no-ops. YDB refuses a scheme statement inside a
// transaction (`Scheme operations cannot be executed inside transaction`), so
// a transaction that claimed to hold DDL would promise an atomicity YDB does
// not provide.
type Writer struct {
	runner sqlrunner.Runner
	scheme Scheme
	// database is the absolute path of the database, and root the absolute
	// path the writer treats as its database: the database itself, or the
	// directory of the dev realm the connection named.
	database string
	root     string
	dryRun   bool
	// builds reaches the operation service, which cancels a build; nil for a
	// writer made over a scheme alone.
	builds *Builds
	// pause waits before a retry, and is replaced in tests.
	pause func(context.Context, int) error
}

// NewWriter returns a writer that executes through runner and reaches the
// scheme and operation services through driver. realm is the dev realm the
// connection's URL named, or "" for none: the writer then treats the realm's
// directory as its database, as runner does (see ydburl.RealmParameter).
func NewWriter(runner sqlrunner.Runner, driver *ydbsdk.Driver, realm string) *Writer {
	connection := ydbsdk.GRPCConn(driver)
	writer := NewWriterFromScheme(runner, grpcScheme{client: Ydb_Scheme_V1.NewSchemeServiceClient(connection)},
		driver.Name(), realm)
	// A statement names a table relative to the root its connection resolves
	// names against, so a build is found under the root.
	writer.builds = NewBuilds(Ydb_Operation_V1.NewOperationServiceClient(connection), writer.root)
	return writer
}

// CancelRunningBuild cancels the build running on table and reports how it
// ended; see [Builds.CancelRunning]. A writer made by [NewWriterFromScheme]
// reaches no operation service and answers with an error.
func (w *Writer) CancelRunningBuild(ctx context.Context, table string, wait BuildWait) (BuildOutcome, error) {
	if w.builds == nil {
		return BuildUnsettled, errors.New("this YDB writer reaches no operation service to cancel a build through")
	}
	return w.builds.CancelRunning(ctx, table, wait)
}

// NewWriterFromScheme returns a writer that executes through runner and asks
// scheme about database, an absolute path such as /local, treating the dev
// realm realm in it as its database, or the database itself when realm is "".
func NewWriterFromScheme(runner sqlrunner.Runner, scheme Scheme, database, realm string) *Writer {
	database = "/" + strings.Trim(database, "/")
	root := database
	if realm != "" {
		root = path.Join(database, ydburl.RealmDirectory, realm)
	}
	return &Writer{
		runner:   runner,
		scheme:   scheme,
		database: database,
		root:     root,
		pause:    retryPause,
	}
}

// SetDryRun makes the writer log what it would execute instead.
func (w *Writer) SetDryRun(dryRun bool) { w.dryRun = dryRun }

// IsDryRun reports whether the writer only logs.
func (w *Writer) IsDryRun() bool { return w.dryRun }

// ExecuteSQL runs sqlExpr as the queries internal/yqlquery splits it into, in
// order, and stops at the first that fails. Each scheme statement is a query
// of its own; consecutive data statements are one query, so a named
// expression or an action they define reaches the statements that use it.
// Arguments are passed to every query, so a text with arguments runs as one.
//
// A data query -- INSERT, UPSERT, REPLACE, UPDATE, DELETE and the rest --
// commits on its own and is run again when YDB aborts it for a conflicting
// transaction (`Transaction locks invalidated`), since an aborted query
// changed nothing. A scheme query is run once.
func (w *Writer) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	queries, err := yqlquery.Split(sqlExpr)
	if err != nil {
		return fmt.Errorf("ydb: %w", err)
	}
	if len(queries) > 1 && len(args) > 0 {
		return fmt.Errorf("ydb: %d queries were given one argument list; pass one query with its arguments",
			len(queries))
	}
	for _, query := range queries {
		if err := w.execute(ctx, query, args); err != nil {
			return err
		}
	}
	return nil
}

// execute runs one query.
func (w *Writer) execute(ctx context.Context, query yqlquery.Query, args []any) error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would execute SQL", "sql", query.Text, "args", args)
		return nil
	}
	if w.runner == nil {
		return fmt.Errorf("no database connection")
	}
	attempts := 1
	if query.Kind == yqlquery.Data {
		attempts = maxDataAttempts
	}
	for attempt := range attempts {
		_, err := w.runner.ExecContext(ctx, query.Text, args...)
		if err == nil {
			return nil
		}
		if attempt == attempts-1 || !atlasretry.IsRetryable(err) {
			return executionError(err, query.Text)
		}
		if err := w.pause(ctx, attempt); err != nil {
			return err
		}
	}
	return nil
}

// executionError names the statement a failure belongs to. Where the server's
// refusal says a feature flag is off, the error also names the capability the
// flag decides, because the plan that sent the statement took it from the
// release line's preset, and this cluster runs with the flag off.
func executionError(err error, statement string) error {
	if gate, off := ydbflags.Refused(err.Error()); off {
		err = &ptaherr.CapabilityError{
			Dialect: platform.YDB,
			Feature: string(gate.Key),
			Err:     fmt.Errorf("%w: %w", ptaherr.ErrUnsupportedFeature, err),
			Message: fmt.Sprintf("capability %s is off on this YDB cluster, which runs with feature flag %s off: %v. "+
				"Turn the flag on, or name the cluster's monitoring endpoint in the URL "+
				"(monitoring=http://host:8765) so Ptah reads the flags before it plans",
				gate.Key, gate.Flag, err),
		}
	}
	return fmt.Errorf("ydb: SQL execution failed: %w\nSQL: %s", err, statement)
}

// maxDataAttempts bounds how often a data query YDB aborted is run.
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

// DropAllTables drops every view, row table, topic and coordination node in
// the database and then removes each directory that dropping them left empty,
// deepest first.
//
// It drops what the schema reader describes and nothing else. A column table
// and the other objects the reader records as not described stay, and so does
// the directory that holds one, so a cleanup planned from a read removes
// exactly what the plan listed. Ptah's own lock node at the root of a
// database ([LockNode]) stays too, as the reader leaves it out, and so does a
// node whose name starts with a dot. Dot-directories are never entered, nor is
// ydburl.RealmDirectory at the root, and a directory that was empty before is
// left alone. A directory's views go before its tables; YDB would take either
// order, since it records no dependency on a view or on the table a view
// reads.
func (w *Writer) DropAllTables(ctx context.Context) error {
	if w.scheme == nil {
		return fmt.Errorf("no YDB scheme connection")
	}
	_, err := w.dropDirectory(ctx, "")
	return err
}

// dropDirectory drops the views, tables, topics and coordination nodes in the
// directory dir, relative to the database root, and the directories under it,
// and reports whether it dropped or removed anything there.
func (w *Writer) dropDirectory(ctx context.Context, dir string) (bool, error) {
	entries, err := w.scheme.ListDirectory(ctx, path.Join(w.root, dir))
	if err != nil {
		return false, err
	}
	slices.SortFunc(entries, func(a, b *Ydb_Scheme.Entry) int {
		if viewFirst := cmp.Compare(dropRank(a), dropRank(b)); viewFirst != 0 {
			return viewFirst
		}
		return strings.Compare(a.GetName(), b.GetName())
	})
	changed := false
	for _, entry := range entries {
		if statement, droppable := w.describedDropStatement(dir, entry); droppable {
			if err := w.ExecuteSQL(ctx, statement); err != nil {
				return changed, err
			}
			changed = true
			continue
		}
		name := entry.GetName()
		if entry.GetType() != Ydb_Scheme.Entry_DIRECTORY || strings.HasPrefix(name, ".") ||
			(dir == "" && name == ydburl.RealmDirectory) {
			continue
		}
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
	return changed, nil
}

// describedDropStatement is the statement DropAllTables drops entry, in the
// directory dir, with: a view, a row table, a topic, or a coordination node
// other than one [Writer.leftAlone] keeps. It reports false for any other
// entry, which the reader does not describe and the cleanup keeps.
func (w *Writer) describedDropStatement(dir string, entry *Ydb_Scheme.Entry) (string, bool) {
	switch entry.GetType() {
	case Ydb_Scheme.Entry_VIEW, Ydb_Scheme.Entry_TABLE, Ydb_Scheme.Entry_TOPIC:
		return dropStatement(entry.GetType(), path.Join(dir, entry.GetName()))
	case Ydb_Scheme.Entry_COORDINATION_NODE:
		if w.leftAlone(dir, entry) {
			return "", false
		}
		return dropStatement(entry.GetType(), path.Join(dir, entry.GetName()))
	default:
		return "", false
	}
}

// dropRank orders a directory's entries for DropAllTables: views first, then
// everything else.
func dropRank(entry *Ydb_Scheme.Entry) int {
	if entry.GetType() == Ydb_Scheme.Entry_VIEW {
		return 0
	}
	return 1
}

// DropDirectory drops dir, a directory relative to the database root, together
// with everything in it: row and column tables, views, topics, coordination
// nodes and the directories below, deepest first. It is the teardown of a
// directory a caller created for itself, such as the capability probe's
// namespace; DropAllTables is the cleanup that keeps what the reader does not
// describe.
//
// dir names a directory below the root and nothing else: a segment that
// starts with a dot -- `.`, `..`, or a server directory such as `.sys` -- is
// refused, so no spelling of dir reaches the root or leaves it. The whole tree
// is read and checked before anything is dropped. An entry of a kind there is
// no measured statement for, such as an external table, and an entry whose
// name starts with a dot, which belongs to the server, stop it with the entry
// named and nothing dropped.
func (w *Writer) DropDirectory(ctx context.Context, dir string) error {
	relative, err := w.droppableDirectory(dir)
	if err != nil {
		return err
	}
	if w.scheme == nil {
		return fmt.Errorf("no YDB scheme connection")
	}
	var steps []treeStep
	if err := w.planTree(ctx, relative, &steps); err != nil {
		return err
	}
	for _, step := range steps {
		if err := w.runTreeStep(ctx, step); err != nil {
			return err
		}
	}
	return nil
}

// droppableDirectory reads dir as a directory below the database root, or
// refuses it.
func (w *Writer) droppableDirectory(dir string) (string, error) {
	for segment := range strings.SplitSeq(dir, "/") {
		if strings.HasPrefix(segment, ".") {
			return "", fmt.Errorf("ydb: directory %q has the segment %q; DropDirectory removes a directory "+
				"below the database root %s, named without dot segments", dir, segment, w.root)
		}
	}
	relative := strings.Trim(path.Clean("/"+dir), "/")
	if relative == "" {
		return "", fmt.Errorf("ydb: the database root %s is not a directory DropDirectory removes", w.root)
	}
	return relative, nil
}

// treeStatements is the statement that drops each kind of entry the teardown
// removes, with the path in place of %s. A directory has none: the scheme
// service removes it once it is empty.
var treeStatements = map[Ydb_Scheme.Entry_Type]string{
	Ydb_Scheme.Entry_TABLE:        "DROP TABLE %s",
	Ydb_Scheme.Entry_COLUMN_TABLE: "DROP TABLE %s",
	Ydb_Scheme.Entry_VIEW:         "DROP VIEW %s",
	Ydb_Scheme.Entry_TOPIC:        "DROP TOPIC %s",
}

// dropStatement is the statement that drops an entry of entryType at target,
// a path the writer's runner resolves, and reports false for a kind the
// teardown has no statement for. A coordination node is dropped with Ptah's
// own statement; see [dropCoordinationNode].
func dropStatement(entryType Ydb_Scheme.Entry_Type, target string) (string, bool) {
	if entryType == Ydb_Scheme.Entry_COORDINATION_NODE {
		return dropCoordinationNode(target), true
	}
	statement, droppable := treeStatements[entryType]
	if !droppable {
		return "", false
	}
	return fmt.Sprintf(statement, sqlident.Quote(platform.YDB, target)), true
}

// dropCoordinationNode is Ptah's statement that drops the coordination node
// at target, a path the writer's runner resolves. Ptah's YDB connection runs
// it through the coordination service; see [ydbcoordination.Recognize].
func dropCoordinationNode(target string) string {
	// A drop carries no setting, so writing it cannot fail.
	text, _ := ydbcoordination.Statement{Verb: ydbcoordination.Drop, Path: target}.Text()
	return text
}

// treeStep is one step of a directory teardown: a statement that drops an
// object, or the removal of a directory, by its absolute path, once it is
// empty.
type treeStep struct {
	statement string
	directory string
}

// planTree appends the steps that drop what dir holds and then dir itself,
// and refuses the whole tree at the first entry it cannot drop.
func (w *Writer) planTree(ctx context.Context, dir string, steps *[]treeStep) error {
	absolute := path.Join(w.root, dir)
	entries, err := w.scheme.ListDirectory(ctx, absolute)
	if err != nil {
		return err
	}
	slices.SortFunc(entries, func(a, b *Ydb_Scheme.Entry) int { return strings.Compare(a.GetName(), b.GetName()) })
	for _, entry := range entries {
		name := entry.GetName()
		child := path.Join(dir, name)
		statement, droppable := dropStatement(entry.GetType(), child)
		switch {
		case strings.HasPrefix(name, "."):
			return fmt.Errorf("ydb: %s holds %s, whose name starts with a dot and so belongs to the server; "+
				"nothing was dropped", absolute, name)
		case entry.GetType() == Ydb_Scheme.Entry_DIRECTORY:
			if err := w.planTree(ctx, child, steps); err != nil {
				return err
			}
		case droppable:
			*steps = append(*steps, treeStep{statement: statement})
		default:
			return fmt.Errorf("ydb: %s holds %s, a %s, which Ptah has no statement to drop; nothing was dropped",
				absolute, name, entryTypeName(entry.GetType()))
		}
	}
	*steps = append(*steps, treeStep{directory: absolute})
	return nil
}

// runTreeStep runs one step of a teardown.
func (w *Writer) runTreeStep(ctx context.Context, step treeStep) error {
	if step.statement != "" {
		return w.ExecuteSQL(ctx, step.statement)
	}
	if w.dryRun {
		slog.Info("[DRY RUN] Would remove the directory", "path", step.directory)
		return nil
	}
	if err := w.scheme.RemoveDirectory(ctx, step.directory); err != nil {
		return fmt.Errorf("ydb: remove directory %s: %w", step.directory, err)
	}
	return nil
}

// removeIfEmpty removes the directory dir when nothing is left in it.
func (w *Writer) removeIfEmpty(ctx context.Context, dir string) error {
	absolute := path.Join(w.root, dir)
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
	_, children, err := (&grpcSource{scheme: s.client}).ListDirectory(ctx, absolute)
	return children, err
}

func (s grpcScheme) RemoveDirectory(ctx context.Context, absolute string) error {
	response, err := s.client.RemoveDirectory(ctx, &Ydb_Scheme.RemoveDirectoryRequest{Path: absolute})
	if err != nil {
		return WithoutStackFrames(err)
	}
	return operationStatus(response.GetOperation())
}

func (s grpcScheme) MakeDirectory(ctx context.Context, absolute string) error {
	response, err := s.client.MakeDirectory(ctx, &Ydb_Scheme.MakeDirectoryRequest{Path: absolute})
	if err != nil {
		return err
	}
	return operationStatus(response.GetOperation())
}

func (s grpcScheme) DescribePath(ctx context.Context, absolute string) (*Ydb_Scheme.Entry, error) {
	response, err := s.client.DescribePath(ctx, &Ydb_Scheme.DescribePathRequest{Path: absolute})
	if err != nil {
		return nil, fmt.Errorf("describe YDB path %s: %w", absolute, err)
	}
	var described Ydb_Scheme.DescribePathResult
	if err := operationResult(response.GetOperation(), &described); err != nil {
		return nil, fmt.Errorf("describe YDB path %s: %w", absolute, err)
	}
	return described.GetSelf(), nil
}
