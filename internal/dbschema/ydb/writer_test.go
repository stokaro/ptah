package ydb_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"

	"ptah.run/core/ptaherr"
	ydbschema "ptah.run/internal/dbschema/ydb"
)

// fakeDatabase is a scheme tree and a statement log. DROP TABLE removes the
// table it names, so the scheme answers the way YDB does after the drop.
type fakeDatabase struct {
	tree     map[string][]*Ydb_Scheme.Entry
	executed []string
	removed  []string
	// failures answers the statement at each position of the log with an
	// error, where the slice holds one.
	failures []error
}

func (f *fakeDatabase) ListDirectory(_ context.Context, dir string) ([]*Ydb_Scheme.Entry, error) {
	entries, ok := f.tree[dir]
	if !ok {
		return nil, fmt.Errorf("listed %s, which the fixture does not hold", dir)
	}
	return entries, nil
}

func (f *fakeDatabase) RemoveDirectory(_ context.Context, dir string) error {
	f.removed = append(f.removed, dir)
	parent, name := path.Split(dir)
	f.tree[strings.TrimSuffix(parent, "/")] = without(f.tree[strings.TrimSuffix(parent, "/")], name)
	delete(f.tree, dir)
	return nil
}

func without(entries []*Ydb_Scheme.Entry, name string) []*Ydb_Scheme.Entry {
	var kept []*Ydb_Scheme.Entry
	for _, entry := range entries {
		if entry.GetName() != name {
			kept = append(kept, entry)
		}
	}
	return kept
}

func (f *fakeDatabase) BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error) {
	return nil, errors.New("not used")
}
func (f *fakeDatabase) Exec(query string, args ...any) (sql.Result, error) {
	return f.ExecContext(context.Background(), query, args...)
}
func (f *fakeDatabase) ExecContext(_ context.Context, query string, _ ...any) (sql.Result, error) {
	position := len(f.executed)
	f.executed = append(f.executed, query)
	if position < len(f.failures) && f.failures[position] != nil {
		return nil, f.failures[position]
	}
	for _, verb := range []string{"DROP TABLE `", "DROP VIEW `"} {
		if object, dropped := strings.CutPrefix(query, verb); dropped {
			full := path.Join("/local", strings.ReplaceAll(strings.TrimSuffix(object, "`"), "\\`", "`"))
			parent, name := path.Split(full)
			f.tree[strings.TrimSuffix(parent, "/")] = without(f.tree[strings.TrimSuffix(parent, "/")], name)
		}
	}
	return driver.RowsAffected(0), nil
}
func (f *fakeDatabase) Query(string, ...any) (*sql.Rows, error) { return nil, errors.New("not used") }
func (f *fakeDatabase) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("not used")
}
func (f *fakeDatabase) QueryRow(string, ...any) *sql.Row { return nil }
func (f *fakeDatabase) QueryRowContext(context.Context, string, ...any) *sql.Row {
	return nil
}

func newFake() *fakeDatabase {
	return &fakeDatabase{tree: map[string][]*Ydb_Scheme.Entry{"/local": nil}}
}

// Each scheme statement of a text runs as a query of its own, since a YDB
// query of several DDL statements is not atomic and compiles each against the
// schema as it was before the query. Consecutive data statements run as one
// query, so a named expression reaches the statement that uses it, and a
// definition reaches every query after it.
func TestWriter_ExecuteSQL_RunsTheQueriesOfTheSplit(t *testing.T) {
	c := qt.New(t)
	fake := newFake()
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local")

	err := writer.ExecuteSQL(context.Background(),
		"ALTER TABLE `t` ADD COLUMN `v` Utf8;\nALTER TABLE `t` ADD INDEX `i` GLOBAL SYNC ON (`v`);\n"+
			"$v = 'x'u;\nUPSERT INTO `t` (`id`, `v`) VALUES (1l, $v);\nUPDATE `t` SET `v` = $v WHERE `id` = 2l;\n"+
			"DROP TABLE `u`;")

	c.Assert(err, qt.IsNil)
	c.Assert(fake.executed, qt.DeepEquals, []string{
		"ALTER TABLE `t` ADD COLUMN `v` Utf8",
		"ALTER TABLE `t` ADD INDEX `i` GLOBAL SYNC ON (`v`)",
		"$v = 'x'u;\nUPSERT INTO `t` (`id`, `v`) VALUES (1l, $v);\nUPDATE `t` SET `v` = $v WHERE `id` = 2l",
		"$v = 'x'u;\nDROP TABLE `u`",
	})
}

// conflictError is a serialization conflict as a SQLSTATE, which
// internal/atlasretry reads the same way it reads YDB's ABORTED status.
type conflictError struct{}

func (conflictError) Error() string    { return "Transaction locks invalidated" }
func (conflictError) SQLState() string { return "40001" }

// A data query that a conflicting transaction aborted changed nothing, so it
// runs again; a scheme query runs once, whatever it answers.
func TestWriter_ExecuteSQL_RetriesAnAbortedDataStatement(t *testing.T) {
	tests := []struct {
		name         string
		statement    string
		failures     []error
		wantExecuted int
		wantErr      string
	}{
		{
			name:         "an upsert that conflicts once",
			statement:    "UPSERT INTO `t` (`id`) VALUES (1)",
			failures:     []error{conflictError{}},
			wantExecuted: 2,
		},
		{
			name:         "a comment before the keyword",
			statement:    "-- seed\nUPDATE `t` SET `v` = 1 WHERE `id` = 1",
			failures:     []error{conflictError{}, conflictError{}},
			wantExecuted: 3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fake := newFake()
			fake.failures = test.failures
			writer := ydbschema.NewWriterFromScheme(fake, fake, "/local")

			err := writer.ExecuteSQL(context.Background(), test.statement)

			c.Assert(err, qt.IsNil)
			c.Assert(fake.executed, qt.HasLen, test.wantExecuted)
		})
	}
}

func TestWriter_ExecuteSQL_FailurePath(t *testing.T) {
	tests := []struct {
		name         string
		statement    string
		args         []any
		failures     []error
		wantExecuted int
		wantErr      string
	}{
		{
			name:         "a scheme statement is not run again",
			statement:    "ALTER TABLE `t` ADD COLUMN `v` Utf8",
			failures:     []error{conflictError{}},
			wantExecuted: 1,
			wantErr:      "ydb: SQL execution failed: Transaction locks invalidated\nSQL: ALTER TABLE `t` ADD COLUMN `v` Utf8",
		},
		{
			name:      "a data statement that keeps conflicting",
			statement: "DELETE FROM `t`",
			failures: []error{conflictError{}, conflictError{}, conflictError{}, conflictError{},
				conflictError{}},
			wantExecuted: 5,
			wantErr:      "ydb: SQL execution failed: Transaction locks invalidated\nSQL: DELETE FROM `t`",
		},
		{
			name:         "a failure that is not a conflict",
			statement:    "INSERT INTO `t` (`id`) VALUES (1)",
			failures:     []error{errors.New("conflict with an existing key")},
			wantExecuted: 1,
			wantErr:      "ydb: SQL execution failed: conflict with an existing key\nSQL: INSERT INTO `t` \\(`id`\\) VALUES \\(1\\)",
		},
		{
			name:         "arguments for several queries",
			statement:    "UPSERT INTO `a` (`id`) VALUES ($p1); DROP TABLE `b`;",
			args:         []any{1},
			wantExecuted: 0,
			wantErr:      "ydb: 2 queries were given one argument list; pass one query with its arguments",
		},
		{
			name:         "a text the split refuses",
			statement:    "--!ansi_lexer\nDROP TABLE `b`;",
			wantExecuted: 0,
			wantErr:      `ydb: unsupported YQL translation setting "--!ansi_lexer": .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fake := newFake()
			fake.failures = test.failures
			writer := ydbschema.NewWriterFromScheme(fake, fake, "/local")

			err := writer.ExecuteSQL(context.Background(), test.statement, test.args...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(fake.executed, qt.HasLen, test.wantExecuted)
		})
	}
}

// A data statement aborted by a conflict is not run again once the context
// has ended: the wait before the retry returns the context's error instead.
func TestWriter_ExecuteSQL_FailurePath_StopsRetryingWhenTheContextEnds(t *testing.T) {
	c := qt.New(t)
	fake := newFake()
	fake.failures = []error{conflictError{}, conflictError{}, conflictError{}, conflictError{}, conflictError{}}
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := writer.ExecuteSQL(ctx, "UPSERT INTO `t` (`id`) VALUES (1)")

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(fake.executed, qt.HasLen, 1)
}

// A dry run executes nothing.
func TestWriter_DryRun(t *testing.T) {
	c := qt.New(t)
	fake := newFake()
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local")
	writer.SetDryRun(true)

	err := writer.ExecuteSQL(context.Background(), "DROP TABLE `t`")

	c.Assert(err, qt.IsNil)
	c.Assert(writer.IsDryRun(), qt.IsTrue)
	c.Assert(fake.executed, qt.HasLen, 0)
}

// A transaction holds no DDL on YDB, so its methods take effect at once and
// Commit and Rollback have nothing to do.
func TestWriter_TransactionIsANoOp(t *testing.T) {
	c := qt.New(t)
	fake := newFake()
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local")

	tx, err := writer.BeginTransaction(context.Background())
	c.Assert(err, qt.IsNil)
	c.Assert(tx.ExecuteSQL(context.Background(), "DROP TABLE `t`"), qt.IsNil)
	c.Assert(tx.Rollback(), qt.IsNil)
	c.Assert(tx.Commit(), qt.IsNil)
	c.Assert(tx.IsDryRun(), qt.IsFalse)
	c.Assert(fake.executed, qt.DeepEquals, []string{"DROP TABLE `t`"})
}

// DropAllTables drops every view and row table, a directory's views first, and
// removes the directories that left empty. What the reader does not describe
// stays, and so does the directory that holds it; a directory that was empty
// before is not touched, and a dot-directory is never listed.
func TestWriter_DropAllTables(t *testing.T) {
	c := qt.New(t)
	fake := &fakeDatabase{tree: map[string][]*Ydb_Scheme.Entry{
		"/local": {
			entry("t1", Ydb_Scheme.Entry_TABLE),
			entry(".sys", Ydb_Scheme.Entry_DIRECTORY),
			entry("app", Ydb_Scheme.Entry_DIRECTORY),
			entry("keep", Ydb_Scheme.Entry_DIRECTORY),
			entry("mixed", Ydb_Scheme.Entry_DIRECTORY),
			entry("olap", Ydb_Scheme.Entry_COLUMN_TABLE),
			entry("v", Ydb_Scheme.Entry_VIEW),
		},
		"/local/app": {
			entry("sub", Ydb_Scheme.Entry_DIRECTORY),
			entry("t2", Ydb_Scheme.Entry_TABLE),
			entry("v2", Ydb_Scheme.Entry_VIEW),
		},
		"/local/app/sub": {entry("t`3", Ydb_Scheme.Entry_TABLE)},
		"/local/keep":    nil,
		"/local/mixed":   {entry("t4", Ydb_Scheme.Entry_TABLE), entry("events", Ydb_Scheme.Entry_TOPIC)},
	}}
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local")

	err := writer.DropAllTables(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(fake.executed, qt.DeepEquals, []string{
		"DROP VIEW `v`",
		"DROP VIEW `app/v2`",
		"DROP TABLE `app/sub/t\\`3`",
		"DROP TABLE `app/t2`",
		"DROP TABLE `mixed/t4`",
		"DROP TABLE `t1`",
	})
	c.Assert(fake.removed, qt.DeepEquals, []string{"/local/app/sub", "/local/app"})
	var left []string
	for _, kept := range fake.tree["/local"] {
		left = append(left, kept.GetName())
	}
	c.Assert(left, qt.DeepEquals, []string{".sys", "keep", "mixed", "olap"})
	c.Assert(fake.tree["/local/mixed"], qt.HasLen, 1)
}

// A drop the server refuses stops the cleanup with the statement named.
func TestWriter_DropAllTables_FailurePath(t *testing.T) {
	c := qt.New(t)
	fake := &fakeDatabase{
		tree:     map[string][]*Ydb_Scheme.Entry{"/local": {entry("t", Ydb_Scheme.Entry_TABLE)}},
		failures: []error{errors.New("SCHEME_ERROR: path is locked")},
	}
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local")

	err := writer.DropAllTables(context.Background())

	c.Assert(err, qt.ErrorMatches, "ydb: SQL execution failed: SCHEME_ERROR: path is locked\nSQL: DROP TABLE `t`")
	c.Assert(fake.removed, qt.HasLen, 0)
}

// DropDirectory removes a directory a caller made for itself with everything
// in it, deepest first: tables of both kinds, views, and the directories below.
// What sits beside the directory is not touched.
func TestWriter_DropDirectory(t *testing.T) {
	c := qt.New(t)
	fake := &fakeDatabase{tree: map[string][]*Ydb_Scheme.Entry{
		"/local": {entry("probe", Ydb_Scheme.Entry_DIRECTORY), entry("app", Ydb_Scheme.Entry_DIRECTORY)},
		"/local/probe": {
			entry("t", Ydb_Scheme.Entry_TABLE),
			entry("olap", Ydb_Scheme.Entry_COLUMN_TABLE),
			entry("v", Ydb_Scheme.Entry_VIEW),
			entry("rb", Ydb_Scheme.Entry_DIRECTORY),
		},
		"/local/probe/rb": {entry("t`2", Ydb_Scheme.Entry_TABLE)},
		"/local/app":      {entry("keep", Ydb_Scheme.Entry_TABLE)},
	}}
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local")

	err := writer.DropDirectory(context.Background(), "/probe/")

	c.Assert(err, qt.IsNil)
	c.Assert(fake.executed, qt.DeepEquals, []string{
		"DROP TABLE `probe/olap`",
		"DROP TABLE `probe/rb/t\\`2`",
		"DROP TABLE `probe/t`",
		"DROP VIEW `probe/v`",
	})
	c.Assert(fake.removed, qt.DeepEquals, []string{"/local/probe/rb", "/local/probe"})
	c.Assert(fake.tree["/local/app"], qt.HasLen, 1)
}

// A teardown that cannot finish drops nothing: a dir that names the root or
// leaves it in any spelling, and a tree holding, at any depth, an entry of a
// kind the writer has no statement for or one that belongs to the server.
func TestWriter_DropDirectory_FailurePath(t *testing.T) {
	const dotSegment = "ydb: directory %q has the segment %q; DropDirectory removes a directory below the " +
		"database root /local, named without dot segments"
	for _, test := range []struct {
		name    string
		dir     string
		wantErr string
	}{
		{name: "the root", dir: "/", wantErr: "ydb: the database root /local is not a directory DropDirectory removes"},
		{name: "a dot", dir: ".", wantErr: fmt.Sprintf(dotSegment, ".", ".")},
		{name: "a dot and a slash", dir: "./", wantErr: fmt.Sprintf(dotSegment, "./", ".")},
		{name: "a dot between slashes", dir: "/./", wantErr: fmt.Sprintf(dotSegment, "/./", ".")},
		{name: "a directory and its parent", dir: "x/..", wantErr: fmt.Sprintf(dotSegment, "x/..", "..")},
		{name: "a real directory and its parent", dir: "app/../", wantErr: fmt.Sprintf(dotSegment, "app/../", "..")},
		{name: "a server directory", dir: ".sys", wantErr: fmt.Sprintf(dotSegment, ".sys", ".sys")},
		{
			name: "a topic below a table the walk meets first",
			dir:  "probe",
			wantErr: "ydb: /local/probe/z holds events, a TOPIC, which Ptah has no statement to drop; " +
				"nothing was dropped",
		},
		{
			name: "a dot directory inside",
			dir:  "scratch",
			wantErr: "ydb: /local/scratch holds .tmp, whose name starts with a dot and so belongs to the server; " +
				"nothing was dropped",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fake := &fakeDatabase{tree: map[string][]*Ydb_Scheme.Entry{
				"/local": {
					entry(".sys", Ydb_Scheme.Entry_DIRECTORY),
					entry("app", Ydb_Scheme.Entry_DIRECTORY),
					entry("orders", Ydb_Scheme.Entry_TABLE),
					entry("probe", Ydb_Scheme.Entry_DIRECTORY),
					entry("scratch", Ydb_Scheme.Entry_DIRECTORY),
				},
				"/local/.sys":         nil,
				"/local/app":          {entry("users", Ydb_Scheme.Entry_TABLE)},
				"/local/probe":        {entry("a", Ydb_Scheme.Entry_TABLE), entry("z", Ydb_Scheme.Entry_DIRECTORY)},
				"/local/probe/z":      {entry("events", Ydb_Scheme.Entry_TOPIC)},
				"/local/scratch":      {entry("t", Ydb_Scheme.Entry_TABLE), entry(".tmp", Ydb_Scheme.Entry_DIRECTORY)},
				"/local/scratch/.tmp": {entry("x", Ydb_Scheme.Entry_TABLE)},
			}}
			writer := ydbschema.NewWriterFromScheme(fake, fake, "/local")

			err := writer.DropDirectory(context.Background(), test.dir)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(fake.executed, qt.HasLen, 0)
			c.Assert(fake.removed, qt.HasLen, 0)
		})
	}
}

// A refusal that says a feature flag is off names the capability the flag
// decides, so the operator learns which key the cluster turned off and how to
// let Ptah read it. The server's own text stays in the message.
func TestWriter_ExecuteSQL_FailurePath_NamesTheCapabilityAFlagTurnedOff(t *testing.T) {
	c := qt.New(t)
	fake := newFake()
	refusal := errors.New("Status: BAD_REQUEST Issues: <main>: Error: Failed item check: " +
		"Adding a unique index to an existing table is disabled")
	fake.failures = []error{refusal}
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local")

	err := writer.ExecuteSQL(context.Background(), "ALTER TABLE `t` ADD INDEX `u` GLOBAL UNIQUE SYNC ON (`n`)")

	c.Assert(err, qt.ErrorMatches, "ydb: SQL execution failed: capability unique_index_on_existing_table is off "+
		"on this YDB cluster, which runs with feature flag EnableAddUniqueIndex off: Status: BAD_REQUEST .*"+
		"Adding a unique index to an existing table is disabled. Turn the flag on, or name the cluster's "+
		"monitoring endpoint in the URL \\(monitoring=http://host:8765\\) so Ptah reads the flags before it plans\n"+
		"SQL: ALTER TABLE `t` ADD INDEX `u` GLOBAL UNIQUE SYNC ON \\(`n`\\)")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorIs, refusal)
	var capabilityErr *ptaherr.CapabilityError
	c.Assert(err, qt.ErrorAs, &capabilityErr)
	c.Assert(capabilityErr.Feature, qt.Equals, "unique_index_on_existing_table")
}
