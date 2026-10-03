package ydb_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"

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
	if table, dropped := strings.CutPrefix(query, "DROP TABLE `"); dropped {
		full := path.Join("/local", strings.ReplaceAll(strings.TrimSuffix(table, "`"), "\\`", "`"))
		parent, name := path.Split(full)
		f.tree[strings.TrimSuffix(parent, "/")] = without(f.tree[strings.TrimSuffix(parent, "/")], name)
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

// Each statement of a text runs as a query of its own, since a YDB query of
// several DDL statements is not atomic and compiles each against the schema
// as it was before the query.
func TestWriter_ExecuteSQL_OneStatementPerQuery(t *testing.T) {
	c := qt.New(t)
	fake := newFake()
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local")

	err := writer.ExecuteSQL(context.Background(),
		"ALTER TABLE `t` ADD COLUMN `v` Utf8;\nALTER TABLE `t` ADD INDEX `i` GLOBAL SYNC ON (`v`);")

	c.Assert(err, qt.IsNil)
	c.Assert(fake.executed, qt.DeepEquals, []string{
		"ALTER TABLE `t` ADD COLUMN `v` Utf8",
		"ALTER TABLE `t` ADD INDEX `i` GLOBAL SYNC ON (`v`)",
	})
}

// conflictError is a serialization conflict as a SQLSTATE, which
// internal/atlasretry reads the same way it reads YDB's ABORTED status.
type conflictError struct{}

func (conflictError) Error() string    { return "Transaction locks invalidated" }
func (conflictError) SQLState() string { return "40001" }

// A data statement that a conflicting transaction aborted changed nothing, so
// it runs again; a scheme statement runs once, whatever it answers.
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
			failures:     []error{errors.New("Conflict with existing key.")},
			wantExecuted: 1,
			wantErr:      "ydb: SQL execution failed: Conflict with existing key.\nSQL: INSERT INTO `t` \\(`id`\\) VALUES \\(1\\)",
		},
		{
			name:         "arguments for several statements",
			statement:    "UPSERT INTO `a` (`id`) VALUES ($p1); UPSERT INTO `b` (`id`) VALUES ($p1);",
			args:         []any{1},
			wantExecuted: 0,
			wantErr:      "ydb: 2 statements were given one argument list; pass one statement with its arguments",
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

// DropAllTables drops every row table and removes the directories that left
// empty. What the reader does not describe stays, and so does the directory
// that holds it; a directory that was empty before is not touched, and a
// dot-directory is never listed.
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
		"/local/app":     {entry("sub", Ydb_Scheme.Entry_DIRECTORY), entry("t2", Ydb_Scheme.Entry_TABLE)},
		"/local/app/sub": {entry("t`3", Ydb_Scheme.Entry_TABLE)},
		"/local/keep":    nil,
		"/local/mixed":   {entry("t4", Ydb_Scheme.Entry_TABLE), entry("events", Ydb_Scheme.Entry_TOPIC)},
	}}
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local")

	err := writer.DropAllTables(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(fake.executed, qt.DeepEquals, []string{
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
	c.Assert(left, qt.DeepEquals, []string{".sys", "keep", "mixed", "olap", "v"})
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
