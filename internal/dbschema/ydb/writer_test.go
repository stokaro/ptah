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
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"

	"ptah.run/core/ptaherr"
	"ptah.run/internal/dbreset"
	ydbschema "ptah.run/internal/dbschema/ydb"
)

// fakeDatabase is a scheme tree and a statement log. DROP TABLE removes the
// table it names, so the scheme answers the way YDB does after the drop.
type fakeDatabase struct {
	tree     map[string][]*Ydb_Scheme.Entry
	executed []string
	removed  []string
	made     []string
	// created is the creation moment DescribePath answers for each path.
	created map[string]*Ydb.VirtualTimestamp
	// failures answers the statement at each position of the log with an
	// error, where the slice holds one.
	failures []error
}

func (f *fakeDatabase) MakeDirectory(_ context.Context, dir string) error {
	f.made = append(f.made, dir)
	return nil
}

func (f *fakeDatabase) DescribePath(_ context.Context, absolute string) (*Ydb_Scheme.Entry, error) {
	created, ok := f.created[absolute]
	if !ok {
		return nil, fmt.Errorf("described %s, which the fixture does not hold", absolute)
	}
	return &Ydb_Scheme.Entry{Name: path.Base(absolute), Type: Ydb_Scheme.Entry_DIRECTORY, CreatedAt: created}, nil
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
	for _, verb := range []string{"DROP TABLE `", "DROP VIEW `", "DROP TOPIC `", "DROP TRANSFER `",
		"DROP ASYNC REPLICATION `"} {
		if object, dropped := strings.CutPrefix(strings.TrimSuffix(query, " CASCADE"), verb); dropped {
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
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", "")

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
			writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", "")

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
			writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", "")

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
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", "")
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
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", "")
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
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", "")

	tx, err := writer.BeginTransaction(context.Background())
	c.Assert(err, qt.IsNil)
	c.Assert(tx.ExecuteSQL(context.Background(), "DROP TABLE `t`"), qt.IsNil)
	c.Assert(tx.Rollback(), qt.IsNil)
	c.Assert(tx.Commit(), qt.IsNil)
	c.Assert(tx.IsDryRun(), qt.IsFalse)
	c.Assert(fake.executed, qt.DeepEquals, []string{"DROP TABLE `t`"})
}

// DropAllTables drops every view, row table and topic, a directory's views
// first, and removes the directories that left empty. What the reader does not
// describe stays, and so does the directory that holds it; a directory that
// was empty before is not touched, and a dot-directory is never listed, nor
// are the dev realms at the root.
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
			entry("ptah_dev", Ydb_Scheme.Entry_DIRECTORY),
			entry("queues", Ydb_Scheme.Entry_DIRECTORY),
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
		"/local/queues":  {entry("events", Ydb_Scheme.Entry_TOPIC), entry("olap", Ydb_Scheme.Entry_COLUMN_TABLE)},
	}}
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", "")

	err := writer.DropAllTables(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(fake.executed, qt.DeepEquals, []string{
		"DROP VIEW `v`",
		"DROP VIEW `app/v2`",
		"DROP TABLE `app/sub/t\\`3`",
		"DROP TABLE `app/t2`",
		"DROP TOPIC `mixed/events`",
		"DROP TABLE `mixed/t4`",
		"DROP TOPIC `queues/events`",
		"DROP TABLE `t1`",
	})
	c.Assert(fake.removed, qt.DeepEquals, []string{"/local/app/sub", "/local/app", "/local/mixed"})
	var left []string
	for _, kept := range fake.tree["/local"] {
		left = append(left, kept.GetName())
	}
	c.Assert(left, qt.DeepEquals, []string{".sys", "keep", "olap", "ptah_dev", "queues"})
	c.Assert(fake.tree["/local/queues"], qt.HasLen, 1)
}

// Every transfer and then every async replication goes first, before the
// tables and topics they read and write, and a replication goes with CASCADE,
// which drops the replica tables it writes: the reader describes those as the
// replication's rather than as tables.
func TestWriter_DropAllTables_DropsReplicationsFirst(t *testing.T) {
	c := qt.New(t)
	fake := &fakeDatabase{tree: map[string][]*Ydb_Scheme.Entry{
		"/local": {
			entry("a_table", Ydb_Scheme.Entry_TABLE),
			entry("dr", Ydb_Scheme.Entry_DIRECTORY),
			entry("ingest", Ydb_Scheme.Entry_TRANSFER),
			entry("mirror", Ydb_Scheme.Entry_REPLICATION),
			entry("ptah_dev", Ydb_Scheme.Entry_DIRECTORY),
		},
		"/local/dr": {entry("archive", Ydb_Scheme.Entry_TRANSFER), entry("failover", Ydb_Scheme.Entry_REPLICATION)},
	}}
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", "")

	err := writer.DropAllTables(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(fake.executed, qt.DeepEquals, []string{
		"DROP TRANSFER `dr/archive`",
		"DROP TRANSFER `ingest`",
		"DROP ASYNC REPLICATION `dr/failover` CASCADE",
		"DROP ASYNC REPLICATION `mirror` CASCADE",
		"DROP TABLE `a_table`",
	})
	c.Assert(fake.removed, qt.DeepEquals, []string{"/local/dr"})
}

// A drop the server refuses stops the cleanup with the statement named.
func TestWriter_DropAllTables_FailurePath(t *testing.T) {
	c := qt.New(t)
	fake := &fakeDatabase{
		tree:     map[string][]*Ydb_Scheme.Entry{"/local": {entry("t", Ydb_Scheme.Entry_TABLE)}},
		failures: []error{errors.New("SCHEME_ERROR: path is locked")},
	}
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", "")

	err := writer.DropAllTables(context.Background())

	c.Assert(err, qt.ErrorMatches, "ydb: SQL execution failed: SCHEME_ERROR: path is locked\nSQL: DROP TABLE `t`")
	c.Assert(fake.removed, qt.HasLen, 0)
}

// DropDirectory removes a directory a caller made for itself with everything
// in it, deepest first: tables of both kinds, views, topics, and the
// directories below.
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
		"/local/probe/rb": {entry("t`2", Ydb_Scheme.Entry_TABLE), entry("events", Ydb_Scheme.Entry_TOPIC)},
		"/local/app":      {entry("keep", Ydb_Scheme.Entry_TABLE)},
	}}
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", "")

	err := writer.DropDirectory(context.Background(), "/probe/")

	c.Assert(err, qt.IsNil)
	c.Assert(fake.executed, qt.DeepEquals, []string{
		"DROP TABLE `probe/olap`",
		"DROP TOPIC `probe/rb/events`",
		"DROP TABLE `probe/rb/t\\`2`",
		"DROP TABLE `probe/t`",
		"DROP VIEW `probe/v`",
	})
	c.Assert(fake.removed, qt.DeepEquals, []string{"/local/probe/rb", "/local/probe"})
	c.Assert(fake.tree["/local/app"], qt.HasLen, 1)
}

// A teardown drops every transfer and then every async replication in the
// directory before anything else, the replication without CASCADE: its
// replica tables are in the tree, and the teardown's own DROP TABLE of each
// would fail on a table CASCADE dropped first.
func TestWriter_DropDirectory_DropsReplicationsFirst(t *testing.T) {
	c := qt.New(t)
	fake := &fakeDatabase{tree: map[string][]*Ydb_Scheme.Entry{
		"/local": {entry("probe", Ydb_Scheme.Entry_DIRECTORY)},
		"/local/probe": {
			entry("a_replica", Ydb_Scheme.Entry_TABLE),
			entry("mirror", Ydb_Scheme.Entry_REPLICATION),
			entry("sub", Ydb_Scheme.Entry_DIRECTORY),
		},
		"/local/probe/sub": {entry("ingest", Ydb_Scheme.Entry_TRANSFER), entry("log", Ydb_Scheme.Entry_TABLE)},
	}}
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", "")

	err := writer.DropDirectory(context.Background(), "probe")

	c.Assert(err, qt.IsNil)
	c.Assert(fake.executed, qt.DeepEquals, []string{
		"DROP TRANSFER `probe/sub/ingest`",
		"DROP ASYNC REPLICATION `probe/mirror`",
		"DROP TABLE `probe/a_replica`",
		"DROP TABLE `probe/sub/log`",
	})
	c.Assert(fake.removed, qt.DeepEquals, []string{"/local/probe/sub", "/local/probe"})
}

// A reset lists a transfer and an async replication as objects it drops, and
// drops them before the tables they read and write.
func TestWriter_DropDatabaseRealm_DropsReplicationsFirst(t *testing.T) {
	c := qt.New(t)
	tree := func() map[string][]*Ydb_Scheme.Entry {
		return map[string][]*Ydb_Scheme.Entry{
			"/local": {
				entry("app", Ydb_Scheme.Entry_DIRECTORY),
				entry("mirror", Ydb_Scheme.Entry_REPLICATION),
			},
			"/local/app": {entry("ingest", Ydb_Scheme.Entry_TRANSFER), entry("log", Ydb_Scheme.Entry_TABLE)},
		}
	}

	objects, err := ydbschema.NewWriterFromScheme(&fakeDatabase{tree: tree()}, &fakeDatabase{tree: tree()},
		"/local", "").ResetObjects(context.Background(), dbreset.Scope{})
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.DeepEquals, []dbreset.Object{
		{Kind: "transfer", Schema: "app", Name: "ingest"},
		{Kind: "table", Schema: "app", Name: "log"},
		{Kind: "directory", Name: "app"},
		{Kind: "replication", Name: "mirror"},
	})

	fake := &fakeDatabase{tree: tree()}
	err = ydbschema.NewWriterFromScheme(fake, fake, "/local", "").DropDatabaseRealm(context.Background())
	c.Assert(err, qt.IsNil)
	c.Assert(fake.executed, qt.DeepEquals, []string{
		"DROP TRANSFER `app/ingest`",
		"DROP ASYNC REPLICATION `mirror`",
		"DROP TABLE `app/log`",
	})
	c.Assert(fake.removed, qt.DeepEquals, []string{"/local/app"})
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
			name: "a coordination node below a table the walk meets first",
			dir:  "probe",
			wantErr: "ydb: /local/probe/z holds locks, a COORDINATION_NODE, which Ptah has no statement to drop; " +
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
				"/local/probe/z":      {entry("locks", Ydb_Scheme.Entry_COORDINATION_NODE)},
				"/local/scratch":      {entry("t", Ydb_Scheme.Entry_TABLE), entry(".tmp", Ydb_Scheme.Entry_DIRECTORY)},
				"/local/scratch/.tmp": {entry("x", Ydb_Scheme.Entry_TABLE)},
			}}
			writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", "")

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
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", "")

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

// rootTree is a database whose root holds the server's directories, Ptah's
// lock node and dev realms, and the objects a reset finds; realm r1 holds a
// table and a directory named like the realms' one.
func rootTree() map[string][]*Ydb_Scheme.Entry {
	return map[string][]*Ydb_Scheme.Entry{
		"/local": {
			entry("users", Ydb_Scheme.Entry_TABLE),
			entry(".sys", Ydb_Scheme.Entry_DIRECTORY),
			entry("app", Ydb_Scheme.Entry_DIRECTORY),
			entry("empty", Ydb_Scheme.Entry_DIRECTORY),
			entry("ptah_dev", Ydb_Scheme.Entry_DIRECTORY),
			entry("ptah_locks", Ydb_Scheme.Entry_COORDINATION_NODE),
		},
		"/local/app": {
			entry("v", Ydb_Scheme.Entry_VIEW), entry("orders", Ydb_Scheme.Entry_TABLE), entry("events", Ydb_Scheme.Entry_TOPIC),
		},
		"/local/empty":                nil,
		"/local/ptah_dev":             {entry("r1", Ydb_Scheme.Entry_DIRECTORY), entry("r2", Ydb_Scheme.Entry_DIRECTORY)},
		"/local/ptah_dev/r1":          {entry("t", Ydb_Scheme.Entry_TABLE), entry("ptah_dev", Ydb_Scheme.Entry_DIRECTORY)},
		"/local/ptah_dev/r1/ptah_dev": {entry("olap", Ydb_Scheme.Entry_COLUMN_TABLE)},
	}
}

// A reset lists what it would drop, contents before their directory, and
// leaves out the server's directories and, at a database's root only, Ptah's
// lock node and the dev realms.
func TestWriter_ResetObjects(t *testing.T) {
	tests := []struct {
		name  string
		realm string
		want  []dbreset.Object
	}{
		{
			name: "a database",
			want: []dbreset.Object{
				{Kind: "topic", Schema: "app", Name: "events"},
				{Kind: "table", Schema: "app", Name: "orders"},
				{Kind: "view", Schema: "app", Name: "v"},
				{Kind: "directory", Name: "app"},
				{Kind: "directory", Name: "empty"},
				{Kind: "table", Name: "users"},
			},
		},
		{
			name:  "a realm",
			realm: "r1",
			want: []dbreset.Object{
				{Kind: "column table", Schema: "ptah_dev", Name: "olap"},
				{Kind: "directory", Name: "ptah_dev"},
				{Kind: "table", Name: "t"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fake := &fakeDatabase{tree: rootTree()}
			writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", test.realm)

			got, err := writer.ResetObjects(context.Background(), dbreset.Scope{})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// A reset drops what it lists and removes the directories below the root,
// deepest first; the statements name objects relative to the root, which is
// the root the connection resolves names against.
func TestWriter_DropDatabaseRealm(t *testing.T) {
	tests := []struct {
		name         string
		realm        string
		wantExecuted []string
		wantRemoved  []string
	}{
		{
			name: "a database",
			wantExecuted: []string{
				"DROP TOPIC `app/events`", "DROP TABLE `app/orders`", "DROP VIEW `app/v`", "DROP TABLE `users`",
			},
			wantRemoved: []string{"/local/app", "/local/empty"},
		},
		{
			name:         "a realm",
			realm:        "r1",
			wantExecuted: []string{"DROP TABLE `ptah_dev/olap`", "DROP TABLE `t`"},
			wantRemoved:  []string{"/local/ptah_dev/r1/ptah_dev"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fake := &fakeDatabase{tree: rootTree()}
			writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", test.realm)

			err := writer.DropDatabaseRealm(context.Background())

			c.Assert(err, qt.IsNil)
			c.Assert(fake.executed, qt.DeepEquals, test.wantExecuted)
			c.Assert(fake.removed, qt.DeepEquals, test.wantRemoved)
		})
	}
}

// An object a reset has no statement for stops it before anything is dropped,
// and so does an environment a YDB reset cannot keep.
func TestWriter_DropDatabaseRealm_FailurePath(t *testing.T) {
	t.Run("a coordination node", func(t *testing.T) {
		c := qt.New(t)
		tree := rootTree()
		tree["/local/app"] = append(tree["/local/app"], entry("semaphores", Ydb_Scheme.Entry_COORDINATION_NODE))
		fake := &fakeDatabase{tree: tree}
		writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", "")

		err := writer.DropDatabaseRealm(context.Background())

		c.Assert(err, qt.ErrorMatches, `ydb: /local holds coordination node "semaphores" in directory "app", `+
			`which Ptah has no statement to drop; nothing was dropped`)
		c.Assert(fake.executed, qt.HasLen, 0)
		c.Assert(fake.removed, qt.HasLen, 0)
	})
	t.Run("a kept extension", func(t *testing.T) {
		c := qt.New(t)
		fake := &fakeDatabase{tree: rootTree()}
		writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", "")

		err := writer.DropAllTablesKeeping(context.Background(), dbreset.Kept{Extensions: []string{"vector"}})

		c.Assert(err, qt.ErrorMatches, "ydb: a YDB dev database has no environment a reset keeps, and this one names some")
		c.Assert(fake.executed, qt.HasLen, 0)
	})
}

// A run's realm goes with everything in it, a name that starts with a dot
// included, through statements that name each object from the database's
// root; the directory of the realms goes with the last realm.
func TestWriter_RemoveRealm(t *testing.T) {
	tests := []struct {
		name         string
		others       []*Ydb_Scheme.Entry
		wantExecuted []string
		wantRemoved  []string
	}{
		{
			name:         "another realm is left",
			others:       []*Ydb_Scheme.Entry{entry("r2", Ydb_Scheme.Entry_DIRECTORY)},
			wantExecuted: []string{"DROP TABLE `ptah_dev/r1/.hidden`", "DROP VIEW `ptah_dev/r1/app/v`", "DROP TABLE `ptah_dev/r1/t`"},
			wantRemoved:  []string{"/local/ptah_dev/r1/app", "/local/ptah_dev/r1"},
		},
		{
			name:         "the last realm",
			wantExecuted: []string{"DROP TABLE `ptah_dev/r1/.hidden`", "DROP VIEW `ptah_dev/r1/app/v`", "DROP TABLE `ptah_dev/r1/t`"},
			wantRemoved:  []string{"/local/ptah_dev/r1/app", "/local/ptah_dev/r1", "/local/ptah_dev"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fake := &fakeDatabase{tree: map[string][]*Ydb_Scheme.Entry{
				"/local":          {entry("ptah_dev", Ydb_Scheme.Entry_DIRECTORY)},
				"/local/ptah_dev": append([]*Ydb_Scheme.Entry{entry("r1", Ydb_Scheme.Entry_DIRECTORY)}, test.others...),
				"/local/ptah_dev/r1": {
					entry("t", Ydb_Scheme.Entry_TABLE),
					entry(".hidden", Ydb_Scheme.Entry_TABLE),
					entry("app", Ydb_Scheme.Entry_DIRECTORY),
				},
				"/local/ptah_dev/r1/app": {entry("v", Ydb_Scheme.Entry_VIEW)},
			}}
			writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", "")

			err := writer.RemoveRealm(context.Background(), "r1")

			c.Assert(err, qt.IsNil)
			c.Assert(fake.executed, qt.DeepEquals, test.wantExecuted)
			c.Assert(fake.removed, qt.DeepEquals, test.wantRemoved)
		})
	}
}

// A realm is removed through its database, by a name a realm can carry, and
// only when everything in it can be dropped; otherwise nothing is dropped.
func TestWriter_RemoveRealm_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		realm   string
		in      string
		wantErr string
	}{
		{
			name:    "through a realm",
			realm:   "r1",
			in:      "r2",
			wantErr: `ydb: a realm is removed through its database, not through /local/ptah_dev/r2`,
		},
		{
			name:    "a name that is a path",
			realm:   "../app",
			wantErr: `the dev_realm parameter "../app" is not a realm name: use 1 to 64 lowercase letters, digits and underscores`,
		},
		{
			name:  "a coordination node inside",
			realm: "r1",
			wantErr: `ydb: the dev realm /local/ptah_dev/r1 holds coordination node "semaphores" in directory "app", ` +
				`which Ptah has no statement to drop; nothing was dropped`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fake := &fakeDatabase{tree: map[string][]*Ydb_Scheme.Entry{
				"/local":                 {entry("ptah_dev", Ydb_Scheme.Entry_DIRECTORY)},
				"/local/ptah_dev":        {entry("r1", Ydb_Scheme.Entry_DIRECTORY)},
				"/local/ptah_dev/r1":     {entry("t", Ydb_Scheme.Entry_TABLE), entry("app", Ydb_Scheme.Entry_DIRECTORY)},
				"/local/ptah_dev/r1/app": {entry("semaphores", Ydb_Scheme.Entry_COORDINATION_NODE)},
			}}
			writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", test.in)

			err := writer.RemoveRealm(context.Background(), test.realm)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(fake.executed, qt.HasLen, 0)
			c.Assert(fake.removed, qt.HasLen, 0)
		})
	}
}

// A realm's identity is its database's, which the server's creation moment
// tells apart from a database of the same path on another server, and the
// realm's directory.
func TestWriter_RealmIdentity(t *testing.T) {
	tests := []struct {
		name    string
		realm   string
		created *Ydb.VirtualTimestamp
		want    string
	}{
		{name: "a database", created: &Ydb.VirtualTimestamp{PlanStep: 1791098831240, TxId: 1}, want: "/local@1791098831240.1"},
		{
			name:    "a realm",
			realm:   "r1",
			created: &Ydb.VirtualTimestamp{PlanStep: 1791098831240, TxId: 1},
			want:    "/local@1791098831240.1/ptah_dev/r1",
		},
		{name: "a server that reports no creation moment", want: "/local"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fake := &fakeDatabase{created: map[string]*Ydb.VirtualTimestamp{"/local": test.created}}
			writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", test.realm)

			got, err := writer.RealmIdentity(context.Background())

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A directory is created under the root, the realm's when there is one.
func TestWriter_MakeDirectory(t *testing.T) {
	c := qt.New(t)
	fake := newFake()

	c.Assert(ydbschema.NewWriterFromScheme(fake, fake, "/local", "").MakeDirectory(context.Background(), "ptah_dev/r1"), qt.IsNil)
	c.Assert(ydbschema.NewWriterFromScheme(fake, fake, "/local", "r1").MakeDirectory(context.Background(), "app"), qt.IsNil)

	c.Assert(fake.made, qt.DeepEquals, []string{"/local/ptah_dev/r1", "/local/ptah_dev/r1/app"})
}

// The migrator's tables are found by name in every directory DropAllTables
// enters, each with its own directory; a view of the same name is not a
// table, and neither the server's directories nor the dev realms are entered.
func TestWriter_TablesNamed(t *testing.T) {
	c := qt.New(t)
	fake := &fakeDatabase{tree: map[string][]*Ydb_Scheme.Entry{
		"/local": {
			entry("schema_migrations", Ydb_Scheme.Entry_TABLE),
			entry("users", Ydb_Scheme.Entry_TABLE),
			entry(".sys", Ydb_Scheme.Entry_DIRECTORY),
			entry("ptah_dev", Ydb_Scheme.Entry_DIRECTORY),
			entry("app", Ydb_Scheme.Entry_DIRECTORY),
		},
		"/local/app": {
			entry("ptah_migration_tags", Ydb_Scheme.Entry_TABLE),
			entry("schema_migrations", Ydb_Scheme.Entry_VIEW),
			entry("sub", Ydb_Scheme.Entry_DIRECTORY),
		},
		"/local/app/sub": {entry("schema_migrations", Ydb_Scheme.Entry_TABLE)},
	}}
	writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", "")

	got, err := writer.TablesNamed(context.Background(), []string{"schema_migrations", "ptah_migration_tags"})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []dbreset.Object{
		{Kind: "table", Schema: "app", Name: "ptah_migration_tags"},
		{Kind: "table", Schema: "app/sub", Name: "schema_migrations"},
		{Kind: "table", Name: "schema_migrations"},
	})
}
