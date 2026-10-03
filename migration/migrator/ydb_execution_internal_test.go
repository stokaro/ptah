package migrator

// White-box testing required: what decides how a YDB body runs -- the units it
// is split into, the source each unit is digested from, the refusal of a body
// that cannot be split, and the hook that commits a data query with its
// checkpoint -- is package-local, and the exported path that reaches it needs a
// live YDB connection. The live tests under integration/dbschema/ydb drive that
// path; these pin each decision on its own.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

const ydbBody = "CREATE TABLE `t` (id Int64 NOT NULL, PRIMARY KEY (id));\n" +
	"$v = 1l;\nUPSERT INTO `t` (id) VALUES ($v);\nUPSERT INTO `t` (id) VALUES ($v + 1);\n" +
	"ALTER TABLE `t` ADD COLUMN `name` Utf8;\n"

// On YDB the units are the queries the body runs as; elsewhere they stay
// statements.
func TestSplitSQLStatementsForDialect_YDBRunsQueries(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{
			name:    "ydb",
			dialect: platform.YDB,
			want: []string{
				"CREATE TABLE `t` (id Int64 NOT NULL, PRIMARY KEY (id))",
				"$v = 1l;\nUPSERT INTO `t` (id) VALUES ($v);\nUPSERT INTO `t` (id) VALUES ($v + 1)",
				"$v = 1l;\nALTER TABLE `t` ADD COLUMN `name` Utf8",
			},
		},
		{
			name:    "postgres keeps statements",
			dialect: platform.Postgres,
			want: []string{
				"CREATE TABLE `t` (id Int64 NOT NULL, PRIMARY KEY (id))",
				"$v = 1l",
				"UPSERT INTO `t` (id) VALUES ($v)",
				"UPSERT INTO `t` (id) VALUES ($v + 1)",
				"ALTER TABLE `t` ADD COLUMN `name` Utf8",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(splitSQLStatementsForDialect(ydbBody, test.dialect), qt.DeepEquals, test.want)
			c.Assert(migrationStatementCountForDialect(ydbBody, test.dialect), qt.Equals, len(test.want))
		})
	}
}

// A unit's source is what partial hashes digest, so the source units have to
// line up with the executed ones: one each, in order.
func TestSourceStatementsForDialect_FollowTheUnits(t *testing.T) {
	c := qt.New(t)

	sources := sourceStatementsForDialect(ydbBody, platform.YDB)

	texts := make([]string, 0, len(sources))
	for _, source := range sources {
		texts = append(texts, source.Text)
	}
	c.Assert(texts, qt.DeepEquals, []string{
		"CREATE TABLE `t` (id Int64 NOT NULL, PRIMARY KEY (id));",
		"$v = 1l;\nUPSERT INTO `t` (id) VALUES ($v);\nUPSERT INTO `t` (id) VALUES ($v + 1);",
		"ALTER TABLE `t` ADD COLUMN `name` Utf8;",
	})
	c.Assert(sources, qt.HasLen, len(splitSQLStatementsForDialect(ydbBody, platform.YDB)))
}

func TestRefuseUnsplittableSQL_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{name: "a client delimiter", body: "DELIMITER //\nDROP TABLE `t` //",
			wantErr: `client delimiter directive in YQL: "DELIMITER //" .*`},
		{name: "a translation setting Ptah cannot split by", body: "--!ansi_lexer\nDROP TABLE `t`;",
			wantErr: `unsupported YQL translation setting "--!ansi_lexer": .*`},
		{name: "a commit inside a data run", body: "UPSERT INTO `t` (id) VALUES (1l);\nCOMMIT;\nUPSERT INTO `t` (id) VALUES (2l);",
			wantErr: `"COMMIT" controls a transaction, and on YDB the migrator runs each data query in a transaction of its own; remove it`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(refuseUnsplittableSQL(platform.YDB, test.body), qt.ErrorMatches, test.wantErr)
		})
	}
}

// The control: the same bodies pass on a dialect whose splitter honors or
// ignores them, and a plain YDB body passes.
func TestRefuseUnsplittableSQL_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		body    string
	}{
		{name: "a YDB body", dialect: platform.YDB, body: ydbBody},
		{name: "a YDB body under the supported setting", dialect: platform.YDB, body: "--!syntax_v1\n" + ydbBody},
		{name: "a client delimiter on MySQL", dialect: platform.MySQL, body: "DELIMITER //\nDROP TABLE t //"},
		{name: "a commit on PostgreSQL", dialect: platform.Postgres, body: "COMMIT;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(refuseUnsplittableSQL(test.dialect, test.body), qt.IsNil)
		})
	}
}

// A committed YDB query leaves no session state to replay: every query
// carries the definitions it needs.
func TestNoTransactionResumeAction_YDBQueriesAreDurable(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "a pragma-headed scheme query", statement: "PRAGMA TablePathPrefix = \"/local/app\";\nCREATE TABLE a (id Int64 NOT NULL, PRIMARY KEY (id))"},
		{name: "a data query", statement: "$v = 1l;\nUPSERT INTO a (id) VALUES ($v)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(noTransactionResumeAction(test.statement, platform.YDB), qt.Equals, noTransactionPrefixDurable)
		})
	}
}

func TestIsYDBDataQuery(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{name: "a data query", query: "$v = 1l;\nUPSERT INTO a (id) VALUES ($v)", want: true},
		{name: "a scheme query", query: "$v = 1l;\nDROP TABLE a", want: false},
		{name: "two queries are not one data query", query: "UPSERT INTO a (id) VALUES (1l); DROP TABLE a;", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(isYDBDataQuery(test.query), qt.Equals, test.want)
		})
	}
}

// committerThatAnswers is a committer of data queries whose run records each
// statement it was handed and answers it with err.
func committerThatAnswers(ran *[]string, err error) statementCommitter {
	return statementCommitter{
		claims: isYDBDataQuery,
		run: func(_ context.Context, event StatementEvent) error {
			*ran = append(*ran, event.Statement)
			return err
		},
	}
}

// The loop asks the committer first, and a statement it claims is run and
// recorded by it alone; one it does not claim is left to the loop.
func TestCommitStatement_HappyPath(t *testing.T) {
	tests := []struct {
		name          string
		statement     string
		wantCommitted bool
		wantRan       []string
	}{
		{
			name: "a data query is committed", statement: "UPSERT INTO a (id) VALUES (1l)",
			wantCommitted: true, wantRan: []string{"UPSERT INTO a (id) VALUES (1l)"},
		},
		{name: "a scheme query is left to the loop", statement: "DROP TABLE a", wantCommitted: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var ran []string
			ctx := withStatementCommitter(context.Background(), committerThatAnswers(&ran, nil))

			committed, err := commitStatement(ctx, StatementEvent{Statement: test.statement})

			c.Assert(err, qt.IsNil)
			c.Assert(committed, qt.Equals, test.wantCommitted)
			c.Assert(ran, qt.DeepEquals, test.wantRan)
		})
	}
}

// Where no committer is installed, every statement is left to the loop.
func TestCommitStatement_WithoutCommitter(t *testing.T) {
	c := qt.New(t)

	committed, err := commitStatement(context.Background(), StatementEvent{Statement: "UPSERT INTO a (id) VALUES (1l)"})

	c.Assert(err, qt.IsNil)
	c.Assert(committed, qt.IsFalse)
}

// A committer's failure is the statement's failure, and the statement still
// counts as claimed: nothing else runs it.
func TestCommitStatement_FailurePath(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("refused")
	var ran []string
	ctx := withStatementCommitter(context.Background(), committerThatAnswers(&ran, failure))

	committed, err := commitStatement(ctx, StatementEvent{Statement: "UPSERT INTO a (id) VALUES (1l)", Index: 1, Total: 1})

	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(committed, qt.IsTrue)
	c.Assert(ran, qt.DeepEquals, []string{"UPSERT INTO a (id) VALUES (1l)"})
}

// fakeTxDriver is a database/sql driver whose connections log what they are
// asked to do, and answer each attempt's query, checkpoint and commit with the
// errors a test gives it, attempt by attempt.
type fakeTxDriver struct {
	log             []string
	queryErrors     []error
	checkpointErrs  []error
	commitErrors    []error
	queryAttempt    int
	checkpointCount int
	commitAttempt   int
}

func (d *fakeTxDriver) Connect(context.Context) (driver.Conn, error) { return fakeTxConn{d: d}, nil }
func (d *fakeTxDriver) Driver() driver.Driver                        { return nil }

type fakeTxConn struct{ d *fakeTxDriver }

func (c fakeTxConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not used") }
func (c fakeTxConn) Close() error                        { return nil }
func (c fakeTxConn) Begin() (driver.Tx, error)           { return nil, errors.New("not used") }
func (c fakeTxConn) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.d.log = append(c.d.log, fmt.Sprintf("begin isolation=%d", opts.Isolation))
	return fakeTx(c), nil
}

func (c fakeTxConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.d.log = append(c.d.log, fmt.Sprintf("exec %s (%d args)", query, len(args)))
	if query == "UPSERT data" {
		attempt := c.d.queryAttempt
		c.d.queryAttempt++
		return driver.RowsAffected(1), errorAt(c.d.queryErrors, attempt)
	}
	attempt := c.d.checkpointCount
	c.d.checkpointCount++
	return driver.RowsAffected(1), errorAt(c.d.checkpointErrs, attempt)
}

type fakeTx fakeTxConn

func (t fakeTx) Commit() error {
	t.d.log = append(t.d.log, "commit")
	attempt := t.d.commitAttempt
	t.d.commitAttempt++
	return errorAt(t.d.commitErrors, attempt)
}

func (t fakeTx) Rollback() error {
	t.d.log = append(t.d.log, "rollback")
	return nil
}

func errorAt(errs []error, attempt int) error {
	if attempt < len(errs) {
		return errs[attempt]
	}
	return nil
}

// abortedError is a serialization conflict as a SQLSTATE, which
// internal/atlasretry reads the way it reads YDB's ABORTED status.
type abortedError struct{}

func (abortedError) Error() string    { return "Transaction locks invalidated" }
func (abortedError) SQLState() string { return "40001" }

// newDataQueryCommit builds the commit under test over fake, with recorded and
// readErr answering a read-back.
func newDataQueryCommit(c *qt.C, fake *fakeTxDriver, recorded bool, readErr error) dataQueryCommit {
	c.Helper()
	db := sql.OpenDB(fake)
	c.Cleanup(func() { _ = db.Close() })
	return dataQueryCommit{
		begin: db.BeginTx,
		query: "UPSERT data",
		checkpoint: func() (string, []any) {
			fake.log = append(fake.log, "build checkpoint")
			return "UPDATE checkpoint", []any{int64(2), int64(1)}
		},
		recorded: func(context.Context) (bool, error) { return recorded, readErr },
		wait:     func(context.Context, int) error { return nil },
	}
}

// The data query and its checkpoint run in one serializable transaction, and a
// transaction YDB aborted runs again as a whole.
func TestDataQueryCommit_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		commitErrors []error
		recorded     bool
		wantLog      []string
	}{
		{
			name: "one transaction",
			wantLog: []string{
				"begin isolation=6", "exec UPSERT data (0 args)", "build checkpoint", "exec UPDATE checkpoint (2 args)", "commit",
			},
		},
		{
			name:         "an aborted commit runs the transaction again",
			commitErrors: []error{abortedError{}},
			wantLog: []string{
				"begin isolation=6", "exec UPSERT data (0 args)", "build checkpoint", "exec UPDATE checkpoint (2 args)", "commit",
				"begin isolation=6", "exec UPSERT data (0 args)", "build checkpoint", "exec UPDATE checkpoint (2 args)", "commit",
			},
		},
		{
			name:         "a commit of unknown outcome whose checkpoint is recorded",
			commitErrors: []error{errors.New("connection reset")},
			recorded:     true,
			wantLog: []string{
				"begin isolation=6", "exec UPSERT data (0 args)", "build checkpoint", "exec UPDATE checkpoint (2 args)", "commit",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fake := &fakeTxDriver{commitErrors: test.commitErrors}

			err := newDataQueryCommit(c, fake, test.recorded, nil).run(context.Background())

			c.Assert(err, qt.IsNil)
			c.Assert(fake.log, qt.DeepEquals, test.wantLog)
		})
	}
}

// A failure leaves the transaction uncommitted: a failing query or checkpoint
// is rolled back, so neither is applied without the other, and a commit of
// unknown outcome whose checkpoint is not recorded is reported.
func TestDataQueryCommit_FailurePath(t *testing.T) {
	tests := []struct {
		name           string
		queryErrors    []error
		checkpointErrs []error
		commitErrors   []error
		readErr        error
		wantErr        string
		wantLog        []string
		wantCommits    int
	}{
		{
			name:        "the query fails",
			queryErrors: []error{errors.New("Conflict with existing key")},
			wantErr:     "Conflict with existing key",
			wantLog:     []string{"begin isolation=6", "exec UPSERT data (0 args)", "rollback"},
			wantCommits: 0,
		},
		{
			name:           "the checkpoint fails",
			checkpointErrs: []error{errors.New("no such column")},
			wantErr:        "record the query in the revision table: no such column",
			wantLog: []string{
				"begin isolation=6", "exec UPSERT data (0 args)", "build checkpoint", "exec UPDATE checkpoint (2 args)", "rollback",
			},
			wantCommits: 0,
		},
		{
			name:         "a commit of unknown outcome whose checkpoint is not recorded",
			commitErrors: []error{errors.New("connection reset")},
			wantErr:      "connection reset",
			wantLog: []string{
				"begin isolation=6", "exec UPSERT data (0 args)", "build checkpoint", "exec UPDATE checkpoint (2 args)", "commit",
			},
			wantCommits: 1,
		},
		{
			// Neither error says the query is not applied, and the caller
			// records the failure without lowering what the row holds; see
			// TestRevisionFailureRecord_HappyPath.
			name:         "a commit of unknown outcome whose read-back fails",
			commitErrors: []error{errors.New("connection reset")},
			readErr:      errors.New("transport: connection refused"),
			wantErr:      "connection reset\nread back whether the query committed: transport: connection refused",
			wantLog: []string{
				"begin isolation=6", "exec UPSERT data (0 args)", "build checkpoint", "exec UPDATE checkpoint (2 args)", "commit",
			},
			wantCommits: 1,
		},
		{
			name: "a conflict on every attempt",
			commitErrors: []error{abortedError{}, abortedError{}, abortedError{}, abortedError{},
				abortedError{}},
			wantErr: "Transaction locks invalidated",
			wantLog: []string{
				"begin isolation=6", "exec UPSERT data (0 args)", "build checkpoint", "exec UPDATE checkpoint (2 args)", "commit",
			},
			wantCommits: ydbTransactionAttempts,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fake := &fakeTxDriver{
				queryErrors: test.queryErrors, checkpointErrs: test.checkpointErrs, commitErrors: test.commitErrors,
			}

			err := newDataQueryCommit(c, fake, false, test.readErr).run(context.Background())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(fake.commitAttempt, qt.Equals, test.wantCommits)
			c.Assert(fake.log[:len(test.wantLog)], qt.DeepEquals, test.wantLog)
		})
	}
}

// A data query's checkpoint is recorded only by a row whose progress reaches
// the query and that holds no failure written over the checkpoint.
func TestCheckpointRecorded(t *testing.T) {
	tests := []struct {
		name     string
		revision *MigrationRevision
		want     bool
	}{
		{name: "no row", revision: nil, want: false},
		{name: "the query's own checkpoint", revision: &MigrationRevision{Applied: 3}, want: true},
		{name: "a later checkpoint", revision: &MigrationRevision{Applied: 4}, want: true},
		{name: "the row as it was before the query", revision: &MigrationRevision{Applied: 2}, want: false},
		{
			name:     "a failure written over the checkpoint",
			revision: &MigrationRevision{Applied: 3, Error: "rpc error: code = Unavailable"},
			want:     false,
		},
		{
			name:     "a statement marked in flight",
			revision: &MigrationRevision{Applied: 3, Error: unknownStatementOutcomeError},
			want:     false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(checkpointRecorded(test.revision, 3), qt.Equals, test.want)
		})
	}
}

// failureRecordUnderTest is a [revisionFailureRecord] over fake whose read
// answers each attempt with the next of revisions and readErrs, and whose
// statement records the progress it was asked to write.
type failureRecordUnderTest struct {
	record  revisionFailureRecord
	written []int
}

func newFailureRecord(
	c *qt.C,
	fake *fakeTxDriver,
	applied int,
	revisions []*MigrationRevision,
	readErrs []error,
) *failureRecordUnderTest {
	c.Helper()
	db := sql.OpenDB(fake)
	c.Cleanup(func() { _ = db.Close() })
	under := &failureRecordUnderTest{}
	reads := 0
	under.record = revisionFailureRecord{
		begin: db.BeginTx,
		read: func(context.Context, *sql.Tx) (*MigrationRevision, error) {
			attempt := reads
			reads++
			fake.log = append(fake.log, "read")
			var revision *MigrationRevision
			if attempt < len(revisions) {
				revision = revisions[attempt]
			}
			return revision, errorAt(readErrs, attempt)
		},
		statement: func(applied int) (string, []any) {
			under.written = append(under.written, applied)
			return "UPDATE failed", []any{int64(applied)}
		},
		applied: applied,
		wait:    func(context.Context, int) error { return nil },
	}
	return under
}

// The failure is written in one serializable transaction with a read of the
// row, and records the larger of the progress the failure states and the
// progress the row holds. A transaction YDB aborts runs again and reads the
// row again.
func TestRevisionFailureRecord_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		revisions    []*MigrationRevision
		readErrs     []error
		commitErrors []error
		wantWritten  []int
		wantLog      []string
	}{
		{
			name:        "the row holds what the failure states",
			revisions:   []*MigrationRevision{{Applied: 2}},
			wantWritten: []int{2},
			wantLog:     []string{"begin isolation=6", "read", "exec UPDATE failed (1 args)", "commit"},
		},
		{
			name:        "the row holds a query whose commit answer was lost",
			revisions:   []*MigrationRevision{{Applied: 3}},
			wantWritten: []int{3},
			wantLog:     []string{"begin isolation=6", "read", "exec UPDATE failed (1 args)", "commit"},
		},
		{
			name:        "no row",
			wantWritten: []int{2},
			wantLog:     []string{"begin isolation=6", "read", "exec UPDATE failed (1 args)", "commit"},
		},
		{
			name:         "the lost commit lands after the read",
			revisions:    []*MigrationRevision{{Applied: 2}, {Applied: 3}},
			commitErrors: []error{abortedError{}},
			wantWritten:  []int{2, 3},
			wantLog: []string{
				"begin isolation=6", "read", "exec UPDATE failed (1 args)", "commit",
				"begin isolation=6", "read", "exec UPDATE failed (1 args)", "commit",
			},
		},
		{
			name:        "a read YDB aborted",
			revisions:   []*MigrationRevision{nil, {Applied: 3}},
			readErrs:    []error{abortedError{}},
			wantWritten: []int{3},
			wantLog: []string{
				"begin isolation=6", "read", "rollback",
				"begin isolation=6", "read", "exec UPDATE failed (1 args)", "commit",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fake := &fakeTxDriver{commitErrors: test.commitErrors}
			under := newFailureRecord(c, fake, 2, test.revisions, test.readErrs)

			err := under.record.run(context.Background())

			c.Assert(err, qt.IsNil)
			c.Assert(under.written, qt.DeepEquals, test.wantWritten)
			c.Assert(fake.log, qt.DeepEquals, test.wantLog)
		})
	}
}

// A row that cannot be read gets nothing written: it already records what the
// body committed, and a resume reads it.
func TestRevisionFailureRecord_FailurePath(t *testing.T) {
	tests := []struct {
		name           string
		readErrs       []error
		checkpointErrs []error
		commitErrors   []error
		wantErr        string
		wantLog        []string
		wantCommits    int
	}{
		{
			name:     "the row cannot be read",
			readErrs: []error{errors.New("transport: connection refused")},
			wantErr: "read the revision before recording the failure over it: " +
				"transport: connection refused",
			wantLog:     []string{"begin isolation=6", "read", "rollback"},
			wantCommits: 0,
		},
		{
			name:           "the write fails",
			checkpointErrs: []error{errors.New("no such column")},
			wantErr:        "no such column",
			wantLog:        []string{"begin isolation=6", "read", "exec UPDATE failed (1 args)", "rollback"},
			wantCommits:    0,
		},
		{
			name: "a conflict on every attempt",
			commitErrors: []error{abortedError{}, abortedError{}, abortedError{}, abortedError{},
				abortedError{}},
			wantErr:     "Transaction locks invalidated",
			wantLog:     []string{"begin isolation=6", "read", "exec UPDATE failed (1 args)", "commit"},
			wantCommits: ydbTransactionAttempts,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fake := &fakeTxDriver{checkpointErrs: test.checkpointErrs, commitErrors: test.commitErrors}
			under := newFailureRecord(c, fake, 2, []*MigrationRevision{{Applied: 3}}, test.readErrs)

			err := under.record.run(context.Background())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(fake.commitAttempt, qt.Equals, test.wantCommits)
			c.Assert(fake.log[:len(test.wantLog)], qt.DeepEquals, test.wantLog)
		})
	}
}
