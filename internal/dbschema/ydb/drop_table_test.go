package ydb_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"

	ydbschema "ptah.run/internal/dbschema/ydb"
)

// settlingDrop answers a drop from a sequence, repeating the final answer.
// A successful answer delegates to the scheme fixture so the table disappears.
type settlingDrop struct {
	*fakeDatabase
	answers []error
	calls   int
}

func (s *settlingDrop) ExecContext(ctx context.Context, statement string, args ...any) (sql.Result, error) {
	answer := s.answers[min(s.calls, len(s.answers)-1)]
	s.calls++
	if answer != nil {
		return nil, answer
	}
	return s.fakeDatabase.ExecContext(ctx, statement, args...)
}

func tableDropFixture(answers []error, kind Ydb_Scheme.Entry_Type) (*ydbschema.Writer, *settlingDrop) {
	runner := &settlingDrop{
		fakeDatabase: &fakeDatabase{tree: map[string][]*Ydb_Scheme.Entry{
			"/local":       {entry("probe", Ydb_Scheme.Entry_DIRECTORY)},
			"/local/probe": {entry("rep", kind)},
		}},
		answers: answers,
	}
	return ydbschema.NewWriterFromScheme(runner, runner.fakeDatabase, "/local", ""), runner
}

// Every cleanup path waits for a listed row table to become visible to SQL.
func TestWriter_TableDropSettles(t *testing.T) {
	tests := []struct {
		name string
		drop func(context.Context, *ydbschema.Writer) error
	}{
		{name: "directory", drop: func(ctx context.Context, w *ydbschema.Writer) error { return w.DropDirectory(ctx, "probe") }},
		{name: "database realm", drop: func(ctx context.Context, w *ydbschema.Writer) error { return w.DropDatabaseRealm(ctx) }},
		{name: "all tables", drop: func(ctx context.Context, w *ydbschema.Writer) error { return w.DropAllTables(ctx) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := qt.New(t)
				writer, runner := tableDropFixture([]error{ydbschema.ErrPathNotVisible, ydbschema.ErrPathNotVisible, nil}, Ydb_Scheme.Entry_TABLE)
				start := time.Now()

				err := test.drop(context.Background(), writer)

				c.Assert(err, qt.IsNil)
				c.Assert(runner.calls, qt.Equals, 3)
				c.Assert(time.Since(start), qt.Equals, 150*time.Millisecond)
				c.Assert(runner.executed, qt.DeepEquals, []string{"DROP TABLE `probe/rep`"})
				c.Assert(runner.removed, qt.DeepEquals, []string{"/local/probe"})
			})
		})
	}
}

// A persistent scheme refusal remains a failure, including a path hidden by
// permissions. Other failures and non-row-table statements do not wait.
func TestWriter_TableDropRefusals(t *testing.T) {
	denied := errors.New("permission denied")
	tests := []struct {
		name    string
		answers []error
		kind    Ydb_Scheme.Entry_Type
		timeout time.Duration
		want    error
		elapsed time.Duration
	}{
		{name: "persistent scheme refusal", answers: []error{ydbschema.ErrPathNotVisible}, kind: Ydb_Scheme.Entry_TABLE,
			timeout: time.Minute, want: context.DeadlineExceeded, elapsed: 10 * time.Second},
		{name: "caller deadline", answers: []error{ydbschema.ErrPathNotVisible}, kind: Ydb_Scheme.Entry_TABLE,
			timeout: 25 * time.Millisecond, want: context.DeadlineExceeded, elapsed: 25 * time.Millisecond},
		{name: "access refusal", answers: []error{denied}, kind: Ydb_Scheme.Entry_TABLE,
			timeout: time.Minute, want: denied},
		{name: "refusal after retry", answers: []error{ydbschema.ErrPathNotVisible, denied}, kind: Ydb_Scheme.Entry_TABLE,
			timeout: time.Minute, want: denied, elapsed: 50 * time.Millisecond},
		{name: "view", answers: []error{ydbschema.ErrPathNotVisible}, kind: Ydb_Scheme.Entry_VIEW,
			timeout: time.Minute, want: ydbschema.ErrPathNotVisible},
		{name: "column table", answers: []error{ydbschema.ErrPathNotVisible}, kind: Ydb_Scheme.Entry_COLUMN_TABLE,
			timeout: time.Minute, want: ydbschema.ErrPathNotVisible},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := qt.New(t)
				writer, runner := tableDropFixture(test.answers, test.kind)
				ctx, cancel := context.WithTimeout(context.Background(), test.timeout)
				defer cancel()
				start := time.Now()

				err := writer.DropDirectory(ctx, "probe")

				c.Assert(err, qt.ErrorIs, test.want)
				c.Assert(err, qt.ErrorIs, test.answers[len(test.answers)-1])
				c.Assert(time.Since(start), qt.Equals, test.elapsed)
				c.Assert(runner.removed, qt.HasLen, 0)
				c.Assert(runner.executed, qt.HasLen, 0)
			})
		})
	}
}

// Ordinary SQL execution must still report a missing table immediately.
func TestWriter_TableDropRetryIsScopedToCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := qt.New(t)
		writer, runner := tableDropFixture([]error{ydbschema.ErrPathNotVisible, nil}, Ydb_Scheme.Entry_TABLE)
		start := time.Now()

		err := writer.ExecuteSQL(context.Background(), "DROP TABLE `probe/rep`")

		c.Assert(err, qt.ErrorIs, ydbschema.ErrPathNotVisible)
		c.Assert(runner.calls, qt.Equals, 1)
		c.Assert(time.Since(start), qt.Equals, time.Duration(0))
	})
}
