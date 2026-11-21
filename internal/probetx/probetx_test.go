package probetx_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/probetx"
)

// answer is what a server says when asked to name the transaction.
type answer struct {
	name string
	err  error
}

// server answers each call with the next answer, as a server names the
// transaction a session holds each time it is asked.
func server(answers ...answer) func(context.Context) (string, error) {
	return func(context.Context) (string, error) {
		next := answers[0]
		answers = answers[1:]
		return next.name, next.err
	}
}

var errUnreachable = errors.New("connection reset")

// TestCheck_HappyPath passes a transaction the probes left as they found it,
// and checks nothing for an engine that names none.
func TestCheck_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		identify func(context.Context) (string, error)
	}{
		{name: "the same transaction", identify: server(answer{name: "17/1"}, answer{name: "17/1"})},
		// A second call would find no answer left and panic.
		{name: "an engine that names none", identify: server(answer{})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, err := probetx.Watch(context.Background(), test.identify)
			c.Assert(err, qt.IsNil)
			c.Assert(probetx.Check(ctx), qt.IsNil)
		})
	}
}

// TestCheck_UnwatchedContextPasses checks nothing outside a watch.
func TestCheck_UnwatchedContextPasses(t *testing.T) {
	c := qt.New(t)
	c.Assert(probetx.Check(context.Background()), qt.IsNil)
}

// TestCheck_FailurePath stops at a transaction a probe ended.
func TestCheck_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, err := probetx.Watch(context.Background(), server(answer{name: "17/1"}, answer{name: "18/1"}))
	c.Assert(err, qt.IsNil)

	err = probetx.Check(ctx)

	c.Assert(err, qt.ErrorIs, probetx.ErrTransactionChanged)
	c.Assert(err, qt.ErrorMatches, `a probe ended the transaction it ran in or changed how it reads statement text; statements it ran may be committed \(transaction 17/1, now 18/1\)`)
}

// TestCheck_ServerCannotBeAskedAgain answers the server's error, since a
// transaction nobody can name may be the one a probe ended.
func TestCheck_ServerCannotBeAskedAgain(t *testing.T) {
	c := qt.New(t)
	ctx, err := probetx.Watch(context.Background(), server(answer{name: "17/1"}, answer{err: errUnreachable}))
	c.Assert(err, qt.IsNil)

	c.Assert(probetx.Check(ctx), qt.ErrorIs, errUnreachable)
}

// TestWatch_FailurePath answers the server's error when the transaction cannot
// be named at the start.
func TestWatch_FailurePath(t *testing.T) {
	c := qt.New(t)
	_, err := probetx.Watch(context.Background(), server(answer{err: errUnreachable}))
	c.Assert(err, qt.ErrorIs, errUnreachable)
}

var errAborted = errors.New("current transaction is aborted")

// restores counts the calls of a restore that answers err.
type restores struct {
	calls int
	err   error
}

func (r *restores) restore(context.Context) error {
	r.calls++
	return r.err
}

// TestSettle_HappyPath names a transaction the body left failed once the
// restore makes it answer again, and restores nothing in one it can name.
func TestSettle_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name     string
		answers  []answer
		restores int
	}{
		{name: "a transaction the server names", answers: []answer{{name: "17/1"}, {name: "17/1"}}},
		{name: "a transaction the body left failed", answers: []answer{{name: "17/1"}, {err: errAborted}, {name: "17/1"}}, restores: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, err := probetx.Watch(context.Background(), server(test.answers...))
			c.Assert(err, qt.IsNil)
			var restore restores

			c.Assert(probetx.Settle(ctx, restore.restore), qt.IsNil)
			c.Assert(restore.calls, qt.Equals, test.restores)
		})
	}
}

// TestSettle_FailurePath stops at a transaction a probe ended, whether it
// answers at once, answers after the restore, or cannot be restored.
func TestSettle_FailurePath(t *testing.T) {
	t.Run("a transaction a probe began", func(t *testing.T) {
		c := qt.New(t)
		ctx, err := probetx.Watch(context.Background(), server(answer{name: "17/1"}, answer{name: "18/1"}))
		c.Assert(err, qt.IsNil)
		var restore restores

		c.Assert(probetx.Settle(ctx, restore.restore), qt.ErrorIs, probetx.ErrTransactionChanged)
		c.Assert(restore.calls, qt.Equals, 0)
	})
	t.Run("a failed transaction a probe began", func(t *testing.T) {
		c := qt.New(t)
		ctx, err := probetx.Watch(context.Background(), server(answer{name: "17/1"}, answer{err: errAborted}, answer{name: "18/1"}))
		c.Assert(err, qt.IsNil)
		var restore restores

		c.Assert(probetx.Settle(ctx, restore.restore), qt.ErrorIs, probetx.ErrTransactionChanged)
		c.Assert(restore.calls, qt.Equals, 1)
	})
	t.Run("a savepoint the transaction does not hold", func(t *testing.T) {
		c := qt.New(t)
		ctx, err := probetx.Watch(context.Background(), server(answer{name: "17/1"}, answer{err: errAborted}))
		c.Assert(err, qt.IsNil)
		restore := restores{err: errors.New(`savepoint "ptah_probe_transaction" does not exist`)}

		err = probetx.Settle(ctx, restore.restore)

		c.Assert(err, qt.ErrorIs, errAborted)
		c.Assert(err, qt.ErrorIs, restore.err)
		c.Assert(err, qt.ErrorMatches, `current transaction is aborted; the savepoint set when the transaction began `+
			`cannot be rolled back to either, so a probe may have ended the transaction: savepoint "ptah_probe_transaction" does not exist`)
	})
	t.Run("no restore", func(t *testing.T) {
		c := qt.New(t)
		ctx, err := probetx.Watch(context.Background(), server(answer{name: "17/1"}, answer{err: errAborted}))
		c.Assert(err, qt.IsNil)

		c.Assert(probetx.Settle(ctx, nil), qt.Equals, errAborted)
	})
}

// TestWatched_HappyPath tells a watched context from one [probetx.Watch] left
// unwatched, for an engine that names no transaction.
func TestWatched_HappyPath(t *testing.T) {
	c := qt.New(t)
	watched, err := probetx.Watch(context.Background(), server(answer{name: "17/1"}))
	c.Assert(err, qt.IsNil)
	unwatched, err := probetx.Watch(context.Background(), server(answer{}))
	c.Assert(err, qt.IsNil)

	c.Assert(probetx.Watched(watched), qt.IsTrue)
	c.Assert(probetx.Watched(unwatched), qt.IsFalse)
}
