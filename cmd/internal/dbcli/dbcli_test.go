package dbcli_test

import (
	"context"
	"errors"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
)

func TestParseConnectTimeout(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    time.Duration
		wantErr bool
		errSub  string
	}{
		{name: "default value", raw: "10s", want: 10 * time.Second},
		{name: "zero disables timeout", raw: "0", want: 0},
		{name: "minutes", raw: "2m", want: 2 * time.Minute},
		{name: "fractional", raw: "1500ms", want: 1500 * time.Millisecond},
		{name: "empty", raw: "", wantErr: true, errSub: "invalid --connect-timeout"},
		{name: "garbage", raw: "soon", wantErr: true, errSub: "invalid --connect-timeout"},
		{name: "negative", raw: "-1s", wantErr: true, errSub: "must not be negative"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := dbcli.ParseConnectTimeout(tt.raw)
			if tt.wantErr {
				c.Assert(err, qt.IsNotNil)
				c.Assert(err.Error(), qt.Contains, tt.errSub)
				return
			}
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

func TestConnectContext_PositiveTimeout(t *testing.T) {
	c := qt.New(t)
	parent := context.Background()
	ctx, cancel := dbcli.ConnectContext(parent, 50*time.Millisecond)
	defer cancel()

	deadline, ok := ctx.Deadline()
	c.Assert(ok, qt.IsTrue, qt.Commentf("expected a deadline when timeout > 0"))
	c.Assert(time.Until(deadline) <= 50*time.Millisecond, qt.IsTrue)
}

func TestConnectContext_ZeroTimeout(t *testing.T) {
	c := qt.New(t)
	parent := context.Background()
	ctx, cancel := dbcli.ConnectContext(parent, 0)
	defer cancel()

	// A zero timeout should not impose a deadline; the parent context is returned.
	_, ok := ctx.Deadline()
	c.Assert(ok, qt.IsFalse)
	c.Assert(ctx, qt.Equals, parent)
}

// nopCloser is a stub that returns a fixed error from Close so the helper's
// log path is exercised. We can't easily assert on slog output without a custom
// handler, so we just verify the helper doesn't panic on either branch.
type nopCloser struct{ err error }

func (n nopCloser) Close() error { return n.err }

func TestWarnOnClose(t *testing.T) {
	c := qt.New(t)

	// Nil closer is a no-op.
	dbcli.WarnOnClose("nothing", nil)

	// Successful close is silent.
	dbcli.WarnOnClose("ok", nopCloser{})

	// Failing close logs (and must not panic).
	dbcli.WarnOnClose("fail", nopCloser{err: errors.New("boom")})

	// If we got here without panicking, the helper behaved correctly.
	c.Assert(true, qt.IsTrue)
}
