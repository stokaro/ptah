package dbcli_test

import (
	"context"
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

	const timeout = 250 * time.Millisecond
	before := time.Now()
	ctx, cancel := dbcli.ConnectContext(parent, timeout)
	defer cancel()

	deadline, ok := ctx.Deadline()
	c.Assert(ok, qt.IsTrue, qt.Commentf("expected a deadline when timeout > 0"))

	remaining := time.Until(deadline)
	c.Assert(remaining > 0, qt.IsTrue, qt.Commentf("deadline must be in the future, got %s", remaining))
	// Deadline = before + timeout (within scheduling jitter). Allow a generous
	// upper bound so CI's noisy clocks don't flake.
	c.Assert(deadline.Sub(before) <= timeout+50*time.Millisecond, qt.IsTrue,
		qt.Commentf("deadline %s exceeds expected %s + slack", deadline.Sub(before), timeout))
	c.Assert(remaining <= timeout, qt.IsTrue,
		qt.Commentf("remaining %s exceeds requested timeout %s", remaining, timeout))
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

func TestConnectContext_NegativeTimeout(t *testing.T) {
	c := qt.New(t)
	parent := context.Background()
	ctx, cancel := dbcli.ConnectContext(parent, -1*time.Second)
	defer cancel()

	// Negative timeouts are treated the same as zero — no deadline imposed.
	_, ok := ctx.Deadline()
	c.Assert(ok, qt.IsFalse)
	c.Assert(ctx, qt.Equals, parent)
}
