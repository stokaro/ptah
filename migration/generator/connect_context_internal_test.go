package generator

// White-box testing required: connectContextFor is unexported and its only
// caller opens a database, so the derivation cannot be observed through an
// exported call without a server. What is under test is which context the
// connect runs under, which is decided before any connection is attempted.

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// TestConnectContextForSpendsTheBudgetOnTheConnect pins that ConnectTimeout
// reaches the connect and nothing else does.
//
// The assertion is on whether a deadline exists, not on whether one fires.
// A budget small enough to fire reliably does not exist: on Windows
// `time.Now()` can return the same instant either side of a nanosecond, so the
// deadline is scheduled instead of already past and a local SQLite connect
// finishes before the timer does. A test written that way passed everywhere
// except the runner this work is about (stokaro/ptah#1749).
func TestConnectContextForSpendsTheBudgetOnTheConnect(t *testing.T) {
	tests := []struct {
		name string
		// timeout is the ConnectTimeout the caller set.
		timeout time.Duration
		// wantDeadline is whether the connect must run under one.
		wantDeadline bool
	}{
		{
			name:         "a budget bounds the connect",
			timeout:      30 * time.Second,
			wantDeadline: true,
		},
		{
			name:         "zero leaves the connect bounded only by the caller",
			timeout:      0,
			wantDeadline: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			ctx, cancel := connectContextFor(
				context.Background(), GenerateMigrationOptions{ConnectTimeout: test.timeout})
			defer cancel()

			_, hasDeadline := ctx.Deadline()
			c.Assert(hasDeadline, qt.Equals, test.wantDeadline)
		})
	}
}

// TestConnectContextForLeavesTheRunUnbounded is the other half, and the defect
// this was written for: the run must not inherit the connect's deadline.
//
// The caller's context is what governs planning, rendering and publication. It
// used to be the connect budget, so a 10s default expired during file
// publication on a slow runner and was reported as
// `error creating migration files: context deadline exceeded`.
func TestConnectContextForLeavesTheRunUnbounded(t *testing.T) {
	c := qt.New(t)

	run := context.Background()
	connect, cancel := connectContextFor(run, GenerateMigrationOptions{ConnectTimeout: time.Second})
	defer cancel()

	_, connectBounded := connect.Deadline()
	c.Assert(connectBounded, qt.IsTrue)

	_, runBounded := run.Deadline()
	c.Assert(runBounded, qt.IsFalse,
		qt.Commentf("the connect budget must not reach the context the run uses"))
}
