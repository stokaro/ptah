package devclean_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devclean"
)

// endsWithin reports whether ctx is done within limit, so a context that never
// ends fails its test instead of hanging it.
func endsWithin(ctx context.Context, limit time.Duration) bool {
	timer := time.NewTimer(limit)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return true
	case <-timer.C:
		return false
	}
}

// TestCleanupContext_OutlivesTheGraceWhileTheCallerIsLive pins what the
// cleanup after a successful replay needs: a cleanup that takes longer than
// the grace is not cut off while the command is still waiting for it.
func TestCleanupContext_OutlivesTheGraceWhileTheCallerIsLive(t *testing.T) {
	c := qt.New(t)
	cleanupCtx, release := devclean.CleanupContext(c.Context(), 10*time.Millisecond)
	defer release()

	time.Sleep(100 * time.Millisecond)

	c.Assert(cleanupCtx.Err(), qt.IsNil)
	_, hasDeadline := cleanupCtx.Deadline()
	c.Assert(hasDeadline, qt.IsFalse)
}

// TestCleanupContext_SurvivesTheCancellationForTheGrace pins that a canceled
// command still cleans up: the cleanup is not canceled with its caller.
func TestCleanupContext_SurvivesTheCancellationForTheGrace(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(c.Context())
	cleanupCtx, release := devclean.CleanupContext(ctx, time.Hour)
	defer release()

	cancel()
	time.Sleep(50 * time.Millisecond)

	c.Assert(cleanupCtx.Err(), qt.IsNil)
}

// TestCleanupContext_EndsTheGraceAfterTheCallerGivesUp pins the bound: once
// the caller is done, the cleanup gets the grace and no more.
func TestCleanupContext_EndsTheGraceAfterTheCallerGivesUp(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(c.Context())
	cleanupCtx, release := devclean.CleanupContext(ctx, 20*time.Millisecond)
	defer release()

	cancel()

	c.Assert(endsWithin(cleanupCtx, 5*time.Second), qt.IsTrue)
	c.Assert(cleanupCtx.Err(), qt.ErrorIs, context.Canceled)
}

// TestCleanupContext_GraceStartsAtOnceForACallerAlreadyDone covers the
// cleanup after a replay the caller has already abandoned.
func TestCleanupContext_GraceStartsAtOnceForACallerAlreadyDone(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	cleanupCtx, release := devclean.CleanupContext(ctx, 20*time.Millisecond)
	defer release()

	c.Assert(endsWithin(cleanupCtx, 5*time.Second), qt.IsTrue)
}

// TestCleanupContext_ReleaseEndsIt pins that the release function ends the
// context, so a caller's defer leaves nothing running.
func TestCleanupContext_ReleaseEndsIt(t *testing.T) {
	c := qt.New(t)
	cleanupCtx, release := devclean.CleanupContext(c.Context(), time.Hour)

	release()
	release()

	c.Assert(cleanupCtx.Err(), qt.ErrorIs, context.Canceled)
}
