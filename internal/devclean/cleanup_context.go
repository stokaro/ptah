package devclean

import (
	"context"
	"sync"
	"time"
)

// CleanupGrace is how long a dev-database cleanup may keep running after the
// command that started it has given up.
const CleanupGrace = 30 * time.Second

// CleanupContext returns the context a cleanup that must outlive its caller
// runs on, and the function that releases it.
//
// A cleanup after a replay, an inspection or a rehearsal has to run even when
// the command was canceled, or the dev database keeps what the run put there.
// So the returned context is not canceled with ctx. It has no deadline of its
// own while ctx is live, because how long a cleanup takes is the size of the
// schema times the round trip to the server: measured, emptying a schema of
// 697 objects took 92 s over a 21 ms link, and a fixed 30 s budget failed a
// replay that had succeeded. Once ctx is done, the cleanup gets grace to
// finish, so a canceled command still returns in bounded time.
//
// The caller must call the returned function when the cleanup is over.
func CleanupContext(ctx context.Context, grace time.Duration) (context.Context, context.CancelFunc) {
	cleanupCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	var (
		mu    sync.Mutex
		timer *time.Timer
		done  bool
	)
	stop := context.AfterFunc(ctx, func() {
		mu.Lock()
		defer mu.Unlock()
		if !done {
			timer = time.AfterFunc(grace, cancel)
		}
	})
	return cleanupCtx, func() {
		stop()
		mu.Lock()
		done = true
		if timer != nil {
			timer.Stop()
		}
		mu.Unlock()
		cancel()
	}
}
