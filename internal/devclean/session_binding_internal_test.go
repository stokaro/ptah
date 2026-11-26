package devclean

// White-box testing required: a run binds its guard to the session it
// replays on inside NewDevReplayGuardFor, which needs a live connection, and
// the realm, trust and dialect tests of devclean_test decide what a MySQL or
// MariaDB statement naming an account gets, which depends on the session's
// account. This file gives those tests the binding step on a fake session,
// in the test binary only.

import (
	"context"

	"ptah.run/internal/devclean/replay"
)

// BindSessionForTest is [ReplayGuard.forSession], for the tests of
// devclean_test.
func (g *ReplayGuard) BindSessionForTest(ctx context.Context, session replay.Session) (*ReplayGuard, error) {
	return g.forSession(ctx, session)
}
