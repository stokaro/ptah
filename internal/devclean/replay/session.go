package replay

import (
	"context"

	"ptah.run/core/dataext"
)

// Session is the dev session a run executes its statements on, as a
// [SessionGuard] reads it.
type Session interface {
	QueryRowContext(ctx context.Context, query string, args ...any) dataext.Row
}

// SessionGuard is a [Guard] whose reading of a statement holds only on a
// session that reads the statement's bytes the way the guard does. devclean
// asks it once, on the session the run executes its statements on, before it
// validates any statement for the run.
type SessionGuard interface {
	// ValidateSession refuses a run under scope on session when the guard
	// cannot read the run's statements as session reads them, and returns an
	// error when it cannot read session.
	ValidateSession(ctx context.Context, session Session, scope Scope) error
}
