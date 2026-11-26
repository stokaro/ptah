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
	// cannot read the run's statements as session reads them, with a
	// [*SessionRefusal], and returns another error when it cannot read
	// session.
	ValidateSession(ctx context.Context, session Session, scope Scope) error
}

// SessionRefusal is the error [SessionGuard.ValidateSession] returns for a
// session it refuses. It is distinct from a failure to read the session, so a
// caller offers a remedy that lifts the refusal, such as trusting the SQL,
// only where there is a refusal to lift.
type SessionRefusal struct {
	// Reason says why the guard cannot read statements as the session does,
	// and how to connect so that it can.
	Reason string
}

func (e *SessionRefusal) Error() string { return e.Reason }

// Is reports whether target is [ErrRefused].
func (e *SessionRefusal) Is(target error) bool { return target == ErrRefused }
