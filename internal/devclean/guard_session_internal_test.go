package devclean

// White-box testing required: validateSession runs only inside
// NewDevReplayGuardFor, which needs a live connection, and the case under test
// is a session read that fails, which no live server can be made to do on
// demand. These give the guard a session guard that answers as the test says.

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devclean/replay"
)

// answeringSessionGuard is a replay guard that reads the session and answers
// with err, except under trust, where it reads nothing and accepts.
type answeringSessionGuard struct {
	err error
}

func (answeringSessionGuard) Validate(string, replay.Scope) error { return nil }

func (g answeringSessionGuard) ValidateSession(_ context.Context, _ replay.Session, scope replay.Scope) error {
	if scope.TrustedSQL {
		return nil
	}
	return g.err
}

var errSessionUnreadable = errors.New("connection reset")

// A session the guard refuses ends with the trust remedy, because trusting
// the SQL lifts that refusal.
func TestReplayGuard_ValidateSession_FailurePathRefusalNamesTrust(t *testing.T) {
	c := qt.New(t)
	guard := &ReplayGuard{
		guard:       answeringSessionGuard{err: &replay.SessionRefusal{Reason: "the session reads latin1"}},
		trustRemedy: "trust remedy",
	}

	err := guard.validateSession(c.Context(), nil)

	c.Assert(err, qt.ErrorMatches, `the session reads latin1; trust remedy`)
}

// A session the guard cannot read is reported as it failed, with no trust
// remedy: trusting the SQL does not mend a connection.
func TestReplayGuard_ValidateSession_FailurePathUnreadableSessionNamesNoTrust(t *testing.T) {
	c := qt.New(t)
	guard := &ReplayGuard{
		guard:       answeringSessionGuard{err: errSessionUnreadable},
		trustRemedy: "trust remedy",
	}

	err := guard.validateSession(c.Context(), nil)

	c.Assert(err, qt.ErrorIs, errSessionUnreadable)
	c.Assert(err, qt.ErrorMatches, `connection reset`)
}
