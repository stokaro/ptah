package sqlserverreplay_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devclean/replay"
)

// identityChanges holds SETUSER in each form. The dev session answers 0 for
// the database o, as it does once SETUSER has made it a user o has no user
// for, so a name of o passes and the identity change is what the guard
// refuses: a bare SETUSER goes back to the login, which reaches o.
var identityChanges = []struct{ name, statement string }{
	{name: "to a user", statement: "SETUSER 'appu'"},
	{name: "to a user named in Unicode, kept", statement: "SETUSER N'appu' WITH NORESET"},
	{name: "lower case", statement: "setuser 'appu'"},
	{name: "a bare SETUSER", statement: "SETUSER"},
	{name: "a bare SETUSER before a write", statement: "IF 1 = 1 BEGIN SETUSER INSERT INTO o.dbo.kept VALUES (88) END"},
	{name: "after another statement", statement: "INSERT INTO dbo.items VALUES (1); SETUSER 'appu'"},
}

// identitySession is a dev session in thisdb that does not reach o.
func identitySession() databaseSession {
	return databaseSession{database: "thisdb", access: map[string]any{"o": int64(0)}, asked: new([]string)}
}

// TestGuard_RefusesSETUSER replays each form without trust.
func TestGuard_RefusesSETUSER(t *testing.T) {
	for _, test := range identityChanges {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := guardFor(c, identitySession()).Validate(test.statement, replay.Scope{})

			c.Assert(err, qt.ErrorIs, replay.ErrRefused)
			c.Assert(err, qt.ErrorMatches, refusal("SETUSER"))
		})
	}
}

// TestGuard_TrustedSQLRunsSETUSER is the control for the refusals above: a run
// that trusts its SQL replays every form, so the refusal comes from SETUSER
// and from nothing else in the statement.
func TestGuard_TrustedSQLRunsSETUSER(t *testing.T) {
	for _, test := range identityChanges {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(guardFor(c, identitySession()).Validate(test.statement, replay.Scope{TrustedSQL: true}), qt.IsNil)
		})
	}
}

// The refusal of SETUSER says why, and that trust lifts it.
func TestGuard_RefusesSETUSEROnlyWithoutTrust(t *testing.T) {
	c := qt.New(t)

	err := guardFor(c, identitySession()).Validate("SETUSER 'appu'", replay.Scope{})

	c.Assert(err, qt.ErrorMatches, `sqlserver migration replay rejects SETUSER because it changes the user the session runs as, `+
		`and with it the databases the session reaches; it runs only when the replayed SQL is trusted`)
}
