package agentapi_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/agentapi"
	"go.5x5.cz/ptah/internal/agentaudit"
	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/agenttarget"
)

// TestSession_NoSurfaceCarriesTheOperatorsPassword sweeps every read-only
// answer a session gives for the one string that must never appear in any of
// them.
//
// One path was measured before this: a failed connection's error message. That
// is the sharpest place a password can leak and it is not the only one. A
// session description names the databases it can reach, an audit record names
// what was decided about them, and both are read by somebody other than the
// operator -- the description by a model, the record by whoever reads the log
// afterwards.
//
// The assertion is over the whole rendered answer rather than over named
// fields, because a field added later is exactly the field nobody thinks to
// check (stokaro/ptah#1490).
//
// The three subtests are not equally sharp today, and saying so is part of the
// measurement. The description and the error are answers that DO carry the
// target, so a leak there is a live risk. The audit record carries no target at
// all -- measured: its twenty-one fields name the session, the operation, the
// capability and the artifact, and nothing names a database (stokaro/ptah#2138).
// That subtest is therefore a guard on the field somebody adds when they close
// that issue, not a check on a present hazard, and a mutant that puts the URL
// into the record's summary survives because the summary is not recorded.
func TestSession_NoSurfaceCarriesTheOperatorsPassword(t *testing.T) {
	c := qt.New(t)
	var audit bytes.Buffer
	url := "postgres://ptah_user:" + agentProbePassword + "@127.0.0.1:1/nope?sslmode=disable"
	session := sessionOptions{
		targets: []agenttarget.Config{{Name: "probe", URL: url, Class: agentpolicy.ClassEphemeral}},
		audit:   auditRecorder(c, &audit),
	}.build(c)

	described, err := session.DescribeSession(c.Context(), agentapi.DescribeSessionRequest{})
	c.Assert(err, qt.IsNil)
	_, readErr := session.ReadDatabase(c.Context(), agentapi.ReadDatabaseRequest{})

	// The read has to have been attempted, or the audit record below is about a
	// call that never happened.
	c.Assert(readErr, qt.IsNotNil)

	answers := map[string]string{
		"describe_session":    renderJSON(c, described),
		"read_database error": readErr.Error(),
		"audit log":           audit.String(),
	}
	for name, answer := range answers {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(answer, qt.Not(qt.Contains), agentProbePassword)
		})
	}
}

// TestSession_TheSweepLooksAtSomethingThatMentionsTheDatabase is the control.
//
// Every assertion above is a "does not contain". A session that answered
// nothing, an audit recorder that wrote nothing, or a fixture whose target was
// never configured would satisfy all three and measure none of them. This
// asserts each answer names the database it was asked about, by the identity
// that is safe to carry.
func TestSession_TheSweepLooksAtSomethingThatMentionsTheDatabase(t *testing.T) {
	c := qt.New(t)
	var audit bytes.Buffer
	url := "postgres://ptah_user:" + agentProbePassword + "@127.0.0.1:1/nope?sslmode=disable"
	session := sessionOptions{
		targets: []agenttarget.Config{{Name: "probe", URL: url, Class: agentpolicy.ClassEphemeral}},
		audit:   auditRecorder(c, &audit),
	}.build(c)

	described, err := session.DescribeSession(c.Context(), agentapi.DescribeSessionRequest{})
	c.Assert(err, qt.IsNil)
	_, readErr := session.ReadDatabase(c.Context(), agentapi.ReadDatabaseRequest{})
	c.Assert(readErr, qt.IsNotNil)

	c.Assert(renderJSON(c, described), qt.Contains, "probe")
	c.Assert(audit.String(), qt.Contains, "database.inspect")
	c.Assert(strings.TrimSpace(audit.String()), qt.Not(qt.Equals), "")
}

// renderJSON is how the far side of a transport reads an answer.
func renderJSON(c *qt.C, value any) string {
	c.Helper()
	encoded, err := json.Marshal(value)
	c.Assert(err, qt.IsNil)
	return string(encoded)
}

// auditRecorder writes JSONL into the buffer, which is where the sweep reads it.
func auditRecorder(c *qt.C, out *bytes.Buffer) agentaudit.Recorder {
	c.Helper()
	writer, err := agentaudit.NewWriter(out, agentaudit.Options{
		SessionID: "sweep", Surface: agentaudit.SurfaceMCP, PtahVersion: "test",
	})
	c.Assert(err, qt.IsNil)
	return writer
}
