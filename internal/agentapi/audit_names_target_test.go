package agentapi_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/agentapi"
	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/agenttarget"
)

// TestSession_AnAuditRecordNamesTheDatabaseItWasAbout closes stokaro/ptah#2138.
//
// The record carried the capability, which ends in the database's CLASS, and
// nothing that said which database. Two configured targets of one class
// produced indistinguishable records, so a log could not answer "which one did
// it read" -- while the page documenting that log said a database is recorded
// by "the identity and class of the configured target".
//
// The identity is the one the operator configured. It is not the URL, and the
// URL still appears in no record: that property has its own sweep, and this
// change is exactly the kind that would have broken it.
func TestSession_AnAuditRecordNamesTheDatabaseItWasAbout(t *testing.T) {
	c := qt.New(t)
	var audit bytes.Buffer
	session := sessionOptions{
		targets: []agenttarget.Config{
			{
				Name:  "primary",
				URL:   probeURL("nope"),
				Class: agentpolicy.ClassEphemeral,
			},
		},
		audit: auditRecorder(c, &audit),
	}.build(c)

	_, err := session.ReadDatabase(c.Context(), agentapi.ReadDatabaseRequest{})
	c.Assert(err, qt.IsNotNil, qt.Commentf("the read must be attempted, or no record is written"))

	record := lastAuditRecord(c, &audit)
	c.Assert(record["target"], qt.Not(qt.Equals), "")
	c.Assert(record["database_class"], qt.Equals, "ephemeral")
	c.Assert(audit.String(), qt.Not(qt.Contains), agentProbePassword,
		qt.Commentf("naming the database must not name its credential"))
}

// TestSession_TwoDatabasesOfOneClassAreToldApart is what the issue was about
// and what a single-target fixture cannot show.
//
// A record that carried a constant, or that carried the class twice, satisfies
// the test above. This reads two targets of the same class and requires their
// records to differ.
func TestSession_TwoDatabasesOfOneClassAreToldApart(t *testing.T) {
	c := qt.New(t)
	var audit bytes.Buffer
	session := sessionOptions{
		targets: []agenttarget.Config{
			{Name: "primary", URL: probeURL("a"), Class: agentpolicy.ClassEphemeral},
			{Name: "replica", URL: probeURL("b"), Class: agentpolicy.ClassEphemeral},
		},
		audit: auditRecorder(c, &audit),
	}.build(c)

	_, _ = session.ReadDatabase(c.Context(), agentapi.ReadDatabaseRequest{Target: "primary"})
	first := lastAuditRecord(c, &audit)
	_, _ = session.ReadDatabase(c.Context(), agentapi.ReadDatabaseRequest{Target: "replica"})
	second := lastAuditRecord(c, &audit)

	c.Assert(first["target"], qt.Not(qt.Equals), second["target"],
		qt.Commentf("two databases of one class produced the same record"))
	c.Assert(first["database_class"], qt.Equals, second["database_class"])
}

// TestSession_AnOperationAboutNoDatabaseNamesNone is the other side.
//
// describe_session is about the session rather than about a target, so a record
// that named one would be inventing a subject. Without this, filling the field
// unconditionally would pass every assertion above.
func TestSession_AnOperationAboutNoDatabaseNamesNone(t *testing.T) {
	c := qt.New(t)
	var audit bytes.Buffer
	session := sessionOptions{
		targets: []agenttarget.Config{
			{Name: "primary", URL: probeURL("a"), Class: agentpolicy.ClassEphemeral},
		},
		audit: auditRecorder(c, &audit),
	}.build(c)

	_, err := session.DescribeSession(c.Context(), agentapi.DescribeSessionRequest{})
	c.Assert(err, qt.IsNil)

	record := lastAuditRecord(c, &audit)
	c.Assert(record["operation"], qt.Equals, "describe_session")
	_, named := record["target"]
	c.Assert(named, qt.IsFalse, qt.Commentf("a session description named a database"))
}

// lastAuditRecord decodes the newest JSONL line.
func lastAuditRecord(c *qt.C, audit *bytes.Buffer) map[string]any {
	c.Helper()
	lines := strings.Split(strings.TrimSpace(audit.String()), "\n")
	c.Assert(lines[len(lines)-1], qt.Not(qt.Equals), "")
	var record map[string]any
	c.Assert(json.Unmarshal([]byte(lines[len(lines)-1]), &record), qt.IsNil)
	return record
}

// probeURL builds a connection string for a database nobody is listening on.
//
// Built rather than written out because a literal with a password in it is a
// hardcoded credential to gosec, and a test about not leaking credentials is a
// poor place to teach the linter to look away. The password is the same
// recognizable word the redaction tests use, so a leak here is still the
// password rather than a coincidence.
func probeURL(database string) string {
	return "postgres://ptah_user:" + agentProbePassword + "@127.0.0.1:1/" + database + "?sslmode=disable"
}
