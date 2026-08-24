package agentapi_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/agentapi"
	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/agenttarget"
)

func TestDescribeSession_WorksWithoutAWorkspace(t *testing.T) {
	// The discovery path a process configured without a workspace needs. One
	// that required the artifact half would leave such a caller with no way to
	// learn anything at all -- which is why the old operation could not be the
	// answer here.
	c := qt.New(t)
	root := c.TempDir()
	session := openSession(c, root)

	described, err := session.DescribeSession(context.Background(), agentapi.DescribeSessionRequest{})

	c.Assert(err, qt.IsNil)
	c.Assert(described.Workspace, qt.IsNil,
		qt.Commentf("absent rather than an empty root, which would read as an empty project"))
	c.Assert(described.SchemaSourceRoots, qt.HasLen, 1)
	c.Assert(described.Databases, qt.HasLen, 0)
	c.Assert(len(described.Capabilities) > 0, qt.IsTrue)
}

func TestDescribeSession_ReportsAuthorityAndReachabilitySeparately(t *testing.T) {
	// "database.inspect:dev ask" is a statement about what policy would permit.
	// Whether a dev database exists is a different statement. Reporting them as
	// one is how a table comes to look like a boundary it is not.
	c := qt.New(t)
	session := sessionOptions{
		roots: []string{c.TempDir()},
	}.build(c)

	described, err := session.DescribeSession(context.Background(), agentapi.DescribeSessionRequest{})

	c.Assert(err, qt.IsNil)
	c.Assert(described.Databases, qt.HasLen, 0,
		qt.Commentf("nothing is reachable"))
	c.Assert(verdictFor(c, described, "database.inspect:dev"), qt.Equals, "ask",
		qt.Commentf("and the authority to inspect a dev database is still reported"))
}

func TestDescribeSession_ReportsEveryConfiguredDatabaseWithoutItsURL(t *testing.T) {
	// What a caller needs to name a target, and nothing that would let it
	// become one.
	c := qt.New(t)
	databaseURL, _ := countingDatabase(c)
	session := sessionOptions{
		targets: []agenttarget.Config{{Name: "app", URL: databaseURL, Class: agentpolicy.ClassDev}},
	}.build(c)

	described, err := session.DescribeSession(context.Background(), agentapi.DescribeSessionRequest{})

	c.Assert(err, qt.IsNil)
	c.Assert(described.Databases, qt.HasLen, 1)
	c.Assert(described.Databases[0].Name, qt.Equals, "app")
	c.Assert(described.Databases[0].Class, qt.Equals, agentpolicy.ClassDev)

	encoded, err := json.Marshal(described)
	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Not(qt.Contains), probeUser,
		qt.Commentf("a discovery response is read by the model; the credential is not for it"))
}

func TestDescribeSession_ReportedVerdictsMatchWhatTheOperationsEnforce(t *testing.T) {
	// The invariant, stated as a comparison rather than as prose: for every
	// operation, what the table says and what the call does are the same thing.
	tests := []struct {
		name       string
		capability agentpolicy.Capability
		reported   string
		call       func(*agentapi.Session) error
	}{
		{
			name: "schema.validate denied", capability: agentpolicy.SchemaValidate,
			reported: "schema.validate",
			call: func(s *agentapi.Session) error {
				_, err := s.ValidateSchema(context.Background(),
					agentapi.ValidateSchemaRequest{Dialect: "postgres"})
				return err
			},
		},
		{
			name: "schema.render denied", capability: agentpolicy.SchemaRender,
			reported: "schema.render",
			call: func(s *agentapi.Session) error {
				_, err := s.RenderSchema(context.Background(),
					agentapi.RenderSchemaRequest{Dialect: "postgres"})
				return err
			},
		},
		{
			name: "schema.lineage denied", capability: agentpolicy.SchemaLineage,
			reported: "schema.lineage",
			call: func(s *agentapi.Session) error {
				_, err := s.SchemaLineage(context.Background(),
					agentapi.SchemaLineageRequest{Dialect: "postgres"})
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			session := sessionOptions{
				rules: []agentpolicy.Rule{{Capability: test.capability, Verdict: agentpolicy.VerdictDeny}},
				roots: []string{c.TempDir()},
			}.build(c)

			described, err := session.DescribeSession(context.Background(),
				agentapi.DescribeSessionRequest{})
			c.Assert(err, qt.IsNil)

			c.Assert(verdictFor(c, described, test.reported), qt.Equals, "deny")
			c.Assert(test.call(session), qt.ErrorAs, new(*agentpolicy.DeniedError))
		})
	}
}

func TestDescribeSession_ReportsTheWholeTableIncludingRefusals(t *testing.T) {
	// A report listing only the grants answers "nothing was granted" the same
	// way as a broken report does.
	c := qt.New(t)
	session := openSession(c, c.TempDir())

	described, err := session.DescribeSession(context.Background(), agentapi.DescribeSessionRequest{})

	c.Assert(err, qt.IsNil)
	c.Assert(verdictFor(c, described, "artifact.delete:migrations"), qt.Equals, "deny")
	c.Assert(verdictFor(c, described, "shell.execute"), qt.Equals, "deny")
	c.Assert(verdictFor(c, described, "network.arbitrary"), qt.Equals, "deny")
}

// verdictFor reads one row of the reported table.
func verdictFor(c *qt.C, described *agentapi.DescribeSessionResponse, capability string) string {
	c.Helper()
	for _, entry := range described.Capabilities {
		if entry.Capability == capability {
			return entry.Verdict
		}
	}
	c.Fatalf("no reported verdict for %q", capability)
	return ""
}
