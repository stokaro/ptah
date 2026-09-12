package mcpserver_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"ptah.run/internal/agentpolicy"
	"ptah.run/internal/mcpserver"
)

// The three tests below vary one axis each, because the question they answer --
// what scope does a preview token belong to -- has three candidate answers and
// a test that changed two things at once could not tell them apart.
//
// ADR 0009 section 2.2 states the structural fact: every tool handler closes
// over one *agentapi.Session, so a server serving several connections hands all
// of them the same session. That is right, and it is narrower than what the code
// does. The session is a field of Config, so it is shared by construction: a
// second connection shares it, and so does a second *server* built from the same
// Config. What separates two token namespaces is neither the connection nor the
// server but the session.
//
// The distinction decides ADR 0009's open question 1, which asks whether
// per-connection scoping is worth doing on its own, and says it is because "two
// concurrent `ptah assist` runs against one workspace share the same four pieces
// of state today". They do not. Each run is a process that builds its own
// session, and nothing persists a token, a grant or the live-preview count
// across processes. The sharing is real and it is latent: nothing that ships
// makes a second connection or a second server, so a remote transport would be
// the first caller to reach it.

// TestSharedSession_ASecondConnectionSpendsTheFirstConnectionsPreviewToken
// drives the first consequence in that table over the real protocol.
//
// The token is unguessable and single-use, and it carries no owner, so the
// connection that did not mint it can spend it.
func TestSharedSession_ASecondConnectionSpendsTheFirstConnectionsPreviewToken(t *testing.T) {
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)
	server := newServer(c, fixture.config)
	first, second := connectTo(c, server), connectTo(c, server)

	before := callTool(c, first, "read_artifact", map[string]any{"artifact": "migrations"})
	minted := previewAdding(c, first, before["digest"], "a")

	applied := callTool(c, second, "apply_patch", map[string]any{
		"preview_token": minted["preview_token"], "patch_id": minted["patch_id"]})

	c.Assert(applied["rolled_back"], qt.Equals, false,
		qt.Commentf("the second connection could not spend the first connection's token"))
	after := callTool(c, second, "read_artifact", map[string]any{"artifact": "migrations"})
	c.Assert(artifactPaths(c, after), qt.Contains, "17000001_a.up.sql")
}

// TestSharedSession_ASecondServerOverOneSessionSpendsTheSameToken varies the
// server while holding the session fixed, and is the test that places the
// boundary.
//
// Without it, the measurement above reads as "a server owns its tokens", which
// would make a remote transport safe as long as it built a server of its own.
// It would not: two servers constructed from one Config share the session field
// and therefore the whole token namespace.
func TestSharedSession_ASecondServerOverOneSessionSpendsTheSameToken(t *testing.T) {
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)
	first := connectTo(c, newServer(c, fixture.config))
	second := connectTo(c, newServer(c, fixture.config))

	before := callTool(c, first, "read_artifact", map[string]any{"artifact": "migrations"})
	minted := previewAdding(c, first, before["digest"], "a")

	applied := callTool(c, second, "apply_patch", map[string]any{
		"preview_token": minted["preview_token"], "patch_id": minted["patch_id"]})

	c.Assert(applied["rolled_back"], qt.Equals, false,
		qt.Commentf("a second server over the same session refused the token, so the scope is the server"))
}

// TestSharedSession_ASecondSessionDoesNotKnowTheToken varies the session and is
// the control the two tests above need.
//
// It is what makes them a statement about the session rather than about the
// process: a token minted against one session is unknown to another in the same
// process, which is why two `ptah assist` runs -- two processes, two sessions --
// share nothing.
func TestSharedSession_ASecondSessionDoesNotKnowTheToken(t *testing.T) {
	c := qt.New(t)
	first := connectTo(c, newServer(c, newWorkspace(c, agentpolicy.VerdictAllow, nil).config))
	second := connectTo(c, newServer(c, newWorkspace(c, agentpolicy.VerdictAllow, nil).config))

	before := callTool(c, first, "read_artifact", map[string]any{"artifact": "migrations"})
	minted := previewAdding(c, first, before["digest"], "a")

	refusal := callToolError(c, second, "apply_patch", map[string]any{
		"preview_token": minted["preview_token"], "patch_id": minted["patch_id"]})

	c.Assert(refusal, qt.Contains, "unknown_preview",
		qt.Commentf("a second session accepted a token it never minted"))
}

// newServer builds one server from a config.
func newServer(c *qt.C, cfg mcpserver.Config) *mcp.Server {
	c.Helper()
	server, err := mcpserver.New(cfg)
	c.Assert(err, qt.IsNil)
	return server
}

// connectTo adds one client connection to an already-built server.
//
// It is separate from connect, which builds a server per call: these tests turn
// on how many servers and how many sessions there are, so the helper that adds
// a connection must not create either.
func connectTo(c *qt.C, server *mcp.Server) *mcp.ClientSession {
	c.Helper()
	ctx := context.Background()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()

	serverSession, err := server.Connect(ctx, serverTransport, nil)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = serverSession.Close() })

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = clientSession.Close() })
	return clientSession
}
