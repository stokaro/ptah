package mcpserver_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go.5x5.cz/ptah/internal/mcpserver"
)

// tools lists what the server offers, by driving a real client over an
// in-memory transport rather than by reading the registration code.
//
// The point is that a tool is only exposed if the protocol says so: a
// registration that fails to take would be invisible to a test that inspected
// the source.
func tools(c *qt.C) []*mcp.Tool {
	c.Helper()
	ctx := context.Background()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()

	server := mcpserver.New("test")
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	c.Assert(err, qt.IsNil)
	defer serverSession.Close()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	c.Assert(err, qt.IsNil)
	defer clientSession.Close()

	result, err := clientSession.ListTools(ctx, nil)
	c.Assert(err, qt.IsNil)
	return result.Tools
}

// toolRow is one tool the read-only surface must offer.
type toolRow struct {
	name  string
	owner string
}

// TestServer_OffersExactlyTheReadOnlyOperations pins the surface both ways.
//
// The names are checked because they are a contract an external client codes
// against. The absence is checked because it is the security property: three of
// Ptah's reading verbs need a scratch database the CLI resets destructively,
// and exposing one here would put a destructive capability behind a read-only
// name (ADR 0002).
func TestServer_OffersExactlyTheReadOnlyOperations(t *testing.T) {
	c := qt.New(t)
	offered := make(map[string]bool)
	for _, tool := range tools(c) {
		offered[tool.Name] = true
	}

	rows := []toolRow{
		{name: "ptah_validate_schema", owner: "internal/schemavalidate"},
		{name: "ptah_render_schema", owner: "core/renderer"},
		{name: "ptah_schema_lineage", owner: "internal/schemalineage"},
		{name: "ptah_read_database", owner: "dbschema"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(offered[row.name], qt.IsTrue, qt.Commentf("owner: %s", row.owner))
		})
	}
	c.Assert(offered, qt.HasLen, len(rows),
		qt.Commentf("a tool added without a row here is a surface nobody reviewed"))

	for _, forbidden := range []string{"ptah_inspect_schema", "ptah_diff_schema", "ptah_lint_migrations"} {
		t.Run("absent "+forbidden, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(offered[forbidden], qt.IsFalse,
				qt.Commentf("this operation needs a scratch database the CLI resets destructively"))
		})
	}
}

// TestServer_DescribesEveryToolItOffers pins that a client sees what a tool
// does before calling it.
//
// A model chooses a tool from its description. An empty one makes the choice a
// guess, and a guessed schema operation is the kind that reaches a database.
func TestServer_DescribesEveryToolItOffers(t *testing.T) {
	c := qt.New(t)

	for _, tool := range tools(c) {
		c.Assert(strings.TrimSpace(tool.Description), qt.Not(qt.Equals), "",
			qt.Commentf("tool %q has no description", tool.Name))
		c.Assert(tool.InputSchema, qt.IsNotNil,
			qt.Commentf("tool %q exposes no input schema", tool.Name))
	}
}

// TestServer_ReportsAnOperationFailureAsAResultRatherThanAProtocolError pins
// the shape a failing call takes.
//
// A schema that will not load is something the caller asked about, and an agent
// can act on a message it can read. Answering with a protocol error would look
// like the server broke, and a client would retry rather than report.
func TestServer_ReportsAnOperationFailureAsAResultRatherThanAProtocolError(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()

	serverSession, err := mcpserver.New("test").Connect(ctx, serverTransport, nil)
	c.Assert(err, qt.IsNil)
	defer serverSession.Close()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	c.Assert(err, qt.IsNil)
	defer clientSession.Close()

	result, err := clientSession.CallTool(ctx, &mcp.CallToolParams{
		Name:      "ptah_render_schema",
		Arguments: map[string]any{"dialect": "postgres"},
	})

	c.Assert(err, qt.IsNil, qt.Commentf("a failed operation must not surface as a protocol error"))
	c.Assert(result.IsError, qt.IsTrue)
}

// TestContractVersion_NamesTheContract pins that a client can read which
// contract it is talking to without calling a tool.
func TestContractVersion_NamesTheContract(t *testing.T) {
	c := qt.New(t)

	c.Assert(mcpserver.ContractVersion(), qt.Contains, "ptah/")
}
