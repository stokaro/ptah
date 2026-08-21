// Package mcpserver exposes Ptah's read-only agent operations to external AI
// clients over the Model Context Protocol.
//
// It is an adapter and nothing more. Every tool here forwards to
// internal/agentapi and adds no schema semantics of its own, which is the
// invariant stokaro/ptah#1483 states: Ptah Assist and this server must be two
// consumers of one contract rather than two implementations.
//
// Transport is stdio, decided in ADR 0002. A remote transport brings
// authentication, which is a security surface a first release does not open
// (stokaro/ptah#1492).
//
// # Every tool here reads
//
// Nothing in this package writes to a database, a migration directory or a
// file. That is a property of the operation set rather than of the adapter, and
// it is asserted in the tests: a tool added later that reaches past agentapi
// has to explain itself to a failing test rather than to nobody.
package mcpserver

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go.5x5.cz/ptah/internal/agentapi"
)

// serverName is what a client sees in its tool list.
const serverName = "ptah"

// New builds the server with every read-only tool registered.
//
// The version is the caller's, so a client can tell which Ptah it is driving
// rather than which version of this package.
func New(version string) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    serverName,
		Version: version,
	}, nil)
	register(server)
	return server
}

// Run serves the protocol over stdio until the session ends or the context is
// cancelled.
//
// A session the client finishes and then closes returns nil. A client that
// vanishes mid-session returns the SDK's error, and that difference is worth
// keeping: the second is a client that died, and reporting it as a clean exit
// would hide the one case an operator wants to see in a log.
//
// Measured rather than assumed, because an earlier revision tried to smooth the
// second case away: the SDK reports it as `server is closing: EOF`, whose chain
// is a wrapped internal wire error. The EOF appears only in the formatted text,
// so errors.Is(err, io.EOF) is false and mcp.ErrConnectionClosed is not in the
// chain either. There is no supported way to recognize it, and matching the
// string would break on a wording change -- so the case is left alone rather
// than handled by something that looks like handling.
func Run(ctx context.Context, version string) error {
	return New(version).Run(ctx, &mcp.StdioTransport{})
}

// register adds every tool. It is separate from New so a test can build the
// server and read its tool list without running a transport.
func register(server *mcp.Server) {
	mcp.AddTool(server, &mcp.Tool{
		Name: "ptah_validate_schema",
		Description: "Report structural problems in a declared Ptah schema for one target dialect, " +
			"without touching a database. Answers whether a schema is sound before anything is applied.",
	}, wrap(agentapi.ValidateSchema))

	mcp.AddTool(server, &mcp.Tool{
		Name: "ptah_render_schema",
		Description: "Render the DDL a declared Ptah schema becomes for one target dialect, " +
			"in the order the statements must run. Reads nothing and applies nothing.",
	}, wrap(agentapi.RenderSchema))

	mcp.AddTool(server, &mcp.Tool{
		Name: "ptah_schema_lineage",
		Description: "Trace which base columns feed each view column in a declared Ptah schema. " +
			"Answers what breaks if a column is dropped, before the drop. Views whose bodies " +
			"cannot be resolved are reported rather than omitted.",
	}, wrap(agentapi.SchemaLineage))

	mcp.AddTool(server, &mcp.Tool{
		Name: "ptah_read_database",
		Description: "Read the schema a live database currently holds: its dialect, version and objects. " +
			"Opens a connection, reads catalogs, and runs no DDL.",
	}, wrap(agentapi.ReadDatabase))
}

// wrap adapts one agent operation to the protocol's handler shape.
//
// The operation's own request type is the tool's input schema -- the SDK derives
// it from the Go type -- so the contract a client sees and the contract the
// operation takes cannot drift apart. Writing the schemas by hand is what would
// let them.
func wrap[Req any, Res any](
	operation func(context.Context, Req) (*Res, error),
) func(context.Context, *mcp.CallToolRequest, Req) (*mcp.CallToolResult, *Res, error) {
	return func(ctx context.Context, _ *mcp.CallToolRequest, req Req) (*mcp.CallToolResult, *Res, error) {
		result, err := operation(ctx, req)
		if err != nil {
			// The error is returned as a tool result rather than as a protocol
			// error: a schema that will not load or a database that will not
			// answer is something the caller asked about, and an agent can act
			// on a message it can read. A protocol error would look like the
			// server broke.
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{&mcp.TextContent{Text: err.Error()}},
			}, nil, nil
		}
		return nil, result, nil
	}
}

// ContractVersion is the agent contract this server speaks, so a client can
// check it without calling a tool.
func ContractVersion() string {
	return fmt.Sprintf("%s/%s", serverName, agentapi.Version)
}
