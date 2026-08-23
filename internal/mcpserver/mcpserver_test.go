package mcpserver_test

import (
	"context"
	"os"
	"path/filepath"
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
func tools(c *qt.C, cfg mcpserver.Config) []*mcp.Tool {
	c.Helper()
	session := connect(c, cfg, nil)
	result, err := session.ListTools(context.Background(), nil)
	c.Assert(err, qt.IsNil)
	return result.Tools
}

// connect wires a client to a server over an in-memory transport and returns
// the client's session.
func connect(c *qt.C, cfg mcpserver.Config, opts *mcp.ClientOptions) *mcp.ClientSession {
	c.Helper()
	ctx := context.Background()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()

	serverSession, err := mcpserver.New(cfg).Connect(ctx, serverTransport, nil)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = serverSession.Close() })

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "1"}, opts)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = clientSession.Close() })
	return clientSession
}

// readOnlyConfig is a server started without a workspace.
func readOnlyConfig() mcpserver.Config {
	return mcpserver.Config{Version: "test"}
}

// toolRow is one tool a surface must offer.
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
	for _, tool := range tools(c, readOnlyConfig()) {
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

// TestServer_WithoutAWorkspaceOffersNoArtifactTool pins the process-level gate.
//
// The artifact tools are absent rather than present-and-refusing when no
// workspace was configured, because a tool list is what a client shows a person
// as "what this server can do", and four tools that can never succeed is a
// worse answer than four tools that are not there.
func TestServer_WithoutAWorkspaceOffersNoArtifactTool(t *testing.T) {
	c := qt.New(t)
	offered := make(map[string]bool)
	for _, tool := range tools(c, readOnlyConfig()) {
		offered[tool.Name] = true
	}

	for _, artifact := range []string{
		"ptah_describe_workspace",
		"ptah_read_artifact",
		"ptah_preview_patch",
		"ptah_apply_patch",
	} {
		t.Run("absent "+artifact, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(offered[artifact], qt.IsFalse)
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

	for _, tool := range tools(c, readOnlyConfig()) {
		c.Assert(strings.TrimSpace(tool.Description), qt.Not(qt.Equals), "",
			qt.Commentf("tool %q has no description", tool.Name))
		c.Assert(tool.InputSchema, qt.IsNotNil,
			qt.Commentf("tool %q exposes no input schema", tool.Name))
	}
}

// TestServer_AnnotatesEveryReadingToolAsReadOnly pins the annotation.
//
// The specification's default for destructiveHint is true, so a server that
// says nothing has its read-only tools advertised to clients as destructive.
// For a surface whose whole security argument is "every tool here reads", the
// absence of this annotation is the argument going unsaid where a client could
// have acted on it.
func TestServer_AnnotatesEveryReadingToolAsReadOnly(t *testing.T) {
	c := qt.New(t)

	for _, tool := range tools(c, readOnlyConfig()) {
		c.Assert(tool.Annotations, qt.IsNotNil,
			qt.Commentf("tool %q carries no annotations", tool.Name))
		c.Assert(tool.Annotations.ReadOnlyHint, qt.IsTrue,
			qt.Commentf("tool %q is not annotated read-only", tool.Name))
	}
}

// TestServer_MarksTheDatabaseReadAsOpenWorld pins the one reading tool that
// dials an address the caller chose, which is a different exposure from the
// three that read declared files.
func TestServer_MarksTheDatabaseReadAsOpenWorld(t *testing.T) {
	c := qt.New(t)
	open := make(map[string]bool)
	for _, tool := range tools(c, readOnlyConfig()) {
		c.Assert(tool.Annotations.OpenWorldHint, qt.IsNotNil)
		open[tool.Name] = *tool.Annotations.OpenWorldHint
	}

	c.Assert(open["ptah_read_database"], qt.IsTrue)
	c.Assert(open["ptah_validate_schema"], qt.IsFalse)
	c.Assert(open["ptah_render_schema"], qt.IsFalse)
	c.Assert(open["ptah_schema_lineage"], qt.IsFalse)
}

// TestServer_StatesTheContractInItsInstructions pins that a client can read
// which contract it is talking to without calling a tool.
//
// ContractVersion() has existed since the first release and was never sent
// anywhere: a client had no way to read it at all.
func TestServer_StatesTheContractInItsInstructions(t *testing.T) {
	c := qt.New(t)
	session := connect(c, readOnlyConfig(), nil)

	result := session.InitializeResult()

	c.Assert(result.Instructions, qt.Contains, "Ptah agent contract")
	c.Assert(result.Instructions, qt.Contains, "Every tool here reads")
}

// TestServer_ReportsAnOperationFailureAsAResultRatherThanAProtocolError pins
// the shape a failing call takes.
//
// A schema that will not load is something the caller asked about, and an agent
// can act on a message it can read. Answering with a protocol error would look
// like the server broke, and a client would retry rather than report.
//
// The arguments are schema-valid and semantically wrong on purpose. An earlier
// version of this test omitted a required argument, so the SDK refused the call
// at input validation and Ptah's own error path was never entered -- the test
// passed without measuring the behavior it names.
func TestServer_ReportsAnOperationFailureAsAResultRatherThanAProtocolError(t *testing.T) {
	c := qt.New(t)
	session := connect(c, readOnlyConfig(), nil)

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "ptah_render_schema",
		Arguments: map[string]any{
			"dialect": "orackle",
			"source":  map[string]any{"root_dirs": []string{"."}},
		},
	})

	c.Assert(err, qt.IsNil, qt.Commentf("a failed operation must not surface as a protocol error"))
	c.Assert(result.IsError, qt.IsTrue)
	c.Assert(textOf(c, result), qt.Contains, `unknown dialect "orackle"`)
	c.Assert(result.StructuredContent, qt.IsNil,
		qt.Commentf("a failed call must not also carry a well-formed empty answer"))
}

// textOf returns a tool result's text content.
func textOf(c *qt.C, result *mcp.CallToolResult) string {
	c.Helper()
	parts := make([]string, 0, len(result.Content))
	for _, content := range result.Content {
		text, isText := content.(*mcp.TextContent)
		c.Assert(isText, qt.IsTrue, qt.Commentf("unexpected content type %T", content))
		parts = append(parts, text.Text)
	}
	return strings.Join(parts, "\n")
}

// TestServer_ReturnsStructuredContentForASuccessfulCall pins the property the
// surface has been getting for free and never asserted.
//
// The SDK derives an output schema from the operation's response type and
// attaches the value as structured content. A refactor that returned `any`, or
// built results by hand, would delete both and every other test here would
// still pass.
func TestServer_ReturnsStructuredContentForASuccessfulCall(t *testing.T) {
	c := qt.New(t)
	session := connect(c, readOnlyConfig(), nil)
	dir := c.TempDir()
	c.Assert(writeFile(dir, "models.go", bookshop), qt.IsNil)

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "ptah_validate_schema",
		Arguments: map[string]any{
			"dialect": "postgres",
			"source":  map[string]any{"root_dirs": []string{dir}},
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.IsError, qt.IsFalse, qt.Commentf("%s", textOf(c, result)))
	c.Assert(result.StructuredContent, qt.IsNotNil)
}

// TestContractVersion_NamesTheContract pins that the adapter reports which
// contract it speaks.
func TestContractVersion_NamesTheContract(t *testing.T) {
	c := qt.New(t)

	c.Assert(mcpserver.ContractVersion(), qt.Contains, "ptah/")
}

// bookshop is a schema that loads, for the tests that need a successful call.
const bookshop = `package models

//ptah:schema:table name="authors"
type Author struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string
}
`

// writeFile puts one file in a directory.
func writeFile(dir, name, content string) error {
	return os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600)
}
