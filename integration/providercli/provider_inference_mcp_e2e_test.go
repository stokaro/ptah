//go:build integration

package providercli_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"ptah.run/internal/agentapi"
	"ptah.run/internal/embedreport"
	"ptah.run/internal/exeext"
)

// buildPtah builds ptah from this checkout. `ptah mcp` speaks the protocol on
// the standard streams of its own process, so the server has to run as one.
func buildPtah(c *qt.C) string {
	c.Helper()
	binary := filepath.Join(c.TempDir(), "ptah"+exeext.Suffix)
	// #nosec G204 -- the test builds this checkout's own command.
	output, err := exec.CommandContext(c.Context(), "go", "build", "-o", binary, "ptah.run/cmd/ptah").CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("go build:\n%s", output))
	return binary
}

// recordingManifest writes a manifest that selects the fixture's build and
// records the process it starts in a PID file of its own, so a test can tell
// which manifest started a provider and whether one started at all.
func (f inferenceFixture) recordingManifest(c *qt.C, name string) providerCLI {
	c.Helper()
	recorded := f.cli
	dir := c.TempDir()
	recorded.manifestPath = filepath.Join(dir, name+".json")
	recorded.pidPath = filepath.Join(dir, name+".pid")
	recorded.selectProvider(c, f.spec)
	return recorded
}

// mcpServer is one `ptah mcp` process and a client connected to it.
type mcpServer struct {
	session *mcp.ClientSession
	stderr  string
}

// serveMCP starts `ptah mcp` for the fixture's database as an operator would
// configure it, with args added, and connects a client over the process's
// standard streams. The specification directory is the one source root.
func (f inferenceFixture) serveMCP(c *qt.C, binary, specDir string, args ...string) mcpServer {
	c.Helper()
	arguments := append([]string{
		"mcp", "--database-url", f.cli.address, "--database-name", "articles", "--database-class", "dev",
		"--allow-database-inspect", "allow", "--schema-source-root", specDir,
	}, args...)
	stderr := filepath.Join(c.TempDir(), "mcp.stderr")
	file, err := os.Create(stderr)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(file.Close(), qt.IsNil) })
	cmd := exec.Command(binary, arguments...) // #nosec G204 -- the test runs the ptah binary it built
	// An exported PTAH_* variable would configure the server beside the flags.
	cmd.Env = slices.DeleteFunc(os.Environ(), func(entry string) bool { return strings.HasPrefix(entry, "PTAH_") })
	cmd.Stderr = file
	client := mcp.NewClient(&mcp.Implementation{Name: "providercli-e2e", Version: "1"}, nil)
	session, err := client.Connect(c.Context(), &mcp.CommandTransport{Command: cmd}, nil)
	server := mcpServer{session: session, stderr: stderr}
	c.Assert(err, qt.IsNil, qt.Commentf("%s", server.diagnostics(c)))
	return server
}

// call calls one tool and returns its result.
func (s mcpServer) call(c *qt.C, tool string, arguments map[string]any) *mcp.CallToolResult {
	c.Helper()
	result, err := s.session.CallTool(c.Context(), &mcp.CallToolParams{Name: tool, Arguments: arguments})
	c.Assert(err, qt.IsNil, qt.Commentf("%s", s.diagnostics(c)))
	return result
}

// decode reads a successful result's structured content into the response
// type of its tool.
func (s mcpServer) decode(c *qt.C, result *mcp.CallToolResult, into any) {
	c.Helper()
	c.Assert(result.IsError, qt.IsFalse, qt.Commentf("%s\n%s", resultText(c, result), s.diagnostics(c)))
	encoded, err := json.Marshal(result.StructuredContent)
	c.Assert(err, qt.IsNil)
	c.Assert(json.Unmarshal(encoded, into), qt.IsNil)
}

// close ends the session, which ends the server process.
func (s mcpServer) close(c *qt.C) {
	c.Helper()
	c.Assert(s.session.Close(), qt.IsNil, qt.Commentf("%s", s.diagnostics(c)))
}

// diagnostics is what the server wrote on stderr so far.
func (s mcpServer) diagnostics(c *qt.C) string {
	c.Helper()
	data, err := os.ReadFile(s.stderr)
	c.Assert(err, qt.IsNil)
	return string(data)
}

// resultText joins the text a tool result carries.
func resultText(c *qt.C, result *mcp.CallToolResult) string {
	c.Helper()
	parts := make([]string, 0, len(result.Content))
	for _, content := range result.Content {
		text, isText := content.(*mcp.TextContent)
		c.Assert(isText, qt.IsTrue, qt.Commentf("unexpected content type %T", content))
		parts = append(parts, text.Text)
	}
	return strings.Join(parts, "\n")
}

// factsOf indexes a plan's facts by name.
func factsOf(plan embedreport.Plan) map[string]embedreport.Fact {
	facts := make(map[string]embedreport.Fact, len(plan.Facts))
	for _, fact := range plan.Facts {
		facts[fact.Name] = fact
	}
	return facts
}

// TestMCPInferenceOnAProviderEngineE2E serves the inference tools of `ptah
// mcp` for a database only a provider serves, with the provider the operator
// named when starting the server. The plan is measured through the provider's
// embedding store, the status reads a run the native CLI recorded there, and
// the provider process ends with the session.
func TestMCPInferenceOnAProviderEngineE2E(t *testing.T) {
	c := qt.New(t)
	f := newInferenceFixture(c)
	binary := buildPtah(c)
	spec := f.writeSpec(c, inferenceSpec{column: "embedding", revision: "1"})
	operator := f.recordingManifest(c, "operator")

	server := f.serveMCP(c, binary, filepath.Dir(spec), "--provider-config", operator.manifestPath)

	var planned agentapi.InferencePlanResponse
	server.decode(c, server.call(c, "inference_plan", map[string]any{"spec": spec}), &planned)
	facts := factsOf(planned.Plan)
	c.Assert(facts["source.estimated_rows"], qt.DeepEquals,
		embedreport.Fact{Name: "source.estimated_rows", Value: "5", Provenance: "measured"})
	c.Assert(facts["target.exists"].Value, qt.Equals, "false")
	c.Assert(facts["target.exists"].Provenance, qt.Equals, "measured")
	c.Assert(facts["target.capability.vector_type"].Value, qt.Equals, "true")
	c.Assert(facts["target.capability.vector_type"].Provenance, qt.Equals, "measured")
	// The fixture builds no index, and the plan holds every database to one.
	c.Assert(facts["target.capability.vector_index"].Provenance, qt.Equals, "unsupported")

	// The native CLI records a run through its own provider process; the
	// server's provider reads it from the same database.
	f.succeed(c, "prepare", spec, "--run-id", "first")
	c.Assert(f.succeed(c, "backfill", spec, "--run-id", "first", "--batch-rows", "2"), qt.Contains,
		"backfill finished: 5 scanned, 5 embedded, 0 skipped")
	var status agentapi.InferenceStatusResponse
	server.decode(c, server.call(c, "inference_status", map[string]any{"run_id": "first"}), &status)
	c.Assert(status.Status.RunID, qt.Equals, "first")
	c.Assert(status.Status.Generation, qt.Equals, planned.Plan.Desired)
	// The source is declared immutable, so a finished backfill is caught up.
	c.Assert(status.Status.Phase, qt.Equals, "caught_up")
	c.Assert(status.Status.Progress.RowsScanned, qt.Equals, int64(5))
	c.Assert(status.Status.Progress.RowsEmbedded, qt.Equals, int64(5))
	c.Assert(status.Status.SnapshotDone, qt.IsTrue)

	server.close(c)
	operator.assertStopped(c)
}

// TestMCPReadDatabaseOnAProviderEngineE2E reads the catalog of a database only
// a provider serves through `ptah mcp`: the provider the operator named serves
// the configured database to every tool that reads it, not only to the
// inference tools. The native CLI creates the tables through the same build,
// because the fixture inspects only tables it can render back exactly, and
// the build is the fixture's base program, which takes the session lock an
// apply needs.
func TestMCPReadDatabaseOnAProviderEngineE2E(t *testing.T) {
	c := qt.New(t)
	build := fixtureSpec(c, "provider")
	f := inferenceFixture{cli: newProviderCLI(c, build), spec: build}
	binary := buildPtah(c)
	f.cli.plan(c)
	f.cli.success(c, "apply", "--plan", f.cli.planPath, "--tx-mode", "none", "--auto-approve", "--json")
	operator := f.recordingManifest(c, "operator")

	server := f.serveMCP(c, binary, c.TempDir(), "--provider-config", operator.manifestPath)

	var read agentapi.ReadDatabaseResponse
	server.decode(c, server.call(c, "read_database", make(map[string]any)), &read)
	c.Assert(read.Dialect, qt.Equals, "fixture")
	c.Assert(read.Objects, qt.DeepEquals, []agentapi.DatabaseObject{
		{Kind: "table", Name: "items", Columns: 1},
		{Kind: "table", Name: "second", Columns: 1},
	})
	server.close(c)
	operator.assertStopped(c)
}

// TestMCPInferenceProviderIsNotTheClientsToNameE2E is the control: a client
// cannot name a manifest. An argument that does is refused before the
// operation runs, on a server with a provider and on one without, and the
// manifest it names starts no process. A server started without a provider
// does not reach the fixture's database through one either, so the provider
// in the test above came from the operator's flag.
func TestMCPInferenceProviderIsNotTheClientsToNameE2E(t *testing.T) {
	c := qt.New(t)
	f := newInferenceFixture(c)
	binary := buildPtah(c)
	spec := f.writeSpec(c, inferenceSpec{column: "embedding", revision: "1"})
	operator := f.recordingManifest(c, "operator")
	client := f.recordingManifest(c, "client")

	for _, row := range []struct {
		name string
		args []string
	}{
		{name: "a server with the operator's provider", args: []string{"--provider-config", operator.manifestPath}},
		{name: "a server without a provider"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			server := f.serveMCP(c, binary, filepath.Dir(spec), row.args...)

			refused := server.call(c, "inference_plan", map[string]any{"spec": spec, "provider_config": client.manifestPath})

			c.Assert(refused.IsError, qt.IsTrue)
			c.Assert(resultText(c, refused), qt.Contains, `validating "arguments"`)
			c.Assert(resultText(c, refused), qt.Contains, "provider_config")
			server.close(c)
			_, err := os.Stat(client.pidPath)
			c.Assert(err, qt.ErrorIs, os.ErrNotExist)
		})
	}

	// Nothing but the operator's flag routes the database to a provider: the
	// URL alone takes the built-in PostgreSQL branch, whose driver cannot
	// read a fixture address.
	t.Run("a server without a provider does not reach the fixture's database", func(t *testing.T) {
		c := qt.New(t)
		server := f.serveMCP(c, binary, filepath.Dir(spec))

		planned := server.call(c, "inference_plan", map[string]any{"spec": spec})
		status := server.call(c, "inference_status", map[string]any{"run_id": "first"})

		c.Assert(planned.IsError, qt.IsTrue)
		c.Assert(resultText(c, planned), qt.Contains, "database_unreachable: connect: ")
		c.Assert(status.IsError, qt.IsTrue)
		c.Assert(resultText(c, status), qt.Contains, "database_unreachable: connect: ")
		server.close(c)
	})
}
