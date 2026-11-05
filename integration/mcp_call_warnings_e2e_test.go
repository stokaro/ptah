//go:build integration

package integration_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"ptah.run/internal/clirun"
)

// `ptah mcp` is one command, so one run report lives for the whole session.
// Each tool call has a report of its own, and the warnings a call raises
// reach the agent in that call's result rather than the server's stderr when
// the session ends. The server runs as the shipped binary, because the run
// report a session starts with is the one the binary's command sets up.

// mcpLongNames is a PostgreSQL schema whose three index names the server cuts
// to 63 bytes, each distinct in its first byte.
func mcpLongNames() string {
	var sql strings.Builder
	sql.WriteString("CREATE TABLE t (a int);\n")
	for _, letter := range []string{"a", "b", "c"} {
		sql.WriteString(`CREATE INDEX "` + letter + strings.Repeat("x", 63) + `_idx" ON t (a);` + "\n")
	}
	return sql.String()
}

// TestMCPCallCarriesTheWarningsItRaisedE2E calls validate_schema twice on a
// schema with three names PostgreSQL cuts. Each result says so after its
// answer, a second call as well as the first, and the server writes no
// summary on stderr when the session ends.
func TestMCPCallCarriesTheWarningsItRaisedE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(mcpLongNames()), 0o600), qt.IsNil)
	stderr := filepath.Join(c.TempDir(), "mcp.stderr")
	errFile, err := os.Create(stderr) // #nosec G304 -- the path is c.TempDir() plus a constant name
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(errFile.Close(), qt.IsNil) })
	// #nosec G204 -- the test runs the ptah binary clirun built from this checkout
	cmd := exec.Command(clirun.Build(c, clirun.Ptah), "mcp", "--schema-source-root", dir)
	// An exported PTAH_* variable would configure the server beside the flags.
	cmd.Env = slices.DeleteFunc(os.Environ(), func(entry string) bool { return strings.HasPrefix(entry, "PTAH_") })
	cmd.Stderr = errFile
	client := mcp.NewClient(&mcp.Implementation{Name: "warnings-e2e", Version: "1"}, nil)
	session, err := client.Connect(c.Context(), &mcp.CommandTransport{Command: cmd}, nil)
	c.Assert(err, qt.IsNil)
	want := &mcp.TextContent{Text: "warning: 3 names are longer than 63 bytes; the server keeps their first 63 bytes," +
		" and they are read that way; the result's _meta lists them under ptah.run/warnings"}

	for range 2 {
		result, err := session.CallTool(c.Context(), &mcp.CallToolParams{Name: "validate_schema", Arguments: map[string]any{
			"dialect": "postgres",
			"source":  map[string]any{"schema_files": []string{filepath.Join(dir, "schema.sql")}},
		}})

		c.Assert(err, qt.IsNil)
		c.Assert(result.IsError, qt.IsFalse)
		c.Assert(result.Content, qt.HasLen, 2)
		c.Assert(result.Content[1], qt.DeepEquals, want)
		c.Assert(result.Meta["ptah.run/warnings"], qt.HasLen, 1)
	}
	c.Assert(session.Close(), qt.IsNil)
	written, err := os.ReadFile(stderr) // #nosec G304 -- the path is c.TempDir() plus a constant name
	c.Assert(err, qt.IsNil)
	c.Assert(string(written), qt.Not(qt.Contains), "warning:")
}
