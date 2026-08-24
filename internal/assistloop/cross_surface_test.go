package assistloop_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go.5x5.cz/ptah/internal/aiprovider"
	"go.5x5.cz/ptah/internal/assistloop"
)

// TestRun_HandsTheModelWhatAnExternalClientWouldGet is the result half of the
// #1483 invariant, and #1490's MCP-versus-Assist box.
//
// That Assist offers the model the same tool LIST an external client gets is
// already pinned. That the model receives the same ANSWER was not: Assist calls
// the tool through the same session an external client would, so a divergence
// here would be the second implementation path the epic exists to forbid --
// and it would be invisible, because both sides would still be talking about
// the same migrations.
//
// The comparison is against the untruncated result on purpose. Truncation is
// Assist's own bound and it is stated in the text when it happens; a fixture
// small enough not to hit it is what keeps this test measuring equivalence
// rather than measuring the limit.
func TestRun_HandsTheModelWhatAnExternalClientWouldGet(t *testing.T) {
	rows := []struct {
		name      string
		tool      string
		arguments map[string]any
	}{
		{
			name:      "read_artifact",
			tool:      "read_artifact",
			arguments: map[string]any{"artifact": "migrations"},
		},
		{
			name:      "describe_session",
			tool:      "describe_session",
			arguments: make(map[string]any),
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			tools := toolSession(c)
			provider := aiprovider.NewFake(
				aiprovider.ToolTurn("t1", row.tool, row.arguments),
				aiprovider.TextTurn("done"),
			)
			loop, err := assistloop.New(assistloop.Options{Provider: provider, Tools: tools})
			c.Assert(err, qt.IsNil)

			run, err := loop.Run(context.Background(), "answer from the tools")
			c.Assert(err, qt.IsNil)

			external := externalResult(c, tools, row.tool, row.arguments)

			c.Assert(run.Tools, qt.HasLen, 1,
				qt.Commentf("the fixture did not reach the tool, so nothing below compares anything"))
			c.Assert(run.Tools[0].Failed, qt.IsFalse)
			c.Assert(run.Tools[0].Truncated, qt.IsFalse,
				qt.Commentf("the fixture outgrew the output limit; shrink it rather than comparing a prefix"))
			c.Assert(run.Tools[0].Result, qt.Equals, external)
		})
	}
}

// externalResult is what a client that is not Assist gets from the same call on
// the same session, rendered the way Assist renders one.
func externalResult(c *qt.C, session *mcp.ClientSession, tool string, arguments map[string]any) string {
	c.Helper()
	answer, err := session.CallTool(context.Background(),
		&mcp.CallToolParams{Name: tool, Arguments: arguments})
	c.Assert(err, qt.IsNil)
	c.Assert(answer.IsError, qt.IsFalse)
	encoded, err := json.Marshal(answer.StructuredContent)
	c.Assert(err, qt.IsNil)
	return string(encoded)
}
