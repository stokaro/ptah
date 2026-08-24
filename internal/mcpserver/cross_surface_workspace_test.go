package mcpserver_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go.5x5.cz/ptah/internal/agentapi"
	"go.5x5.cz/ptah/internal/agentpolicy"
)

// TestServer_AnswersWhatTheDirectCallAnswersWithAWorkspace extends the
// cross-surface equivalence of #1490 to the two reads a workspace adds.
//
// The three operations already compared take a schema source and no workspace.
// describe_session and read_artifact take the workspace instead, and they are
// the two an agent leans on hardest: the first is what it calls to learn what it
// may do, and the second is what it calls before composing any patch. A wrapper
// that dropped a field from either would leave the agent working from a
// different picture than Ptah has, and nothing would say so.
//
// Both are reads, so calling each twice is safe and the second call cannot see
// an effect of the first.
func TestServer_AnswersWhatTheDirectCallAnswersWithAWorkspace(t *testing.T) {
	tests := []struct {
		name      string
		tool      string
		arguments map[string]any
		direct    func(context.Context, *agentapi.Session) (any, error)
	}{
		{
			name:      "describe_session",
			tool:      "describe_session",
			arguments: make(map[string]any),
			direct: func(ctx context.Context, s *agentapi.Session) (any, error) {
				return s.DescribeSession(ctx, agentapi.DescribeSessionRequest{})
			},
		},
		{
			name:      "read_artifact, a whole directory",
			tool:      "read_artifact",
			arguments: map[string]any{"artifact": "migrations"},
			direct: func(ctx context.Context, s *agentapi.Session) (any, error) {
				return s.ReadArtifact(ctx, agentapi.ReadArtifactRequest{
					Artifact: agentpolicy.ClassMigrations,
				})
			},
		},
		{
			name:      "read_artifact, one file",
			tool:      "read_artifact",
			arguments: map[string]any{"artifact": "migrations", "path": "1700000000_init.up.sql"},
			direct: func(ctx context.Context, s *agentapi.Session) (any, error) {
				return s.ReadArtifact(ctx, agentapi.ReadArtifactRequest{
					Artifact: agentpolicy.ClassMigrations,
					Path:     "1700000000_init.up.sql",
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)

			answer, err := test.direct(ctx, sessionOf(c, fixture.config))
			c.Assert(err, qt.IsNil)

			result, callErr := connect(c, fixture.config, nil).CallTool(ctx,
				&mcp.CallToolParams{Name: test.tool, Arguments: test.arguments})

			c.Assert(callErr, qt.IsNil)
			c.Assert(result.IsError, qt.IsFalse, qt.Commentf("%s", textOf(c, result)))
			c.Assert(result.StructuredContent, qt.DeepEquals, asJSON(c, answer))
		})
	}
}
