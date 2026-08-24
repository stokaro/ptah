package mcpserver_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go.5x5.cz/ptah/internal/agentapi"
	"go.5x5.cz/ptah/internal/mcpserver"
)

// TestServer_AnswersWhatTheDirectCallAnswers is the cross-surface equivalence
// #1490 asks for, on the half that needs no database.
//
// The surfaces are not two implementations: an MCP tool wraps the session
// method of the same name. That is exactly why nothing measured it. The
// existing assertions cover the tool LIST -- names, descriptions, schemas
// (internal/assistloop: TestRun_OffersTheModelTheSameToolsAnExternalClientGets)
// -- and, on this side, only that a successful call carries some structured
// content at all. Neither says the two surfaces answer the same thing, so a
// wrapper that dropped a field, reordered a list, or renamed a key would be a
// silent divergence between what an agent is told through MCP and what Ptah
// itself computed.
//
// Equality is asserted over the JSON both sides are read as, because that is
// the form the far side actually receives.
func TestServer_AnswersWhatTheDirectCallAnswers(t *testing.T) {
	tests := []struct {
		name   string
		tool   string
		direct func(*agentapi.Session, context.Context, agentapi.SchemaSource) (any, error)
	}{
		{
			name: "validate_schema",
			tool: "validate_schema",
			direct: func(s *agentapi.Session, ctx context.Context, source agentapi.SchemaSource) (any, error) {
				return s.ValidateSchema(ctx, agentapi.ValidateSchemaRequest{
					Source: source, Dialect: "postgres",
				})
			},
		},
		{
			name: "render_schema",
			tool: "render_schema",
			direct: func(s *agentapi.Session, ctx context.Context, source agentapi.SchemaSource) (any, error) {
				return s.RenderSchema(ctx, agentapi.RenderSchemaRequest{
					Source: source, Dialect: "postgres",
				})
			},
		},
		{
			name: "schema_lineage",
			tool: "schema_lineage",
			direct: func(s *agentapi.Session, ctx context.Context, source agentapi.SchemaSource) (any, error) {
				return s.SchemaLineage(ctx, agentapi.SchemaLineageRequest{
					Source: source, Dialect: "postgres",
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			dir := c.TempDir()
			c.Assert(writeFile(dir, "models.go", bookshop), qt.IsNil)

			// One session reached two ways, so a divergence would have to be
			// introduced deliberately rather than merely allowed. The source
			// directory is the configured scope: a schema is read from where
			// the operator said and nowhere else.
			cfg := readOnlyConfig(c, dir)

			answer, err := test.direct(sessionOf(c, cfg), ctx,
				agentapi.SchemaSource{RootDirs: []string{dir}})
			c.Assert(err, qt.IsNil)

			result, callErr := connect(c, cfg, nil).CallTool(ctx, &mcp.CallToolParams{
				Name: test.tool,
				Arguments: map[string]any{
					"dialect": "postgres",
					"source":  map[string]any{"root_dirs": []string{dir}},
				},
			})

			c.Assert(callErr, qt.IsNil)
			c.Assert(result.IsError, qt.IsFalse, qt.Commentf("%s", textOf(c, result)))
			c.Assert(result.StructuredContent, qt.DeepEquals, asJSON(c, answer))
		})
	}
}

// asJSON reads a value the way the far side of the transport reads it, so the
// two answers are compared in one form rather than across two.
func asJSON(c *qt.C, value any) any {
	c.Helper()
	encoded, err := json.Marshal(value)
	c.Assert(err, qt.IsNil)
	var decoded any
	c.Assert(json.Unmarshal(encoded, &decoded), qt.IsNil)
	return decoded
}

// sessionOf is the session a configuration carries, for the direct half of a
// cross-surface comparison.
func sessionOf(c *qt.C, cfg mcpserver.Config) *agentapi.Session {
	c.Helper()
	c.Assert(cfg.Session, qt.IsNotNil)
	return cfg.Session
}
