package mcpserver_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go.5x5.cz/ptah/internal/agentapi"
	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/agenttarget"
	"go.5x5.cz/ptah/internal/mcpserver"
)

// surfaceFixture is one session reached three ways.
//
// The three surfaces are not three implementations: Ptah Assist connects a
// client to this same server over an in-memory transport, and the server calls
// the same session an in-process caller does. The point of driving all three is
// that a divergence would have to be introduced deliberately, and this fails
// when somebody does.
type surfaceFixture struct {
	session *agentapi.Session
	client  *mcp.ClientSession
	assist  *mcp.ClientSession
}

// newSurfaces builds one session and reaches it through every surface.
func newSurfaces(c *qt.C, rules []agentpolicy.Rule, targets ...agenttarget.Config) surfaceFixture {
	c.Helper()
	layers := make([]agentpolicy.LayerRules, 0, 1)
	if len(rules) > 0 {
		layers = append(layers, agentpolicy.LayerRules{
			Layer: agentpolicy.LayerInvocation, Source: "test", Rules: rules,
		})
	}
	policy, err := agentpolicy.Assemble(layers...)
	c.Assert(err, qt.IsNil)

	built := make([]*agenttarget.Target, 0, len(targets))
	for _, cfg := range targets {
		target, targetErr := agenttarget.New(cfg)
		c.Assert(targetErr, qt.IsNil)
		built = append(built, target)
	}
	set, err := agenttarget.NewSet(built...)
	c.Assert(err, qt.IsNil)

	session, err := agentapi.NewSession(agentapi.SessionConfig{
		Broker:      agentpolicy.NewBroker(policy),
		Targets:     set,
		SourceRoots: []string{c.TempDir()},
	})
	c.Assert(err, qt.IsNil)

	cfg := mcpserver.Config{Version: "test", Session: session}
	return surfaceFixture{
		session: session,
		client:  connect(c, cfg, nil),
		// Assist's path is a second client onto the same server, which is what
		// cmd/assist builds. A private route would show up here as a surface
		// that answered differently.
		assist: connect(c, cfg, nil),
	}
}

func TestSurfaces_ADeniedDatabaseReadIsRefusedEverywhere(t *testing.T) {
	c := qt.New(t)
	surfaces := newSurfaces(c, nil, agenttarget.Config{
		Name: "app", URL: "postgres://u@127.0.0.1:1/app", Class: agentpolicy.ClassProduction,
	})

	_, direct := surfaces.session.ReadDatabase(context.Background(), agentapi.ReadDatabaseRequest{})
	c.Assert(direct, qt.ErrorAs, new(*agentpolicy.DeniedError))

	for name, client := range map[string]*mcp.ClientSession{
		"mcp": surfaces.client, "assist": surfaces.assist,
	} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			result, err := client.CallTool(context.Background(), &mcp.CallToolParams{
				Name: "read_database", Arguments: make(map[string]any),
			})

			c.Assert(err, qt.IsNil)
			c.Assert(result.IsError, qt.IsTrue)
			c.Assert(textOf(c, result), qt.Contains, "database.inspect")
		})
	}
}

func TestSurfaces_ADeniedSchemaOperationIsRefusedEverywhere(t *testing.T) {
	c := qt.New(t)
	surfaces := newSurfaces(c, []agentpolicy.Rule{
		{Capability: agentpolicy.SchemaValidate, Verdict: agentpolicy.VerdictDeny},
	})

	_, direct := surfaces.session.ValidateSchema(context.Background(),
		agentapi.ValidateSchemaRequest{Dialect: "postgres"})
	c.Assert(direct, qt.ErrorAs, new(*agentpolicy.DeniedError))

	for name, client := range map[string]*mcp.ClientSession{
		"mcp": surfaces.client, "assist": surfaces.assist,
	} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			result, err := client.CallTool(context.Background(), &mcp.CallToolParams{
				Name: "validate_schema",
				Arguments: map[string]any{
					"dialect": "postgres",
					"source":  map[string]any{"root_dirs": []string{"."}},
				},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(result.IsError, qt.IsTrue)
			c.Assert(textOf(c, result), qt.Contains, "schema.validate")
		})
	}
}

func TestSurfaces_DiscoveryAgreesAcrossSurfaces(t *testing.T) {
	// Not "the same tool names are listed" -- the same answer. A surface that
	// reported a different policy would be a surface a person could be told
	// the wrong thing by.
	c := qt.New(t)
	surfaces := newSurfaces(c, nil, agenttarget.Config{
		Name: "app", URL: "postgres://u@127.0.0.1:1/app", Class: agentpolicy.ClassDev,
	})

	direct, err := surfaces.session.DescribeSession(context.Background(),
		agentapi.DescribeSessionRequest{})
	c.Assert(err, qt.IsNil)
	fromDirect, err := json.Marshal(direct)
	c.Assert(err, qt.IsNil)

	for name, client := range map[string]*mcp.ClientSession{
		"mcp": surfaces.client, "assist": surfaces.assist,
	} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			result := structured(c, call(c, client, "describe_session", make(map[string]any)))

			overWire, marshalErr := json.Marshal(result)
			c.Assert(marshalErr, qt.IsNil)
			c.Assert(equalJSON(c, overWire, fromDirect), qt.IsTrue,
				qt.Commentf("wire: %s\ndirect: %s", overWire, fromDirect))
		})
	}
}

// equalJSON compares two documents by value rather than by spelling.
func equalJSON(c *qt.C, left, right []byte) bool {
	c.Helper()
	var a, b any
	c.Assert(json.Unmarshal(left, &a), qt.IsNil)
	c.Assert(json.Unmarshal(right, &b), qt.IsNil)
	remarshalledA, err := json.Marshal(a)
	c.Assert(err, qt.IsNil)
	remarshalledB, err := json.Marshal(b)
	c.Assert(err, qt.IsNil)
	return string(remarshalledA) == string(remarshalledB)
}
