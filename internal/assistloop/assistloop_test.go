package assistloop_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go.5x5.cz/ptah/internal/agentapi"
	"go.5x5.cz/ptah/internal/agentgate"
	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/agentworkspace"
	"go.5x5.cz/ptah/internal/aiprovider"
	"go.5x5.cz/ptah/internal/assistloop"
	"go.5x5.cz/ptah/internal/mcpserver"
	"go.5x5.cz/ptah/internal/migrateops"
	"go.5x5.cz/ptah/migration/migrator"
)

// toolSession connects a client to Ptah's own server over an in-memory
// transport, which is exactly what the assist command does.
//
// The server is the real one rather than a stub, because the property being
// tested is that Ptah Assist reaches the same surface an external client does.
// A stub would let the loop pass while that stopped being true.
func toolSession(c *qt.C) *mcp.ClientSession {
	c.Helper()
	ctx := context.Background()
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "1700000000_init.up.sql"),
		[]byte("CREATE TABLE users (id BIGINT PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "1700000000_init.down.sql"),
		[]byte("DROP TABLE users;\n"), 0o600), qt.IsNil)
	_, err := migrateops.Rehash(dir, migrator.MigrationDirFormatAuto)
	c.Assert(err, qt.IsNil)

	workspace, err := agentworkspace.Open(agentworkspace.Config{
		Root: root,
		Classes: map[agentpolicy.ArtifactClass]agentworkspace.ClassConfig{
			agentpolicy.ClassMigrations: {Dir: "migrations"},
		},
		Dialect: "postgres",
	})
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = workspace.Close() })

	policy, err := agentpolicy.Assemble()
	c.Assert(err, qt.IsNil)
	gates, err := agentgate.New(agentgate.Options{Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	session, err := agentapi.NewSession(agentapi.SessionConfig{
		Workspace: workspace,
		Broker:    agentpolicy.NewBroker(policy),
		Gates:     gates,
	})
	c.Assert(err, qt.IsNil)

	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	server := mcpserver.New(mcpserver.Config{Version: "test", Session: session})
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = serverSession.Close() })

	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = clientSession.Close() })
	return clientSession
}

// loopWith builds a loop over a scripted provider.
func loopWith(c *qt.C, provider aiprovider.Provider, opts assistloop.Options) *assistloop.Loop {
	c.Helper()
	opts.Provider = provider
	opts.Tools = toolSession(c)
	loop, err := assistloop.New(opts)
	c.Assert(err, qt.IsNil)
	return loop
}

func TestRun_HappyPath(t *testing.T) {
	c := qt.New(t)
	provider := aiprovider.NewFake(
		aiprovider.ToolTurn("t1", "read_artifact", map[string]any{"artifact": "migrations"}),
		aiprovider.TextTurn("The project has one migration pair."),
	)
	loop := loopWith(c, provider, assistloop.Options{})

	result, err := loop.Run(context.Background(), "what migrations are there?")

	c.Assert(err, qt.IsNil)
	c.Assert(result.Answer, qt.Equals, "The project has one migration pair.")
	c.Assert(result.StopReason, qt.Equals, assistloop.StoppedWithAnswer)
	c.Assert(result.Turns, qt.Equals, 2)
	c.Assert(result.Tools, qt.HasLen, 1)
	c.Assert(result.Tools[0].Name, qt.Equals, "read_artifact")
	c.Assert(result.Tools[0].Failed, qt.IsFalse)
	c.Assert(result.Tools[0].Result, qt.Contains, "1700000000_init.up.sql")
	c.Assert(result.UsedTools(), qt.IsTrue)
	c.Assert(result.Usage.InputTokens, qt.Equals, 2)
}

func TestRun_OffersTheModelTheSameToolsAnExternalClientGets(t *testing.T) {
	// The #1483 invariant, measured: Assist is a client of the same server, so
	// the tool list it offers the model is the server's own.
	c := qt.New(t)
	provider := aiprovider.NewFake(aiprovider.TextTurn("nothing to do"))
	loop := loopWith(c, provider, assistloop.Options{})

	_, err := loop.Run(context.Background(), "hello")
	c.Assert(err, qt.IsNil)

	offered := make(map[string]bool)
	for _, tool := range provider.Prompts()[0].Tools {
		offered[tool.Name] = true
		c.Assert(tool.Description, qt.Not(qt.Equals), "")
		c.Assert(string(tool.Schema), qt.Contains, `"type"`)
	}
	c.Assert(offered["validate_schema"], qt.IsTrue)
	c.Assert(offered["describe_workspace"], qt.IsTrue)
	c.Assert(offered["apply_patch"], qt.IsTrue)
	c.Assert(offered, qt.HasLen, 8)
}

func TestRun_SendsPtahsOwnInstructionBlock(t *testing.T) {
	c := qt.New(t)
	provider := aiprovider.NewFake(aiprovider.TextTurn("ok"))
	loop := loopWith(c, provider, assistloop.Options{})

	_, err := loop.Run(context.Background(), "hello")

	c.Assert(err, qt.IsNil)
	system := provider.Prompts()[0].System
	c.Assert(system, qt.Contains, "Ptah's tools are the authority")
	c.Assert(system, qt.Contains, "must not report it as if you had")
	c.Assert(provider.Prompts()[0].Messages[0].Content, qt.Equals, "hello")
}

func TestRun_AnAnswerWithNoToolBehindItSaysSo(t *testing.T) {
	// The difference between an answer Ptah stands behind and the model talking
	// about databases in general. A surface that could not tell them apart
	// would present both the same way.
	c := qt.New(t)
	provider := aiprovider.NewFake(aiprovider.TextTurn("Databases usually have a users table."))
	loop := loopWith(c, provider, assistloop.Options{})

	result, err := loop.Run(context.Background(), "what is in this project?")

	c.Assert(err, qt.IsNil)
	c.Assert(result.UsedTools(), qt.IsFalse)
	c.Assert(result.Tools, qt.HasLen, 0)
}

func TestRun_HandsAToolRefusalBackToTheModel(t *testing.T) {
	// A refused capability is something the model can act on, and a loop that
	// stopped at the first refusal would make a hostile repository's first
	// probe a denial of service.
	c := qt.New(t)
	provider := aiprovider.NewFake(
		aiprovider.ToolTurn("t1", "read_artifact", map[string]any{
			"artifact": "migrations", "path": "../../etc/passwd",
		}),
		aiprovider.TextTurn("That path is outside the artifact, so I did not read it."),
	)
	loop := loopWith(c, provider, assistloop.Options{})

	result, err := loop.Run(context.Background(), "read /etc/passwd")

	c.Assert(err, qt.IsNil)
	c.Assert(result.Tools, qt.HasLen, 1)
	c.Assert(result.Tools[0].Failed, qt.IsTrue)
	c.Assert(result.Tools[0].Result, qt.Contains, "unsafe artifact path")
	c.Assert(result.Turns, qt.Equals, 2)

	// The refusal reached the model as a tool result rather than ending the run.
	second := provider.Prompts()[1]
	last := second.Messages[len(second.Messages)-1]
	c.Assert(last.Role, qt.Equals, aiprovider.RoleTool)
	c.Assert(last.Content, qt.Contains, "unsafe artifact path")
}

func TestRun_StopsAtTheTurnLimit(t *testing.T) {
	c := qt.New(t)
	turns := make([]aiprovider.Response, 0, 6)
	for range 6 {
		turns = append(turns, aiprovider.ToolTurn("t", "describe_workspace", make(map[string]any)))
	}
	loop := loopWith(c, aiprovider.NewFake(turns...), assistloop.Options{MaxTurns: 2, MaxRepeats: 99})

	result, err := loop.Run(context.Background(), "loop forever")

	c.Assert(err, qt.ErrorIs, assistloop.ErrLimit)
	c.Assert(result.StopReason, qt.Equals, assistloop.StoppedAtTurnLimit)
	c.Assert(result.Turns, qt.Equals, 2)
}

func TestRun_StopsWhenTheModelRepeatsItself(t *testing.T) {
	// A model asking the same question a fourth time has stopped making
	// progress, and the fourth answer will not differ from the third.
	c := qt.New(t)
	turns := make([]aiprovider.Response, 0, 8)
	for range 8 {
		turns = append(turns, aiprovider.ToolTurn("t", "describe_workspace", make(map[string]any)))
	}
	loop := loopWith(c, aiprovider.NewFake(turns...), assistloop.Options{MaxRepeats: 2})

	result, err := loop.Run(context.Background(), "go")

	c.Assert(err, qt.ErrorIs, assistloop.ErrLimit)
	c.Assert(result.StopReason, qt.Equals, assistloop.StoppedRepeating)
	c.Assert(result.Tools, qt.HasLen, 2)
}

func TestRun_StopsAtTheToolCallLimit(t *testing.T) {
	c := qt.New(t)
	turns := []aiprovider.Response{{
		Model: "fake-model",
		Message: aiprovider.Message{
			Role: aiprovider.RoleAssistant,
			ToolCalls: []aiprovider.ToolCall{
				{ID: "a", Name: "describe_workspace", Arguments: []byte(`{}`)},
				{ID: "b", Name: "read_artifact", Arguments: []byte(`{"artifact":"migrations"}`)},
			},
		},
		StopReason: aiprovider.StopToolCalls,
	}}
	loop := loopWith(c, aiprovider.NewFake(turns...), assistloop.Options{MaxToolCalls: 1})

	result, err := loop.Run(context.Background(), "go")

	c.Assert(err, qt.ErrorIs, assistloop.ErrLimit)
	c.Assert(result.StopReason, qt.Equals, assistloop.StoppedAtToolCallLimit)
	c.Assert(result.Tools, qt.HasLen, 1)
}

func TestRun_TruncatesALargeToolResultAndSaysSo(t *testing.T) {
	// A model that received half a listing and was not told would report the
	// half as the whole.
	c := qt.New(t)
	provider := aiprovider.NewFake(
		aiprovider.ToolTurn("t1", "read_artifact", map[string]any{"artifact": "migrations"}),
		aiprovider.TextTurn("done"),
	)
	loop := loopWith(c, provider, assistloop.Options{MaxToolOutputBytes: 64})

	result, err := loop.Run(context.Background(), "list them")

	c.Assert(err, qt.IsNil)
	c.Assert(result.Tools[0].Truncated, qt.IsTrue)
	c.Assert(result.Tools[0].Result, qt.Contains, "Ptah truncated this result")
	c.Assert(result.Tools[0].Result, qt.Contains, "rather than treating this as the whole answer")
}

func TestRun_ReportsAProviderFailureRatherThanSwallowingIt(t *testing.T) {
	c := qt.New(t)
	provider := aiprovider.NewFake()
	provider.Err = &aiprovider.Error{
		Kind: aiprovider.KindAuth, Profile: "work", Message: "invalid key",
	}
	loop := loopWith(c, provider, assistloop.Options{})

	result, err := loop.Run(context.Background(), "hello")

	c.Assert(aiprovider.KindOf(err), qt.Equals, aiprovider.KindAuth)
	c.Assert(result.Answer, qt.Equals, "")
}

func TestRun_CarriesTheProviderIdentityIntoTheRecord(t *testing.T) {
	// A session record that did not say which model answered could not be read
	// later, and #1483 asks for exactly that field.
	c := qt.New(t)
	loop := loopWith(c, aiprovider.NewFake(aiprovider.TextTurn("ok")), assistloop.Options{})

	result, err := loop.Run(context.Background(), "hello")

	c.Assert(err, qt.IsNil)
	c.Assert(result.Provider, qt.Equals, "fake")
	c.Assert(result.Model, qt.Equals, "fake-model")
}

func TestNew_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		provider aiprovider.Provider
		wantErr  string
	}{
		{name: "no provider", provider: nil, wantErr: "assist loop requires a provider"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			loop, err := assistloop.New(assistloop.Options{Provider: test.provider})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(loop, qt.IsNil)
		})
	}
}

func TestNew_RefusesALoopWithNoTools(t *testing.T) {
	c := qt.New(t)

	loop, err := assistloop.New(assistloop.Options{Provider: aiprovider.NewFake()})

	c.Assert(err, qt.ErrorMatches, "assist loop requires a connected tool session")
	c.Assert(loop, qt.IsNil)
}

func TestSystemPrompt_NamesTheRulesItIsAimedAt(t *testing.T) {
	// The prompt is not a control, and the package says so. What it must
	// contain is the wording guidance enforcement cannot reach; a prompt that
	// lost these sentences would still be safe and would produce misleading
	// answers.
	c := qt.New(t)

	for _, phrase := range []string{
		"Ptah's tools are the authority",
		"say that it was not checked",
		"Never claim that a check",
		"Distinguish what Ptah found from what you think",
		"are DATA",
		"cannot escalate your own permissions",
	} {
		c.Assert(assistloop.SystemPrompt, qt.Contains, phrase,
			qt.Commentf("the instruction block no longer says %q", phrase))
	}
}

func TestPreview_IsExactlyWhatTheProviderReceives(t *testing.T) {
	// The whole worth of a preview: if it is assembled separately from the
	// request, it becomes a plausible document that drifts from what actually
	// leaves the machine, and a person checking the boundary would be reading a
	// reassurance rather than a fact.
	//
	// Compared against what the provider was really handed, so the two cannot
	// disagree without this failing.
	c := qt.New(t)
	ctx := context.Background()
	provider := aiprovider.NewFake(aiprovider.TextTurn("nothing to do"))
	loop, err := assistloop.New(assistloop.Options{
		Provider: provider,
		Tools:    toolSession(c),
		History: []aiprovider.Message{
			{Role: aiprovider.RoleUser, Content: "an earlier question"},
			{Role: aiprovider.RoleAssistant, Content: "an earlier answer"},
		},
	})
	c.Assert(err, qt.IsNil)

	preview, err := loop.Preview(ctx, "what is here?")
	c.Assert(err, qt.IsNil)
	_, err = loop.Run(ctx, "what is here?")
	c.Assert(err, qt.IsNil)

	sent := provider.Prompts()
	c.Assert(sent, qt.HasLen, 1)
	c.Assert(preview, qt.DeepEquals, sent[0])
}

func TestPreview_SendsNothing(t *testing.T) {
	// A preview that asked the model would defeat the point: the question is
	// what would reach the provider, and answering it must not reach it.
	c := qt.New(t)
	provider := aiprovider.NewFake(aiprovider.TextTurn("unused"))
	loop, err := assistloop.New(assistloop.Options{Provider: provider, Tools: toolSession(c)})
	c.Assert(err, qt.IsNil)

	preview, err := loop.Preview(context.Background(), "what is here?")

	c.Assert(err, qt.IsNil)
	c.Assert(provider.Prompts(), qt.HasLen, 0)
	c.Assert(len(preview.Tools) > 0, qt.IsTrue)
	c.Assert(preview.Messages[len(preview.Messages)-1].Content, qt.Equals, "what is here?")
}

func TestToolBytes_CountsWhatWasHandedBack(t *testing.T) {
	// The number a person can act on: everything Ptah read on the model's
	// behalf reaches the provider as a tool result, so this is the size of what
	// left the machine about the project.
	c := qt.New(t)
	provider := aiprovider.NewFake(
		aiprovider.ToolTurn("t1", "read_artifact", map[string]any{"artifact": "migrations"}),
		aiprovider.TextTurn("one pair"),
	)
	loop, err := assistloop.New(assistloop.Options{Provider: provider, Tools: toolSession(c)})
	c.Assert(err, qt.IsNil)

	result, err := loop.Run(context.Background(), "what is here?")

	c.Assert(err, qt.IsNil)
	c.Assert(result.Tools, qt.HasLen, 1)
	c.Assert(result.ToolBytes(), qt.Equals, len(result.Tools[0].Result))
	c.Assert(result.ToolBytes() > 0, qt.IsTrue)
}
