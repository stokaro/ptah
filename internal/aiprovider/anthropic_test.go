package aiprovider_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/aiprovider"
)

// anthropicConfig is the configuration every Messages-API test uses.
func anthropicConfig(base string) aiprovider.Config {
	return aiprovider.Config{
		Profile: "work",
		BaseURL: base,
		Model:   "a-model",
		APIKey:  "not-a-real-key",
		Timeout: 5 * time.Second,
		Sleep:   func(context.Context, time.Duration) error { return nil },
	}
}

func TestAnthropic_ReadsTextAndToolUseFromOneContentList(t *testing.T) {
	// The shape that would break a leaky abstraction: a tool call is a block
	// inside the assistant's content rather than a parallel array, and text and
	// tool calls arrive in the same list.
	c := qt.New(t)
	stub := &recorder{body: `{
      "id": "msg_1",
      "model": "a-model-20260101",
      "stop_reason": "tool_use",
      "content": [
        {"type": "text", "text": "Reading the workspace."},
        {"type": "tool_use", "id": "toolu_1", "name": "ptah_describe_workspace", "input": {"detail": true}}
      ],
      "usage": {"input_tokens": 42, "output_tokens": 7, "cache_read_input_tokens": 5}
    }`}
	provider := aiprovider.NewAnthropic(anthropicConfig(serve(c, stub.handler())))

	answer, err := provider.Chat(context.Background(), aiprovider.Request{
		System:   "ptah system text",
		Messages: []aiprovider.Message{{Role: aiprovider.RoleUser, Content: "describe"}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(answer.Message.Content, qt.Equals, "Reading the workspace.")
	c.Assert(answer.Message.ToolCalls, qt.HasLen, 1)
	c.Assert(answer.Message.ToolCalls[0].ID, qt.Equals, "toolu_1")
	c.Assert(string(answer.Message.ToolCalls[0].Arguments), qt.Equals, `{"detail": true}`)
	c.Assert(answer.StopReason, qt.Equals, aiprovider.StopToolCalls)
	c.Assert(answer.Model, qt.Equals, "a-model-20260101")
	c.Assert(answer.Usage.InputTokens, qt.Equals, 42)
	c.Assert(answer.Usage.CachedInputTokens, qt.Equals, 5)
}

func TestAnthropic_SendsTheSystemInstructionAsItsOwnField(t *testing.T) {
	// The other API puts it in the message list. Both are reached from one
	// normalized Request, which is the abstraction's job.
	c := qt.New(t)
	stub := &recorder{body: `{"id":"msg_1","stop_reason":"end_turn","content":[{"type":"text","text":"ok"}]}`}
	provider := aiprovider.NewAnthropic(anthropicConfig(serve(c, stub.handler())))

	_, err := provider.Chat(context.Background(), aiprovider.Request{
		System:   "ptah system text",
		Messages: []aiprovider.Message{{Role: aiprovider.RoleUser, Content: "hello"}},
	})

	c.Assert(err, qt.IsNil)
	sent := decodeBody(c, stub.bodies[0])
	c.Assert(sent["system"], qt.Equals, "ptah system text")
	messages, _ := sent["messages"].([]any)
	c.Assert(messages, qt.HasLen, 1)
	c.Assert(stub.paths[0], qt.Equals, "/v1/messages?")
	c.Assert(stub.auth[0], qt.Equals, "||not-a-real-key")
	c.Assert(stub.requests[0].Header.Get("anthropic-version"), qt.Equals, "2023-06-01")
}

func TestAnthropic_MergesConsecutiveToolResultsIntoOneUserMessage(t *testing.T) {
	// This API has no tool role: a result is a tool_result block in a user
	// message, and two results belong in one message. A caller that produced
	// one normalized message per result must not have to know that.
	c := qt.New(t)
	stub := &recorder{body: `{"id":"msg_1","stop_reason":"end_turn","content":[{"type":"text","text":"done"}]}`}
	provider := aiprovider.NewAnthropic(anthropicConfig(serve(c, stub.handler())))

	_, err := provider.Chat(context.Background(), aiprovider.Request{
		Messages: []aiprovider.Message{
			{Role: aiprovider.RoleUser, Content: "describe and read"},
			{Role: aiprovider.RoleAssistant, ToolCalls: []aiprovider.ToolCall{
				{ID: "t1", Name: "ptah_describe_workspace", Arguments: json.RawMessage(`{}`)},
				{ID: "t2", Name: "ptah_read_artifact", Arguments: json.RawMessage(`{"artifact":"migrations"}`)},
			}},
			{Role: aiprovider.RoleTool, ToolCallID: "t1", Content: `{"artifacts":[]}`},
			{Role: aiprovider.RoleTool, ToolCallID: "t2", Content: `{"entries":[]}`},
		},
	})

	c.Assert(err, qt.IsNil)
	sent := decodeBody(c, stub.bodies[0])
	messages, _ := sent["messages"].([]any)
	c.Assert(messages, qt.HasLen, 3)

	assistant, _ := messages[1].(map[string]any)
	blocks, _ := assistant["content"].([]any)
	c.Assert(blocks, qt.HasLen, 2)
	first, _ := blocks[0].(map[string]any)
	c.Assert(first["type"], qt.Equals, "tool_use")

	results, _ := messages[2].(map[string]any)
	c.Assert(results["role"], qt.Equals, "user")
	resultBlocks, _ := results["content"].([]any)
	c.Assert(resultBlocks, qt.HasLen, 2)
	one, _ := resultBlocks[0].(map[string]any)
	c.Assert(one["type"], qt.Equals, "tool_result")
	c.Assert(one["tool_use_id"], qt.Equals, "t1")
	two, _ := resultBlocks[1].(map[string]any)
	c.Assert(two["tool_use_id"], qt.Equals, "t2")
}

func TestAnthropic_SendsAMaxTokensTheCallerDidNotState(t *testing.T) {
	// The Messages API requires max_tokens. A request that omitted it would be
	// refused, so the adapter has to have an answer and it has to be large
	// enough not to truncate ordinary replies.
	c := qt.New(t)
	stub := &recorder{body: `{"id":"msg_1","stop_reason":"end_turn","content":[{"type":"text","text":"ok"}]}`}
	provider := aiprovider.NewAnthropic(anthropicConfig(serve(c, stub.handler())))

	_, err := provider.Chat(context.Background(), aiprovider.Request{
		Messages: []aiprovider.Message{{Role: aiprovider.RoleUser, Content: "hello"}},
	})

	c.Assert(err, qt.IsNil)
	sent := decodeBody(c, stub.bodies[0])
	c.Assert(sent["max_tokens"], qt.Equals, float64(8192))
}

func TestAnthropic_ProbeMeasuresToolCalling(t *testing.T) {
	c := qt.New(t)
	stub := &recorder{body: `{"id":"msg_1","stop_reason":"tool_use","content":[
		{"type":"tool_use","id":"t1","name":"ptah_connectivity_check","input":{"ok":true}}]}`}
	provider := aiprovider.NewAnthropic(anthropicConfig(serve(c, stub.handler())))

	probe, err := provider.Probe(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(probe.Reachable, qt.IsTrue)
	c.Assert(probe.Authenticated, qt.IsTrue)
	c.Assert(probe.ToolCalling, qt.IsTrue)
	c.Assert(probe.ModelListed, qt.IsFalse,
		qt.Commentf("this API serves no model list, and claiming otherwise would be inventing one"))
}

func TestAnthropic_ProbeReportsARefusedCredential(t *testing.T) {
	c := qt.New(t)
	stub := &recorder{
		status: http.StatusUnauthorized,
		body:   `{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}`,
	}
	provider := aiprovider.NewAnthropic(anthropicConfig(serve(c, stub.handler())))

	probe, err := provider.Probe(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(probe.Reachable, qt.IsTrue)
	c.Assert(probe.Authenticated, qt.IsFalse)
}
