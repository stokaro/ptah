package aiprovider_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/aiprovider"
)

// recorder is a stub endpoint: it records what it was sent and answers with a
// scripted status and body.
type recorder struct {
	status  int
	body    string
	headers map[string]string

	requests []*http.Request
	bodies   []string
	paths    []string
	auth     []string
}

func (r *recorder) handler() http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		payload := make([]byte, request.ContentLength)
		if request.ContentLength > 0 {
			_, _ = request.Body.Read(payload)
		}
		r.requests = append(r.requests, request)
		r.bodies = append(r.bodies, string(payload))
		r.paths = append(r.paths, request.URL.Path+"?"+request.URL.RawQuery)
		r.auth = append(r.auth, request.Header.Get("Authorization")+
			"|"+request.Header.Get("api-key")+"|"+request.Header.Get("x-api-key"))
		for name, value := range r.headers {
			writer.Header().Set(name, value)
		}
		status := r.status
		if status == 0 {
			status = http.StatusOK
		}
		writer.WriteHeader(status)
		_, _ = writer.Write([]byte(r.body))
	}
}

// serve starts the stub and returns its base URL.
func serve(c *qt.C, handler http.Handler) string {
	c.Helper()
	server := httptest.NewServer(handler)
	c.Cleanup(server.Close)
	return server.URL + "/v1"
}

// openAIConfig is the configuration every OpenAI-compatible test uses.
func openAIConfig(base string) aiprovider.Config {
	return aiprovider.Config{
		Profile: "local",
		BaseURL: base,
		Model:   "qwen",
		APIKey:  "not-a-real-key",
		Timeout: 5 * time.Second,
		Sleep:   func(context.Context, time.Duration) error { return nil },
	}
}

// decodeBody reads a recorded request body as a map.
func decodeBody(c *qt.C, raw string) map[string]any {
	c.Helper()
	decoded := make(map[string]any)
	c.Assert(json.Unmarshal([]byte(raw), &decoded), qt.IsNil)
	return decoded
}

// The answer shape measured against an MLX server on 2026-08-23: no `content`
// key on a tool-calling turn, and no `index` on the tool call.
const measuredToolCallAnswer = `{
  "id": "chatcmpl-1",
  "model": "mlx-community/Qwen3.6-27B-8bit",
  "choices": [{
    "index": 0,
    "finish_reason": "tool_calls",
    "message": {
      "role": "assistant",
      "tool_calls": [{
        "function": {"name": "describe_workspace", "arguments": "{}"},
        "type": "function",
        "id": "5a5cf78e"
      }]
    }
  }],
  "usage": {"prompt_tokens": 271, "completion_tokens": 18, "total_tokens": 289,
            "prompt_tokens_details": {"cached_tokens": 7}}
}`

func TestOpenAICompatible_ReadsAToolCall(t *testing.T) {
	// The fixture is a real answer from a real local server rather than one
	// composed from the API documentation, because the shapes that break a
	// client are the ones documentation does not mention.
	c := qt.New(t)
	stub := &recorder{body: measuredToolCallAnswer}
	provider := aiprovider.NewOpenAICompatible(openAIConfig(serve(c, stub.handler())))

	answer, err := provider.Chat(context.Background(), aiprovider.Request{
		System:   "you are a schema tool",
		Messages: []aiprovider.Message{{Role: aiprovider.RoleUser, Content: "describe the workspace"}},
		Tools: []aiprovider.ToolDefinition{{
			Name:        "describe_workspace",
			Description: "report the workspace",
			Schema:      json.RawMessage(`{"type":"object"}`),
		}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(answer.StopReason, qt.Equals, aiprovider.StopToolCalls)
	c.Assert(answer.Message.ToolCalls, qt.HasLen, 1)
	c.Assert(answer.Message.ToolCalls[0].ID, qt.Equals, "5a5cf78e")
	c.Assert(answer.Message.ToolCalls[0].Name, qt.Equals, "describe_workspace")
	c.Assert(string(answer.Message.ToolCalls[0].Arguments), qt.Equals, "{}")
	c.Assert(answer.Model, qt.Equals, "mlx-community/Qwen3.6-27B-8bit")
	c.Assert(answer.Usage.InputTokens, qt.Equals, 271)
	c.Assert(answer.Usage.OutputTokens, qt.Equals, 18)
	c.Assert(answer.Usage.CachedInputTokens, qt.Equals, 7)
	c.Assert(answer.RequestID, qt.Equals, "chatcmpl-1")
}

func TestOpenAICompatible_SendsTheSystemInstructionAsTheFirstMessage(t *testing.T) {
	c := qt.New(t)
	stub := &recorder{body: `{"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}]}`}
	provider := aiprovider.NewOpenAICompatible(openAIConfig(serve(c, stub.handler())))

	_, err := provider.Chat(context.Background(), aiprovider.Request{
		System:   "ptah system text",
		Messages: []aiprovider.Message{{Role: aiprovider.RoleUser, Content: "hello"}},
	})

	c.Assert(err, qt.IsNil)
	sent := decodeBody(c, stub.bodies[0])
	messages, _ := sent["messages"].([]any)
	c.Assert(messages, qt.HasLen, 2)
	first, _ := messages[0].(map[string]any)
	c.Assert(first["role"], qt.Equals, "system")
	c.Assert(first["content"], qt.Equals, "ptah system text")
	c.Assert(sent["model"], qt.Equals, "qwen")
	c.Assert(stub.paths[0], qt.Equals, "/v1/chat/completions?")
	c.Assert(stub.auth[0], qt.Equals, "Bearer not-a-real-key||")
}

func TestOpenAICompatible_SendsAToolResultAsItsOwnMessage(t *testing.T) {
	c := qt.New(t)
	stub := &recorder{body: `{"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"done"}}]}`}
	provider := aiprovider.NewOpenAICompatible(openAIConfig(serve(c, stub.handler())))

	_, err := provider.Chat(context.Background(), aiprovider.Request{
		Messages: []aiprovider.Message{
			{Role: aiprovider.RoleUser, Content: "describe"},
			{Role: aiprovider.RoleAssistant, ToolCalls: []aiprovider.ToolCall{{
				ID: "call-1", Name: "describe_workspace", Arguments: json.RawMessage(`{}`),
			}}},
			{Role: aiprovider.RoleTool, ToolCallID: "call-1",
				ToolName: "describe_workspace", Content: `{"artifacts":[]}`},
		},
	})

	c.Assert(err, qt.IsNil)
	sent := decodeBody(c, stub.bodies[0])
	messages, _ := sent["messages"].([]any)
	c.Assert(messages, qt.HasLen, 3)
	result, _ := messages[2].(map[string]any)
	c.Assert(result["role"], qt.Equals, "tool")
	c.Assert(result["tool_call_id"], qt.Equals, "call-1")
	c.Assert(result["content"], qt.Equals, `{"artifacts":[]}`)
}

func TestOpenAICompatible_UsesTheAzureHeaderWhenConfigured(t *testing.T) {
	// Azure OpenAI takes the key in api-key and the deployment's api-version in
	// the query. Both are configuration rather than a second adapter.
	c := qt.New(t)
	stub := &recorder{body: `{"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}]}`}
	cfg := openAIConfig(serve(c, stub.handler()))
	cfg.Headers = map[string]string{"api-key": ""}
	cfg.Query = map[string]string{"api-version": "2026-01-01"}
	provider := aiprovider.NewOpenAICompatible(cfg)

	_, err := provider.Chat(context.Background(), aiprovider.Request{
		Messages: []aiprovider.Message{{Role: aiprovider.RoleUser, Content: "hello"}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(stub.paths[0], qt.Equals, "/v1/chat/completions?api-version=2026-01-01")
	c.Assert(stub.auth[0], qt.Equals, "|not-a-real-key|")
}

func TestOpenAICompatible_ReadsAToolCallWithNoFinishReason(t *testing.T) {
	// A local server that returns tool calls and no finish reason is a real
	// shape. Reporting that turn as a complete answer would make the loop drop
	// the call and report the model as unhelpful.
	c := qt.New(t)
	stub := &recorder{body: `{"choices":[{"message":{"role":"assistant","tool_calls":[
		{"id":"c1","type":"function","function":{"name":"t","arguments":""}}]}}]}`}
	provider := aiprovider.NewOpenAICompatible(openAIConfig(serve(c, stub.handler())))

	answer, err := provider.Chat(context.Background(), aiprovider.Request{
		Messages: []aiprovider.Message{{Role: aiprovider.RoleUser, Content: "go"}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(answer.StopReason, qt.Equals, aiprovider.StopToolCalls)
	c.Assert(string(answer.Message.ToolCalls[0].Arguments), qt.Equals, "{}")
}

func TestOpenAICompatible_StopReasons(t *testing.T) {
	tests := []struct {
		name   string
		finish string
		want   aiprovider.StopReason
	}{
		{name: "stop", finish: "stop", want: aiprovider.StopEnd},
		{name: "length", finish: "length", want: aiprovider.StopMaxTokens},
		{name: "content filter", finish: "content_filter", want: aiprovider.StopFiltered},
		{name: "something else", finish: "banana", want: aiprovider.StopUnknown},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			stub := &recorder{body: `{"choices":[{"finish_reason":"` + test.finish +
				`","message":{"role":"assistant","content":"text"}}]}`}
			provider := aiprovider.NewOpenAICompatible(openAIConfig(serve(c, stub.handler())))

			answer, err := provider.Chat(context.Background(), aiprovider.Request{
				Messages: []aiprovider.Message{{Role: aiprovider.RoleUser, Content: "hello"}},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(answer.StopReason, qt.Equals, test.want)
		})
	}
}

func TestOpenAICompatible_ProbeMeasuresToolCalling(t *testing.T) {
	// The capability is measured rather than declared: a deployment that
	// documents tool calling and one that supports it are different things.
	c := qt.New(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/models", func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"data":[{"id":"qwen"}]}`))
	})
	mux.HandleFunc("/v1/chat/completions", func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(measuredToolCallAnswer))
	})
	provider := aiprovider.NewOpenAICompatible(openAIConfig(serve(c, mux)))

	probe, err := provider.Probe(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(probe.Reachable, qt.IsTrue)
	c.Assert(probe.Authenticated, qt.IsTrue)
	c.Assert(probe.ModelListed, qt.IsTrue)
	c.Assert(probe.ToolCalling, qt.IsTrue)
}

func TestOpenAICompatible_ProbeReportsAModelThatWillNotCallTools(t *testing.T) {
	// The control for the test above. A probe that reported tool calling
	// whatever came back would pass on a model that cannot do it, which is the
	// one thing this check exists to catch.
	c := qt.New(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/models", func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"data":[{"id":"qwen"}]}`))
	})
	mux.HandleFunc("/v1/chat/completions", func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(
			`{"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"I cannot call tools."}}]}`))
	})
	provider := aiprovider.NewOpenAICompatible(openAIConfig(serve(c, mux)))

	probe, err := provider.Probe(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(probe.Reachable, qt.IsTrue)
	c.Assert(probe.ToolCalling, qt.IsFalse)
	c.Assert(probe.Notes, qt.Not(qt.HasLen), 0)
}

func TestOpenAICompatible_ProbeReportsARefusedCredential(t *testing.T) {
	c := qt.New(t)
	stub := &recorder{status: http.StatusUnauthorized, body: `{"error":{"message":"invalid api key"}}`}
	provider := aiprovider.NewOpenAICompatible(openAIConfig(serve(c, stub.handler())))

	probe, err := provider.Probe(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(probe.Reachable, qt.IsTrue)
	c.Assert(probe.Authenticated, qt.IsFalse)
	c.Assert(probe.ToolCalling, qt.IsFalse)
}
