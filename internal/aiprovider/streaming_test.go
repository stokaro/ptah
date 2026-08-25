package aiprovider_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/aiprovider"
)

// sseServer serves a fixed event stream and records what was asked for.
func sseServer(c *qt.C, events string) (base string, asked *[]string) {
	c.Helper()
	requests := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(
		func(writer http.ResponseWriter, request *http.Request) {
			body := make([]byte, request.ContentLength)
			if request.ContentLength > 0 {
				_, _ = request.Body.Read(body)
			}
			requests = append(requests, string(body))
			writer.Header().Set("Content-Type", "text/event-stream")
			_, _ = writer.Write([]byte(events))
		}))
	c.Cleanup(server.Close)
	return server.URL, &requests
}

func TestOpenAIStream_DeliversProseAsItArrivesAndAssemblesTheAnswer(t *testing.T) {
	// The property streaming exists for: the caller sees the answer in pieces,
	// and still gets one whole Response at the end. A design that returned the
	// fragments instead would make every caller reassemble them, and they
	// would disagree about how.
	c := qt.New(t)
	base, _ := sseServer(c, strings.Join([]string{
		`data: {"id":"c1","model":"m","choices":[{"delta":{"content":"Two "}}]}`,
		``,
		`data: {"choices":[{"delta":{"content":"migration "}}]}`,
		``,
		`data: {"choices":[{"delta":{"content":"files."}}]}`,
		``,
		`data: {"choices":[{"finish_reason":"stop","delta":{}}],"usage":{"prompt_tokens":11,"completion_tokens":4}}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n"))

	provider := aiprovider.NewOpenAICompatible(aiprovider.Config{BaseURL: base, Model: "m"})

	fragments := make([]string, 0, 3)
	answer, err := provider.Chat(context.Background(), aiprovider.Request{
		Messages: []aiprovider.Message{{Role: aiprovider.RoleUser, Content: "how many?"}},
		OnText:   func(fragment string) { fragments = append(fragments, fragment) },
	})

	c.Assert(err, qt.IsNil)
	c.Assert(fragments, qt.DeepEquals, []string{"Two ", "migration ", "files."})
	c.Assert(answer.Message.Content, qt.Equals, "Two migration files.")
	c.Assert(answer.Model, qt.Equals, "m")
	c.Assert(answer.RequestID, qt.Equals, "c1")
	c.Assert(answer.Usage.InputTokens, qt.Equals, 11)
	c.Assert(answer.Usage.OutputTokens, qt.Equals, 4)
}

func TestOpenAIStream_AToolCallArrivesWholeOrNotAtAll(t *testing.T) {
	// The arguments come in fragments that are not valid JSON on their own.
	// A caller must never see one: acting on half an argument list is the
	// failure this assembly exists to prevent.
	c := qt.New(t)
	base, _ := sseServer(c, strings.Join([]string{
		`data: {"id":"c1","model":"m","choices":[{"delta":{"tool_calls":[{"index":0,"id":"t1","function":{"name":"read_artifact"}}]}}]}`,
		``,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"artifact\":"}}]}}]}`,
		``,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"migrations\"}"}}]}}]}`,
		``,
		`data: {"choices":[{"finish_reason":"tool_calls","delta":{}}]}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n"))

	provider := aiprovider.NewOpenAICompatible(aiprovider.Config{BaseURL: base, Model: "m"})

	fragments := make([]string, 0)
	answer, err := provider.Chat(context.Background(), aiprovider.Request{
		Messages: []aiprovider.Message{{Role: aiprovider.RoleUser, Content: "read it"}},
		OnText:   func(fragment string) { fragments = append(fragments, fragment) },
	})

	c.Assert(err, qt.IsNil)
	c.Assert(fragments, qt.HasLen, 0,
		qt.Commentf("a tool call is not prose and must not reach the text callback"))
	c.Assert(answer.Message.ToolCalls, qt.HasLen, 1)
	c.Assert(answer.Message.ToolCalls[0].Name, qt.Equals, "read_artifact")
	c.Assert(string(answer.Message.ToolCalls[0].Arguments), qt.Equals, `{"artifact":"migrations"}`)
	c.Assert(answer.StopReason, qt.Equals, aiprovider.StopToolCalls)
}

func TestOpenAIStream_TwoToolCallsAreKeptApartByIndex(t *testing.T) {
	// The index is the only thing tying a fragment to its call. Without it the
	// arguments of one would be appended to the other, and both would parse.
	c := qt.New(t)
	base, _ := sseServer(c, strings.Join([]string{
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"t1","function":{"name":"a","arguments":"{\"x\":1}"}}]}}]}`,
		``,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":1,"id":"t2","function":{"name":"b","arguments":"{\"y\":2}"}}]}}]}`,
		``,
		`data: {"choices":[{"finish_reason":"tool_calls","delta":{}}]}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n"))

	provider := aiprovider.NewOpenAICompatible(aiprovider.Config{BaseURL: base, Model: "m"})

	answer, err := provider.Chat(context.Background(), aiprovider.Request{
		Messages: []aiprovider.Message{{Role: aiprovider.RoleUser, Content: "both"}},
		OnText:   func(string) {},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(answer.Message.ToolCalls, qt.HasLen, 2)
	c.Assert(answer.Message.ToolCalls[0].Name, qt.Equals, "a")
	c.Assert(string(answer.Message.ToolCalls[0].Arguments), qt.Equals, `{"x":1}`)
	c.Assert(answer.Message.ToolCalls[1].Name, qt.Equals, "b")
	c.Assert(string(answer.Message.ToolCalls[1].Arguments), qt.Equals, `{"y":2}`)
}

func TestOpenAIStream_TheRequestAsksForUsage(t *testing.T) {
	// Usage is not sent on a streamed answer unless it is asked for, and a
	// session that could not say what a turn cost would be a worse record than
	// the unstreamed one it replaces.
	c := qt.New(t)
	base, asked := sseServer(c, "data: {\"choices\":[{\"finish_reason\":\"stop\",\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n")

	provider := aiprovider.NewOpenAICompatible(aiprovider.Config{BaseURL: base, Model: "m"})

	_, err := provider.Chat(context.Background(), aiprovider.Request{
		Messages: []aiprovider.Message{{Role: aiprovider.RoleUser, Content: "hi"}},
		OnText:   func(string) {},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(*asked, qt.HasLen, 1)
	c.Assert((*asked)[0], qt.Contains, `"stream":true`)
	c.Assert((*asked)[0], qt.Contains, `"include_usage":true`)
}

func TestAnthropicStream_DeliversProseAndAssemblesAToolCall(t *testing.T) {
	// The other event vocabulary. Anthropic names each event and delivers a
	// tool call's arguments as partial_json, which is not valid JSON until the
	// last fragment lands.
	c := qt.New(t)
	base, _ := sseServer(c, strings.Join([]string{
		`event: message_start`,
		`data: {"type":"message_start","message":{"id":"m1","model":"claude","usage":{"input_tokens":9}}}`,
		``,
		`event: content_block_start`,
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"text"}}`,
		``,
		`event: content_block_delta`,
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"One "}}`,
		``,
		`event: content_block_delta`,
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"pair."}}`,
		``,
		`event: content_block_start`,
		`data: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"t1","name":"read_artifact"}}`,
		``,
		`event: content_block_delta`,
		`data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"artifact\":"}}`,
		``,
		`event: content_block_delta`,
		`data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"\"migrations\"}"}}`,
		``,
		`event: message_delta`,
		`data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":7}}`,
		``,
	}, "\n"))

	provider := aiprovider.NewAnthropic(aiprovider.Config{BaseURL: base, Model: "claude", APIKey: "k"})

	fragments := make([]string, 0, 2)
	answer, err := provider.Chat(context.Background(), aiprovider.Request{
		Messages: []aiprovider.Message{{Role: aiprovider.RoleUser, Content: "what is here?"}},
		OnText:   func(fragment string) { fragments = append(fragments, fragment) },
	})

	c.Assert(err, qt.IsNil)
	c.Assert(fragments, qt.DeepEquals, []string{"One ", "pair."})
	c.Assert(answer.Message.Content, qt.Equals, "One pair.")
	c.Assert(answer.Message.ToolCalls, qt.HasLen, 1)
	c.Assert(string(answer.Message.ToolCalls[0].Arguments), qt.Equals, `{"artifact":"migrations"}`)
	c.Assert(answer.Usage.InputTokens, qt.Equals, 9)
	c.Assert(answer.Usage.OutputTokens, qt.Equals, 7)
	c.Assert(answer.RequestID, qt.Equals, "m1")
}

func TestAnthropicStream_AnInBandErrorIsReported(t *testing.T) {
	// Once a stream has started this API reports a failure as an event rather
	// than as a status code, so a client that only classified status codes
	// would read a failed turn as an empty answer.
	c := qt.New(t)
	base, _ := sseServer(c, strings.Join([]string{
		`event: message_start`,
		`data: {"type":"message_start","message":{"id":"m1","model":"claude"}}`,
		``,
		`event: error`,
		`data: {"type":"error","error":{"type":"overloaded_error","message":"the model is overloaded"}}`,
		``,
	}, "\n"))

	provider := aiprovider.NewAnthropic(aiprovider.Config{BaseURL: base, Model: "claude", APIKey: "k"})

	_, err := provider.Chat(context.Background(), aiprovider.Request{
		Messages: []aiprovider.Message{{Role: aiprovider.RoleUser, Content: "hi"}},
		OnText:   func(string) {},
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "the model is overloaded")
}
