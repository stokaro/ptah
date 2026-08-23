package aiprovider

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

// OpenAICompatible is the adapter for every endpoint that speaks OpenAI's
// Chat Completions API.
//
// That is one adapter and a large number of destinations: OpenAI itself, Azure
// OpenAI, OpenRouter, LiteLLM and other gateways, vLLM, LM Studio, Ollama, and
// the MLX server. Chat Completions rather than the Responses API on purpose --
// the newer API is the one OpenAI's own CLI has moved to exclusively, and it is
// the one almost nothing else implements, so choosing it would trade every
// local and self-hosted destination for one hosted one.
//
// Measured against an MLX server serving Qwen3.6-27B on 2026-08-23: a tools
// request answers `finish_reason: "tool_calls"` with a `tool_calls` array whose
// entries carry no `index`, and the assistant message carries no `content` key
// at all. Both shapes are read here, because "OpenAI-compatible" describes an
// intent rather than a conformance suite.
type OpenAICompatible struct {
	cfg Config
}

// NewOpenAICompatible builds the adapter.
func NewOpenAICompatible(cfg Config) *OpenAICompatible {
	return &OpenAICompatible{cfg: cfg.withDefaults()}
}

// Profile is the operator's name for this configuration.
func (p *OpenAICompatible) Profile() string { return p.cfg.Profile }

// Model is the configured model identifier.
func (p *OpenAICompatible) Model() string { return p.cfg.Model }

// authorize applies the credential in whichever of the two forms this endpoint
// wants.
//
// Azure OpenAI takes the key in an `api-key` header and everything else takes a
// bearer token. The operator selects Azure by configuring the header
// explicitly; a configured Authorization or api-key header is left alone rather
// than overwritten, so a gateway wanting something unusual is reachable without
// a new adapter.
func (p *OpenAICompatible) authorize(request *http.Request) {
	if p.cfg.APIKey == "" {
		return
	}
	if _, set := p.cfg.Headers["Authorization"]; set {
		return
	}
	if _, set := p.cfg.Headers["api-key"]; set {
		request.Header.Set("api-key", p.cfg.APIKey)
		return
	}
	request.Header.Set("Authorization", "Bearer "+p.cfg.APIKey)
}

// chatRequest is the wire shape sent to /chat/completions.
type chatRequest struct {
	Model       string      `json:"model"`
	Messages    []chatWire  `json:"messages"`
	Tools       []toolWire  `json:"tools,omitempty"`
	MaxTokens   int         `json:"max_tokens,omitempty"`
	Temperature *float64    `json:"temperature,omitempty"`
	Stream      bool        `json:"stream"`
	StreamOpts  *streamOpts `json:"stream_options,omitempty"`
	ToolChoice  *any        `json:"tool_choice,omitempty"`
}

// streamOpts is how usage is requested on a streamed answer. It is declared
// because the streaming turn will need it and unset here.
type streamOpts struct {
	IncludeUsage bool `json:"include_usage"`
}

// chatWire is one message on the wire.
type chatWire struct {
	Role       string         `json:"role"`
	Content    string         `json:"content,omitempty"`
	ToolCalls  []toolCallWire `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
	Name       string         `json:"name,omitempty"`
}

// toolWire is one tool definition on the wire.
type toolWire struct {
	Type     string       `json:"type"`
	Function functionWire `json:"function"`
}

type functionWire struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// toolCallWire is one tool call on the wire, in the shape both a strict OpenAI
// answer and a looser local server produce.
type toolCallWire struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// chatResponse is the wire shape read back.
type chatResponse struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Choices []struct {
		FinishReason string `json:"finish_reason"`
		Message      struct {
			Role      string         `json:"role"`
			Content   string         `json:"content"`
			ToolCalls []toolCallWire `json:"tool_calls"`
		} `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens        int `json:"prompt_tokens"`
		CompletionTokens    int `json:"completion_tokens"`
		PromptTokensDetails struct {
			CachedTokens int `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
	} `json:"usage"`
}

// Chat sends one turn.
func (p *OpenAICompatible) Chat(ctx context.Context, req Request) (*Response, error) {
	wire := chatRequest{
		Model:       p.cfg.Model,
		Messages:    p.encodeMessages(req),
		Tools:       encodeTools(req.Tools),
		MaxTokens:   req.MaxOutputTokens,
		Temperature: req.Temperature,
	}

	var answer chatResponse
	if err := p.cfg.call(ctx, http.MethodPost, "chat/completions", wire, p.authorize, &answer); err != nil {
		return nil, err
	}
	if len(answer.Choices) == 0 {
		return nil, p.cfg.wrap(KindMalformedResponse, 0,
			"the endpoint answered with no choices", nil)
	}

	choice := answer.Choices[0]
	message := Message{Role: RoleAssistant, Content: choice.Message.Content}
	for _, call := range choice.Message.ToolCalls {
		message.ToolCalls = append(message.ToolCalls, ToolCall{
			ID:        call.ID,
			Name:      call.Function.Name,
			Arguments: rawArguments(call.Function.Arguments),
		})
	}
	return &Response{
		Model:      firstNonEmpty(answer.Model, p.cfg.Model),
		Message:    message,
		StopReason: stopReasonFor(choice.FinishReason, len(message.ToolCalls)),
		Usage: Usage{
			InputTokens:       answer.Usage.PromptTokens,
			OutputTokens:      answer.Usage.CompletionTokens,
			CachedInputTokens: answer.Usage.PromptTokensDetails.CachedTokens,
		},
		RequestID: answer.ID,
	}, nil
}

// encodeMessages places the system instruction where this API wants it, which
// is as the first message.
func (p *OpenAICompatible) encodeMessages(req Request) []chatWire {
	wire := make([]chatWire, 0, len(req.Messages)+1)
	if req.System != "" {
		wire = append(wire, chatWire{Role: "system", Content: req.System})
	}
	for _, message := range req.Messages {
		encoded := chatWire{
			Role:       string(message.Role),
			Content:    message.Content,
			ToolCallID: message.ToolCallID,
			Name:       message.ToolName,
		}
		for _, call := range message.ToolCalls {
			encoded.ToolCalls = append(encoded.ToolCalls, toolCallWire{
				ID:   call.ID,
				Type: "function",
				Function: struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				}{Name: call.Name, Arguments: string(call.Arguments)},
			})
		}
		wire = append(wire, encoded)
	}
	return wire
}

// encodeTools converts tool definitions to the wire shape.
func encodeTools(tools []ToolDefinition) []toolWire {
	if len(tools) == 0 {
		return nil
	}
	wire := make([]toolWire, 0, len(tools))
	for _, tool := range tools {
		wire = append(wire, toolWire{
			Type: "function",
			Function: functionWire{
				Name:        tool.Name,
				Description: tool.Description,
				Parameters:  tool.Schema,
			},
		})
	}
	return wire
}

// rawArguments keeps a tool call's arguments as JSON, tolerating an endpoint
// that sent an empty string for "no arguments".
func rawArguments(arguments string) json.RawMessage {
	if strings.TrimSpace(arguments) == "" {
		return json.RawMessage("{}")
	}
	return json.RawMessage(arguments)
}

// stopReasonFor maps a finish reason, and falls back to what the answer
// contains.
//
// The fallback is not defensive: a local server that returns tool calls with no
// finish reason at all is a real shape, and reporting that turn as a complete
// answer would make the loop drop the call.
func stopReasonFor(finish string, toolCalls int) StopReason {
	switch finish {
	case "stop", "end_turn":
		return StopEnd
	case "tool_calls", "function_call", "tool_use":
		return StopToolCalls
	case "length", "max_tokens":
		return StopMaxTokens
	case "content_filter":
		return StopFiltered
	}
	if toolCalls > 0 {
		return StopToolCalls
	}
	if finish == "" {
		return StopEnd
	}
	return StopUnknown
}

// firstNonEmpty returns the first non-empty string.
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// modelList is the wire shape of /models.
type modelList struct {
	Data []struct {
		ID string `json:"id"`
	} `json:"data"`
}

// Probe checks the configuration without sending any project content.
//
// Two requests: the model list, which answers reachability, authentication and
// whether the model is one the endpoint admits to serving; and a trivial tool
// call, which answers the only capability question that decides whether Assist
// can run. The second is measured rather than read off documentation, because a
// deployment that documents tool calling and one that supports it are different
// things.
func (p *OpenAICompatible) Probe(ctx context.Context) (Probe, error) {
	started := time.Now()
	probe := Probe{Provider: p.cfg.Profile, Model: p.cfg.Model}

	var listed modelList
	listErr := p.cfg.call(ctx, http.MethodGet, "models", nil, p.authorize, &listed)
	switch KindOf(listErr) {
	case KindAuth:
		probe.Reachable = true
		probe.Notes = append(probe.Notes, "the endpoint refused the credential")
		probe.Latency = time.Since(started)
		return probe, nil
	case KindUnreachable, KindTimeout:
		probe.Notes = append(probe.Notes, "the endpoint did not answer: "+listErr.Error())
		probe.Latency = time.Since(started)
		return probe, nil
	}
	probe.Reachable = true
	probe.Authenticated = true
	if listErr != nil {
		// A gateway that serves chat and not /models is common enough that this
		// is a note rather than a failure.
		probe.Notes = append(probe.Notes, "the endpoint served no model list: "+listErr.Error())
	}
	for _, model := range listed.Data {
		if model.ID == p.cfg.Model {
			probe.ModelListed = true
		}
	}
	if !probe.ModelListed && len(listed.Data) > 0 {
		probe.Notes = append(probe.Notes,
			"the model is not in the endpoint's list, which some gateways do not populate")
	}

	answer, err := p.Chat(ctx, probeRequest())
	probe.Latency = time.Since(started)
	if err != nil {
		probe.Notes = append(probe.Notes, "the tool-calling check failed: "+err.Error())
		probe.Authenticated = KindOf(err) != KindAuth
		return probe, nil
	}
	probe.ToolCalling = len(answer.Message.ToolCalls) > 0
	if !probe.ToolCalling {
		probe.Notes = append(probe.Notes,
			"the model answered with text where a tool call was the only sensible answer")
	}
	return probe, nil
}

// probeRequest is the smallest request that distinguishes a model that can call
// a tool from one that cannot.
//
// It carries no project content, which is #1488's requirement for this check:
// an operator testing a provider configuration has not agreed to send their
// schema anywhere yet.
func probeRequest() Request {
	return Request{
		System: "You verify connectivity. Call the tool. Do not answer in words.",
		Messages: []Message{{
			Role:    RoleUser,
			Content: "Call ptah_connectivity_check with ok set to true.",
		}},
		Tools: []ToolDefinition{{
			Name:        "ptah_connectivity_check",
			Description: "Confirm that tool calling works. Call this and nothing else.",
			Schema: json.RawMessage(
				`{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"]}`),
		}},
		MaxOutputTokens: 128,
	}
}
