package aiprovider

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// anthropicVersion is the API version header the Messages API requires.
//
// It is a constant rather than a configuration field because it is a contract
// with one provider, not a knob: an operator who needs a different one needs a
// different adapter, and pretending otherwise would let a typo produce a
// failure that reads like a broken key.
const anthropicVersion = "2023-06-01"

// defaultAnthropicMaxTokens is what is sent when the caller states no limit.
//
// The Messages API requires max_tokens, unlike Chat Completions, so this
// adapter has to have an answer. It is deliberately large: the limit exists to
// bound the reply, and a small default would truncate answers in a way the
// caller never asked for and would read as the model being bad at finishing
// sentences.
const defaultAnthropicMaxTokens = 8192

// Anthropic is the adapter for the Messages API.
//
// It exists to prove the abstraction rather than to complete a set: #1488 asks
// for two direct adapters with materially different API shapes, and this one is
// different in every place that matters. The system instruction is a top-level
// field rather than a message; content is a list of typed blocks rather than a
// string; a tool call is a `tool_use` block inside the assistant's content
// rather than a parallel array; a tool result is a `tool_result` block inside a
// USER message rather than a message with its own role; the credential is an
// `x-api-key` header rather than a bearer token; and the token counts have
// different names.
//
// Everything above is a place a leaky abstraction would have shown. The
// normalized types in this package survived all of them, which is the argument
// for them.
type Anthropic struct {
	cfg Config
}

// DefaultAnthropicBaseURL is the public endpoint.
const DefaultAnthropicBaseURL = "https://api.anthropic.com/v1"

// NewAnthropic builds the adapter.
func NewAnthropic(cfg Config) *Anthropic {
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultAnthropicBaseURL
	}
	return &Anthropic{cfg: cfg.withDefaults()}
}

// Profile is the operator's name for this configuration.
func (p *Anthropic) Profile() string { return p.cfg.Profile }

// Model is the configured model identifier.
func (p *Anthropic) Model() string { return p.cfg.Model }

// authorize applies the credential and the API version.
func (p *Anthropic) authorize(request *http.Request) {
	if p.cfg.APIKey != "" {
		request.Header.Set("x-api-key", p.cfg.APIKey)
	}
	if request.Header.Get("anthropic-version") == "" {
		request.Header.Set("anthropic-version", anthropicVersion)
	}
}

// messagesRequest is the wire shape sent to /messages.
type messagesRequest struct {
	Model       string            `json:"model"`
	System      string            `json:"system,omitempty"`
	Messages    []anthropicWire   `json:"messages"`
	Tools       []anthropicTool   `json:"tools,omitempty"`
	MaxTokens   int               `json:"max_tokens"`
	Temperature *float64          `json:"temperature,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// anthropicWire is one message, whose content is always a block list here even
// where a bare string would be accepted, so one encoder covers every turn.
type anthropicWire struct {
	Role    string           `json:"role"`
	Content []anthropicBlock `json:"content"`
}

// anthropicBlock is one content block, in the union the API uses.
type anthropicBlock struct {
	Type string `json:"type"`
	// Text is set for a text block.
	Text string `json:"text,omitempty"`
	// ID, Name and Input are set for a tool_use block.
	ID    string          `json:"id,omitempty"`
	Name  string          `json:"name,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`
	// ToolUseID and Content are set for a tool_result block.
	ToolUseID string `json:"tool_use_id,omitempty"`
	Content   string `json:"content,omitempty"`
}

// anthropicTool is one tool definition.
type anthropicTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema"`
}

// messagesResponse is the wire shape read back.
type messagesResponse struct {
	ID         string           `json:"id"`
	Model      string           `json:"model"`
	StopReason string           `json:"stop_reason"`
	Content    []anthropicBlock `json:"content"`
	Usage      struct {
		InputTokens              int `json:"input_tokens"`
		OutputTokens             int `json:"output_tokens"`
		CacheReadInputTokens     int `json:"cache_read_input_tokens"`
		CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
	} `json:"usage"`
}

// Chat sends one turn.
func (p *Anthropic) Chat(ctx context.Context, req Request) (*Response, error) {
	maxTokens := req.MaxOutputTokens
	if maxTokens <= 0 {
		maxTokens = defaultAnthropicMaxTokens
	}
	wire := messagesRequest{
		Model:       p.cfg.Model,
		System:      req.System,
		Messages:    encodeAnthropicMessages(req.Messages),
		Tools:       encodeAnthropicTools(req.Tools),
		MaxTokens:   maxTokens,
		Temperature: req.Temperature,
	}

	var answer messagesResponse
	if err := p.cfg.call(ctx, http.MethodPost, "messages", wire, p.authorize, &answer); err != nil {
		return nil, err
	}

	message := Message{Role: RoleAssistant}
	for _, block := range answer.Content {
		switch block.Type {
		case "text":
			message.Content += block.Text
		case "tool_use":
			message.ToolCalls = append(message.ToolCalls, ToolCall{
				ID:        block.ID,
				Name:      block.Name,
				Arguments: rawInput(block.Input),
			})
		}
	}
	return &Response{
		Model:      firstNonEmpty(answer.Model, p.cfg.Model),
		Message:    message,
		StopReason: anthropicStopReason(answer.StopReason, len(message.ToolCalls)),
		Usage: Usage{
			InputTokens:       answer.Usage.InputTokens,
			OutputTokens:      answer.Usage.OutputTokens,
			CachedInputTokens: answer.Usage.CacheReadInputTokens,
		},
		RequestID: answer.ID,
	}, nil
}

// encodeAnthropicMessages folds the normalized turns into this API's shape.
//
// The fold that matters: a tool result is not a message with its own role here.
// It is a tool_result block in a user message, and consecutive results belong in
// one message. A caller that produced one message per tool result sees them
// merged, which is what the API requires and what the normalized shape hides.
func encodeAnthropicMessages(messages []Message) []anthropicWire {
	wire := make([]anthropicWire, 0, len(messages))
	for _, message := range messages {
		if message.Role == RoleTool {
			block := anthropicBlock{
				Type:      "tool_result",
				ToolUseID: message.ToolCallID,
				Content:   message.Content,
			}
			if last := len(wire) - 1; last >= 0 && wire[last].Role == "user" &&
				isToolResultOnly(wire[last]) {
				wire[last].Content = append(wire[last].Content, block)
				continue
			}
			wire = append(wire, anthropicWire{Role: "user", Content: []anthropicBlock{block}})
			continue
		}

		blocks := make([]anthropicBlock, 0, 1+len(message.ToolCalls))
		if message.Content != "" {
			blocks = append(blocks, anthropicBlock{Type: "text", Text: message.Content})
		}
		for _, call := range message.ToolCalls {
			blocks = append(blocks, anthropicBlock{
				Type:  "tool_use",
				ID:    call.ID,
				Name:  call.Name,
				Input: rawInput(call.Arguments),
			})
		}
		wire = append(wire, anthropicWire{Role: string(message.Role), Content: blocks})
	}
	return wire
}

// isToolResultOnly reports a message built entirely of tool results, which is
// the only one a further result may join.
func isToolResultOnly(message anthropicWire) bool {
	for _, block := range message.Content {
		if block.Type != "tool_result" {
			return false
		}
	}
	return len(message.Content) > 0
}

// encodeAnthropicTools converts tool definitions.
func encodeAnthropicTools(tools []ToolDefinition) []anthropicTool {
	if len(tools) == 0 {
		return nil
	}
	wire := make([]anthropicTool, 0, len(tools))
	for _, tool := range tools {
		wire = append(wire, anthropicTool{
			Name:        tool.Name,
			Description: tool.Description,
			InputSchema: rawInput(tool.Schema),
		})
	}
	return wire
}

// rawInput defaults absent JSON to an empty object, which both a tool with no
// arguments and a schema-less tool definition need.
func rawInput(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("{}")
	}
	return raw
}

// anthropicStopReason maps this API's stop reasons.
func anthropicStopReason(reason string, toolCalls int) StopReason {
	switch reason {
	case "end_turn", "stop_sequence":
		return StopEnd
	case "tool_use":
		return StopToolCalls
	case "max_tokens":
		return StopMaxTokens
	case "refusal":
		return StopFiltered
	}
	if toolCalls > 0 {
		return StopToolCalls
	}
	if reason == "" {
		return StopEnd
	}
	return StopUnknown
}

// Probe checks the configuration without sending any project content.
//
// One request rather than two: the Messages API has no model list to read, so
// the trivial tool call answers reachability, the credential and the capability
// together. A model list would be the nicer answer for "is the model name
// right", and inventing one from a hardcoded list of model names would go stale
// between releases, which is worse than a note saying it was not checked.
func (p *Anthropic) Probe(ctx context.Context) (Probe, error) {
	started := time.Now()
	probe := Probe{
		Provider: p.cfg.Profile,
		Model:    p.cfg.Model,
		Notes:    []string{"this API serves no model list, so the model name was not checked"},
	}

	answer, err := p.Chat(ctx, probeRequest())
	probe.Latency = time.Since(started)
	if err != nil {
		switch KindOf(err) {
		case KindUnreachable, KindTimeout:
			probe.Notes = append(probe.Notes, "the endpoint did not answer: "+err.Error())
		case KindAuth:
			probe.Reachable = true
			probe.Notes = append(probe.Notes, "the endpoint refused the credential")
		default:
			probe.Reachable = true
			probe.Authenticated = true
			probe.Notes = append(probe.Notes, "the tool-calling check failed: "+err.Error())
		}
		return probe, nil
	}

	probe.Reachable = true
	probe.Authenticated = true
	probe.ToolCalling = len(answer.Message.ToolCalls) > 0
	if !probe.ToolCalling {
		probe.Notes = append(probe.Notes,
			"the model answered with text where a tool call was the only sensible answer")
	}
	return probe, nil
}
