// Package assistloop runs Ptah Assist's model loop: the model proposes, Ptah's
// own tools answer, and Ptah decides what the model was allowed to do.
//
// # One surface, two transports
//
// #1483 states an invariant: every capability Ptah Assist has must be the same
// public contract external AI clients get, and Assist must not become a second
// implementation path around Ptah Core. That is easy to promise and easy to
// drift from -- a second call site, a shortcut past the broker, one extra thing
// Assist can do.
//
// So Assist does not call the operations. It speaks the Model Context Protocol
// to Ptah's own server over an in-memory transport, exactly as an external
// client speaks it over stdio. Same tools, same schemas, same capability broker,
// same gates, same audit records. The invariant is structural: giving Assist
// something extra would mean adding a tool to the server, where an external
// client would get it too.
//
// # What the loop is responsible for
//
// Bounding itself. A model that loops, repeats one call, or asks for a hundred
// tools is not a failure the tools can catch, because each call is individually
// legitimate. The limits here are what turns that into a terminated run with a
// diagnostic rather than a session that never ends.
//
// # What it must never do
//
// Report that Ptah checked something Ptah did not check. The final answer is the
// model's words, and the tool results are Ptah's; a caller that mixes them is
// the failure #1483 names, so [Result] keeps them apart and the surfaces render
// them apart.
package assistloop

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go.5x5.cz/ptah/internal/aiprovider"
)

// Limits a run is bounded by when the caller states none.
const (
	// DefaultMaxTurns bounds how many times the model is asked to continue.
	DefaultMaxTurns = 12
	// DefaultMaxToolCalls bounds the whole run's tool calls.
	DefaultMaxToolCalls = 40
	// DefaultMaxRepeats bounds identical calls. A model that asks the same
	// question three times has stopped making progress, and the third answer
	// will not be different from the second.
	DefaultMaxRepeats = 3
	// DefaultMaxToolOutputBytes bounds one tool result before it is truncated.
	// A directory listing of a large migration tree is a real answer and a bad
	// thing to put in a context window whole.
	DefaultMaxToolOutputBytes = 32 << 10
)

var (
	// ErrToolCallingUnavailable reports a model that never called a tool when
	// tools were the only way to answer.
	ErrToolCallingUnavailable = errors.New(
		"the model answered without calling any Ptah tool, so nothing it said was checked")
	// ErrLimit reports a run that hit one of its bounds.
	ErrLimit = errors.New("the run hit a limit")
)

// StopReason says why the loop ended, in Ptah's words rather than the
// provider's.
type StopReason string

const (
	// StoppedWithAnswer is the model finishing.
	StoppedWithAnswer StopReason = "answer"
	// StoppedAtTurnLimit is the model still going when the turn budget ran out.
	StoppedAtTurnLimit StopReason = "turn limit"
	// StoppedAtToolCallLimit is the same for tool calls.
	StoppedAtToolCallLimit StopReason = "tool call limit"
	// StoppedRepeating is a model asking the same thing over and over.
	StoppedRepeating StopReason = "repeated tool call"
	// StoppedTruncated is an answer the provider cut off at its output limit.
	StoppedTruncated StopReason = "output limit"
)

// ToolRecord is one tool call and what Ptah answered.
//
// It is the evidence half of a run. A surface that showed the model's summary
// and not these would be showing an account of what happened with no way to
// check it.
type ToolRecord struct {
	Name string          `json:"name"`
	Args json.RawMessage `json:"arguments"`
	// Failed reports that the tool refused or errored, which is as much a part
	// of the record as a success: a refused capability is what a hostile
	// repository produces.
	Failed bool `json:"failed"`
	// Result is what Ptah returned, truncated to the output limit.
	Result string `json:"result"`
	// Truncated reports that Result is not the whole answer.
	Truncated bool          `json:"truncated,omitempty"`
	Elapsed   time.Duration `json:"-"`
}

// Result is one run.
type Result struct {
	// Answer is the model's final text. It is the model's words, and a surface
	// must present it as such.
	Answer string `json:"answer"`
	// Tools is what Ptah actually did, in order.
	Tools []ToolRecord `json:"tools"`
	// Turns counts model requests.
	Turns      int                  `json:"turns"`
	Usage      aiprovider.Usage     `json:"usage"`
	Provider   string               `json:"provider"`
	Model      string               `json:"model"`
	StopReason StopReason           `json:"stop_reason"`
	Messages   []aiprovider.Message `json:"-"`
}

// ToolBytes is how much tool output was handed back to the provider.
//
// Everything Ptah read on the model's behalf reaches the provider as a tool
// result, so this is the size of what left the machine about the project. The
// question and the system prompt are not counted: one is the person's own words
// and the other is Ptah's, and neither describes the project.
func (r *Result) ToolBytes() int {
	total := 0
	for _, tool := range r.Tools {
		total += len(tool.Result)
	}
	return total
}

// UsedTools reports whether any tool actually answered, which is the difference
// between an answer Ptah stands behind and one the model composed unaided.
//
// A failed call does not count. A refused capability, a stale digest or a tool
// that could not be called produces a record, and counting records would report
// "checked against this project" for a run where Ptah answered nothing and the
// model wrote around the refusal. That is the one reading this field exists to
// prevent.
func (r Result) UsedTools() bool {
	for _, tool := range r.Tools {
		if !tool.Failed {
			return true
		}
	}
	return false
}

// Event is progress, for a surface that shows what is happening.
type Event struct {
	Kind string
	Text string
}

// Options configures a loop.
type Options struct {
	// Provider is the model.
	Provider aiprovider.Provider
	// Tools is what Ptah's own tools are called through: a connected protocol
	// client session. It is an interface rather than the concrete session so a
	// caller can narrow what the loop can reach, and so the loop's own tests do
	// not depend on a transport.
	Tools ToolSession
	// System overrides Ptah's instruction block. Empty uses [SystemPrompt].
	System string
	// History is the conversation a resumed session continues from, oldest
	// first. It carries requests and answers rather than tool results: those
	// described the project as it was, and replaying them would have the model
	// reasoning about a directory that may have changed. It re-reads instead.
	History []aiprovider.Message

	MaxTurns           int
	MaxToolCalls       int
	MaxRepeats         int
	MaxToolOutputBytes int

	// Emit receives progress. A nil value discards it.
	Emit func(Event)
	// OnText receives the model's answer in fragments as it arrives.
	//
	// A nil value asks for the answer whole. It is separate from Emit because
	// Emit is Ptah's own words about what is happening and this is the model's
	// text: a surface that prints them the same way would be putting a
	// diagnostic and an answer in one voice.
	//
	// Only prose arrives here. A tool call is assembled by the adapter and
	// reaches the loop complete, so nothing here can act on half an argument.
	OnText func(fragment string)
	// OnTool receives each tool record as the call completes, before the model
	// is shown the result. A nil value discards it.
	//
	// Separate from Emit because a progress line is for a person watching and
	// this is the record itself. A consumer that waited for Run to return would
	// learn about a refused capability only after the model had already acted
	// on it, and a run killed halfway would leave nothing behind at all.
	OnTool func(ToolRecord)
	// Now is the clock, injectable for a test that asserts on elapsed time.
	Now func() time.Time
}

// ToolSession is the half of a protocol client the loop uses: list what is
// available, and call one.
type ToolSession interface {
	ListTools(ctx context.Context, params *mcp.ListToolsParams) (*mcp.ListToolsResult, error)
	CallTool(ctx context.Context, params *mcp.CallToolParams) (*mcp.CallToolResult, error)
}

// Loop is a configured run.
type Loop struct {
	opts Options
}

// New builds a loop, refusing options that would make a run unbounded.
func New(opts Options) (*Loop, error) {
	if opts.Provider == nil {
		return nil, errors.New("assist loop requires a provider")
	}
	if opts.Tools == nil {
		return nil, errors.New("assist loop requires a connected tool session")
	}
	if opts.MaxTurns <= 0 {
		opts.MaxTurns = DefaultMaxTurns
	}
	if opts.MaxToolCalls <= 0 {
		opts.MaxToolCalls = DefaultMaxToolCalls
	}
	if opts.MaxRepeats <= 0 {
		opts.MaxRepeats = DefaultMaxRepeats
	}
	if opts.MaxToolOutputBytes <= 0 {
		opts.MaxToolOutputBytes = DefaultMaxToolOutputBytes
	}
	if opts.System == "" {
		opts.System = SystemPrompt
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Emit == nil {
		opts.Emit = func(Event) {}
	}
	if opts.OnTool == nil {
		opts.OnTool = func(ToolRecord) {}
	}
	return &Loop{opts: opts}, nil
}

// Run drives one request to an answer.
func (l *Loop) Run(ctx context.Context, request string) (*Result, error) {
	tools, err := l.toolDefinitions(ctx)
	if err != nil {
		return nil, err
	}

	messages := l.openingMessages(request)

	result := &Result{
		Provider: l.opts.Provider.Profile(),
		Model:    l.opts.Provider.Model(),
		Tools:    make([]ToolRecord, 0, 4),
		Messages: messages,
	}
	repeats := make(map[string]int)

	for result.Turns < l.opts.MaxTurns {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		result.Turns++
		l.opts.Emit(Event{Kind: "turn", Text: fmt.Sprintf("asking %s", result.Model)})

		answer, err := l.opts.Provider.Chat(ctx, aiprovider.Request{
			System:   l.opts.System,
			Messages: result.Messages,
			Tools:    tools,
			OnText:   l.opts.OnText,
		})
		if err != nil {
			return result, err
		}
		result.Usage.InputTokens += answer.Usage.InputTokens
		result.Usage.OutputTokens += answer.Usage.OutputTokens
		result.Usage.CachedInputTokens += answer.Usage.CachedInputTokens
		result.Messages = append(result.Messages, answer.Message)

		if len(answer.Message.ToolCalls) == 0 {
			result.Answer = answer.Message.Content
			result.StopReason = StoppedWithAnswer
			if answer.StopReason == aiprovider.StopMaxTokens {
				result.StopReason = StoppedTruncated
			}
			return result, nil
		}

		stop, err := l.runToolCalls(ctx, answer.Message.ToolCalls, result, repeats)
		if err != nil {
			return result, err
		}
		if stop != "" {
			result.StopReason = stop
			result.Answer = answer.Message.Content
			return result, fmt.Errorf("%w: %s", ErrLimit, stop)
		}
	}

	result.StopReason = StoppedAtTurnLimit
	return result, fmt.Errorf("%w: %s after %d turns", ErrLimit, StoppedAtTurnLimit, result.Turns)
}

// runToolCalls executes one turn's calls, appending each result to the
// conversation.
//
// Serially, and that is a decision rather than an omission: two calls that
// wrote to one artifact directory would race, and the loop cannot tell a
// read-only call from a writing one without knowing more about the tools than
// an adapter should. Parallel read-only calls are an optimization this phase
// does not need.
func (l *Loop) runToolCalls(
	ctx context.Context,
	calls []aiprovider.ToolCall,
	result *Result,
	repeats map[string]int,
) (StopReason, error) {
	for _, call := range calls {
		if len(result.Tools) >= l.opts.MaxToolCalls {
			return StoppedAtToolCallLimit, nil
		}
		fingerprint := call.Name + string(call.Arguments)
		repeats[fingerprint]++
		if repeats[fingerprint] > l.opts.MaxRepeats {
			return StoppedRepeating, nil
		}

		record := l.callTool(ctx, call)
		result.Tools = append(result.Tools, record)
		l.opts.OnTool(record)
		result.Messages = append(result.Messages, aiprovider.Message{
			Role:       aiprovider.RoleTool,
			ToolCallID: call.ID,
			ToolName:   call.Name,
			Content:    record.Result,
		})
	}
	return "", nil
}

// openingMessages is the conversation a request starts from: the resumed
// history, then the question.
func (l *Loop) openingMessages(request string) []aiprovider.Message {
	messages := make([]aiprovider.Message, 0, len(l.opts.History)+1)
	messages = append(messages, l.opts.History...)
	messages = append(messages, aiprovider.Message{Role: aiprovider.RoleUser, Content: request})
	return messages
}

// Preview builds the request a Run would send first, and sends nothing.
//
// It is the same construction [Run] uses rather than a description of it: a
// preview assembled separately would be a plausible document that drifts from
// what actually leaves the machine, which is the one thing it must not be. The
// tool list is read from the connected server, so it is what the model would
// really be offered.
func (l *Loop) Preview(ctx context.Context, request string) (aiprovider.Request, error) {
	tools, err := l.toolDefinitions(ctx)
	if err != nil {
		return aiprovider.Request{}, err
	}
	return aiprovider.Request{
		System:   l.opts.System,
		Messages: l.openingMessages(request),
		Tools:    tools,
	}, nil
}

// callTool runs one tool through the protocol and records what came back.
//
// A tool error is recorded and handed to the model rather than ending the run:
// a refused capability or a stale digest is something the model can act on, and
// a loop that stopped at the first refusal would make a hostile repository's
// first probe a denial of service.
func (l *Loop) callTool(ctx context.Context, call aiprovider.ToolCall) ToolRecord {
	started := l.opts.Now()
	l.opts.Emit(Event{Kind: "tool", Text: call.Name})

	record := ToolRecord{Name: call.Name, Args: call.Arguments}
	arguments := make(map[string]any)
	if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
		record.Failed = true
		record.Result = "the arguments were not a JSON object: " + err.Error()
		record.Elapsed = l.opts.Now().Sub(started)
		return record
	}

	answer, err := l.opts.Tools.CallTool(ctx, &mcp.CallToolParams{
		Name:      call.Name,
		Arguments: arguments,
	})
	record.Elapsed = l.opts.Now().Sub(started)
	if err != nil {
		record.Failed = true
		record.Result = "the tool could not be called: " + err.Error()
		return record
	}

	record.Failed = answer.IsError
	record.Result, record.Truncated = l.renderResult(answer)
	return record
}

// renderResult turns a tool result into the text the model reads.
//
// Structured content is preferred over the text blocks when both are present,
// because it is the answer's own shape rather than a rendering of it. The
// truncation is stated in the text rather than applied silently: a model that
// received half a listing and was not told would report the half as the whole.
func (l *Loop) renderResult(answer *mcp.CallToolResult) (string, bool) {
	rendered := ""
	if answer.StructuredContent != nil {
		if encoded, err := json.Marshal(answer.StructuredContent); err == nil {
			rendered = string(encoded)
		}
	}
	if rendered == "" {
		parts := make([]string, 0, len(answer.Content))
		for _, content := range answer.Content {
			if text, isText := content.(*mcp.TextContent); isText {
				parts = append(parts, text.Text)
			}
		}
		rendered = strings.Join(parts, "\n")
	}
	if len(rendered) <= l.opts.MaxToolOutputBytes {
		return rendered, false
	}
	return rendered[:l.opts.MaxToolOutputBytes] +
		fmt.Sprintf("\n\n[Ptah truncated this result at %d bytes. Ask for a narrower "+
			"query rather than treating this as the whole answer.]", l.opts.MaxToolOutputBytes), true
}

// toolDefinitions reads the tool list from the server, so the model is offered
// exactly what an external client would be.
func (l *Loop) toolDefinitions(ctx context.Context) ([]aiprovider.ToolDefinition, error) {
	listed, err := l.opts.Tools.ListTools(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("list Ptah tools: %w", err)
	}
	definitions := make([]aiprovider.ToolDefinition, 0, len(listed.Tools))
	for _, tool := range listed.Tools {
		schema, marshalErr := json.Marshal(tool.InputSchema)
		if marshalErr != nil {
			return nil, fmt.Errorf("encode the schema of %q: %w", tool.Name, marshalErr)
		}
		definitions = append(definitions, aiprovider.ToolDefinition{
			Name:        tool.Name,
			Description: tool.Description,
			Schema:      schema,
		})
	}
	if len(definitions) == 0 {
		return nil, errors.New("the Ptah tool surface offered no tools")
	}
	return definitions, nil
}
