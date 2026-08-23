// Package aiprovider is the provider-neutral contract Ptah Assist talks to a
// model through: bring your own key, your own model, and your own endpoint.
//
// # Why Ptah owns this rather than borrowing it
//
// A server could have asked the client for a completion instead -- the protocol
// had a mechanism for exactly that, and it needed no key of Ptah's. It is
// deprecated as of protocol revision 2026-07-28, and the migration the
// specification names is "integrate directly with LLM provider APIs". So Ptah
// Assist owns its provider clients, and the local-first consequence is the
// feature rather than the cost: the model can be one running on the operator's
// own machine, and nothing has to leave it.
//
// # What is normalized, and what deliberately is not
//
// Normalized: the message shape, tool definitions and tool calls, stop reasons,
// token usage, and the error taxonomy a caller has to branch on. Those are the
// parts every provider expresses differently and means the same by.
//
// Not normalized: whether a model can call tools at all, how large its context
// is, and what it costs. Those differ by model rather than by provider, they
// change without a Ptah release, and a value invented here would be a
// confident answer about something Ptah did not measure. [Provider.Probe]
// measures the one that decides whether Assist can run at all.
//
// # Credentials
//
// A provider is configured with a credential REFERENCE, resolved at the moment
// a request is made and never stored, logged, or placed in a message. Ptah
// writes no key anywhere: the reference names an environment variable or a file
// the operator already controls.
package aiprovider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Role names who produced a message.
type Role string

const (
	// RoleUser is the person, or Ptah speaking on their behalf.
	RoleUser Role = "user"
	// RoleAssistant is the model.
	RoleAssistant Role = "assistant"
	// RoleTool carries one tool's result back to the model.
	RoleTool Role = "tool"
)

// Message is one turn.
//
// The system instruction is not a message here. Providers disagree about
// whether it is a message, a separate field, or a prefix, and carrying it in
// [Request.System] lets each adapter place it where its own API wants rather
// than having every adapter unpick the first element of a list.
type Message struct {
	Role Role
	// Content is the text. An assistant turn that only called tools has none.
	Content string
	// ToolCalls are what an assistant turn asked for.
	ToolCalls []ToolCall
	// ToolCallID and ToolName identify which call a RoleTool message answers.
	ToolCallID string
	ToolName   string
}

// ToolDefinition is one tool offered to the model.
//
// Schema is raw JSON Schema rather than a Go type, because the definitions come
// from the agent contract, which already derives them from its own request
// types. Re-deriving them here would be a second source for the same shape.
type ToolDefinition struct {
	Name        string
	Description string
	Schema      json.RawMessage
}

// ToolCall is the model asking for one tool.
type ToolCall struct {
	ID        string
	Name      string
	Arguments json.RawMessage
}

// StopReason is why the model stopped.
type StopReason string

const (
	// StopEnd is a complete answer.
	StopEnd StopReason = "end"
	// StopToolCalls means the model wants tools run before it continues.
	StopToolCalls StopReason = "tool_calls"
	// StopMaxTokens means the output limit ended the turn, so the answer is
	// truncated rather than finished. Reporting it as an end is how a caller
	// comes to act on half a sentence.
	StopMaxTokens StopReason = "max_tokens"
	// StopFiltered means the provider refused to return what the model produced.
	StopFiltered StopReason = "content_filtered"
	// StopUnknown is a reason this build does not recognize, carried rather
	// than mapped onto one of the others.
	StopUnknown StopReason = "unknown"
)

// Usage is what the turn cost, in the provider's own counting.
//
// Cost in money is deliberately absent: it depends on a price list that changes
// without a Ptah release, and a stale multiplier reported as a number is worse
// than no number. A caller that wants money can multiply these by prices it
// knows.
type Usage struct {
	InputTokens       int
	OutputTokens      int
	CachedInputTokens int
}

// Request is one turn's input.
type Request struct {
	// System is the instruction block. It is Ptah's text.
	System string
	// Messages is the conversation so far, oldest first.
	Messages []Message
	// Tools are the tools the model may call.
	Tools []ToolDefinition
	// MaxOutputTokens bounds the answer. Zero lets the provider decide.
	MaxOutputTokens int
	// Temperature is passed through when set. A nil value sends nothing, which
	// is not the same as sending zero.
	Temperature *float64
}

// Response is one turn's output.
type Response struct {
	// Model is what the provider says answered, which is not always what was
	// asked for: a gateway may route elsewhere, and the session record should
	// show what actually ran.
	Model      string
	Message    Message
	StopReason StopReason
	Usage      Usage
	// RequestID is the provider's own identifier where it supplies one, for
	// correlating a Ptah session with a provider's logs.
	RequestID string
}

// Probe is what a provider answered about itself.
//
// Every field is measured rather than declared. A provider that documents tool
// calling and a deployment that does not support it are indistinguishable from
// documentation, and Ptah Assist cannot run without the capability.
type Probe struct {
	Provider string
	Model    string
	// Reachable reports that the endpoint answered at all.
	Reachable bool
	// Authenticated reports that the credential was accepted.
	Authenticated bool
	// ModelListed reports that the configured model appears in the provider's
	// own list. A false value is not fatal -- plenty of gateways serve models
	// they do not list -- so it is reported rather than enforced.
	ModelListed bool
	// ToolCalling reports that the endpoint returned a tool call when asked
	// for one. This is the capability Assist requires.
	ToolCalling bool
	// Latency is how long the probe's own round trip took, which for a local
	// model is mostly how long it took to load.
	Latency time.Duration
	// Notes carry what the probe saw and could not turn into a boolean.
	Notes []string
}

// Provider is one configured way to reach a model.
//
// Implementations are adapters over an HTTP API and hold no conversation state:
// the caller owns the message list, because the caller is the one that has to
// decide what to send and what to leave out.
type Provider interface {
	// Profile is the operator's name for this configuration.
	Profile() string
	// Model is the configured model identifier.
	Model() string
	// Chat sends one turn.
	Chat(ctx context.Context, req Request) (*Response, error)
	// Probe checks the configuration without sending any project content.
	Probe(ctx context.Context) (Probe, error)
}

// ErrorKind classifies a provider failure into the categories a caller acts on
// differently.
//
// The categories are chosen so a person reading a message can tell four things
// apart that otherwise all read as "it did not work": their key is wrong, their
// model name is wrong, the provider is busy, and the model produced something
// unusable. #1488 asks for exactly that distinction, and names the failure to
// make it -- turning everything into "could not complete the task".
type ErrorKind string

const (
	// KindAuth is a credential the provider refused.
	KindAuth ErrorKind = "auth"
	// KindUnknownModel is a model identifier the provider does not serve.
	KindUnknownModel ErrorKind = "unknown_model"
	// KindRateLimited is a provider asking for less traffic.
	KindRateLimited ErrorKind = "rate_limited"
	// KindTooLarge is a request the provider refused for its size, including a
	// context window the conversation outgrew.
	KindTooLarge ErrorKind = "too_large"
	// KindTimeout is a request that did not answer in time.
	KindTimeout ErrorKind = "timeout"
	// KindUnavailable is a provider that is reachable and not serving.
	KindUnavailable ErrorKind = "unavailable"
	// KindUnreachable is an endpoint that did not answer at all: a wrong base
	// URL, a stopped local server, a network that is not there.
	KindUnreachable ErrorKind = "unreachable"
	// KindContentFiltered is a provider refusing to return an answer.
	KindContentFiltered ErrorKind = "content_filtered"
	// KindToolsUnsupported is an endpoint that refused the tool definitions.
	KindToolsUnsupported ErrorKind = "tools_unsupported"
	// KindMalformedResponse is an answer this package could not read, which is
	// the common shape of an "OpenAI-compatible" endpoint that is not.
	KindMalformedResponse ErrorKind = "malformed_response"
	// KindProvider is everything else the provider reported.
	KindProvider ErrorKind = "provider_error"
)

// Error is a normalized provider failure.
//
// It carries the provider's own message, because the normalization is for
// branching and the original text is what a person needs to act. It never
// carries the credential: nothing in this package puts a key into a message, so
// the absence is structural rather than a filter over the text.
type Error struct {
	Kind ErrorKind
	// Profile and Model name the configuration that failed, so a session with
	// two providers says which one.
	Profile string
	Model   string
	// Status is the HTTP status, zero when the failure was not an HTTP answer.
	Status int
	// Message is the provider's own words, or Ptah's where there were none.
	Message string
	// RetryAfter is what the provider asked for, zero when it asked for
	// nothing.
	RetryAfter time.Duration
	// Err is the underlying cause, for errors.Is against transport sentinels.
	Err error
}

// Error names the profile, the classification and the provider's message.
func (e *Error) Error() string {
	if e.Status == 0 {
		return fmt.Sprintf("%s (%s): %s", e.Profile, e.Kind, e.Message)
	}
	return fmt.Sprintf("%s (%s, HTTP %d): %s", e.Profile, e.Kind, e.Status, e.Message)
}

// Unwrap exposes the transport cause.
func (e *Error) Unwrap() error { return e.Err }

// Retryable reports whether sending the same request again could succeed.
//
// It is a property of the classification rather than a field, so a new failure
// kind cannot be added without deciding this about it. Note what is absent: a
// request the provider ANSWERED is never retried here, whatever the answer --
// deciding to try again after a model produced something unusable belongs to
// the loop that knows whether a tool already ran.
func (e *Error) Retryable() bool {
	switch e.Kind {
	case KindRateLimited, KindUnavailable, KindTimeout, KindUnreachable:
		return true
	case KindAuth, KindUnknownModel, KindTooLarge, KindContentFiltered,
		KindToolsUnsupported, KindMalformedResponse, KindProvider:
		return false
	}
	return false
}

// KindOf reports a provider error's classification, and KindProvider for
// anything that is not one.
func KindOf(err error) ErrorKind {
	if providerErr, is := errors.AsType[*Error](err); is {
		return providerErr.Kind
	}
	return KindProvider
}

// ErrToolCallingUnsupported reports a model that cannot do what Assist needs.
//
// It is a sentinel because the caller's response to it is specific and stated
// in #1488: refuse the agent mode with a clear capability error rather than
// silently degrading into a mode that invents SQL and claims it was validated.
var ErrToolCallingUnsupported = errors.New(
	"the selected model does not provide the tool-calling capabilities Ptah Assist requires")
