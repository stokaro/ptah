package aiprovider

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// Fake is a provider that answers from a script.
//
// It exists so the agent loop can be tested without a model. A test that drove
// a real provider would be measuring the model's mood: the same prompt produces
// a tool call on one run and a paragraph on the next, and a suite that flakes
// for that reason teaches people to rerun rather than to read.
//
// It is not reachable from the command line: `type: fake` is not a profile type
// and `--provider-profile fake` names nothing. An operator checking whether
// Ptah's half of the loop works uses `ptah assist provider test`, which
// measures their endpoint rather than replacing it.
type Fake struct {
	mu      sync.Mutex
	turns   []Response
	next    int
	prompts []Request

	// ProbeResult is what Probe answers. The zero value reports a provider that
	// works, because a fake that failed its own probe would need configuring
	// before it could be used to avoid configuring anything.
	ProbeResult Probe
	// Err, when set, is returned by every Chat call instead of a turn.
	Err error
}

// NewFake builds a provider that answers with the given turns in order.
func NewFake(turns ...Response) *Fake {
	return &Fake{
		turns: turns,
		ProbeResult: Probe{
			Provider:      "fake",
			Model:         "fake-model",
			Reachable:     true,
			Authenticated: true,
			ModelListed:   true,
			ToolCalling:   true,
		},
	}
}

// Profile names the fake.
func (f *Fake) Profile() string { return "fake" }

// Model names the fake's model.
func (f *Fake) Model() string { return "fake-model" }

// Chat returns the next scripted turn.
//
// Running out of script is an error rather than a repeat of the last turn: a
// loop that asked for one more turn than the test scripted has done something
// the test did not describe, and answering it anyway hides that.
func (f *Fake) Chat(_ context.Context, req Request) (*Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.prompts = append(f.prompts, req)
	if f.Err != nil {
		return nil, f.Err
	}
	if f.next >= len(f.turns) {
		return nil, &Error{
			Kind:    KindProvider,
			Profile: "fake",
			Model:   "fake-model",
			Message: fmt.Sprintf("the script has %d turns and the caller asked for %d",
				len(f.turns), f.next+1),
		}
	}
	turn := f.turns[f.next]
	f.next++
	return &turn, nil
}

// Probe answers with ProbeResult.
func (f *Fake) Probe(context.Context) (Probe, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ProbeResult, nil
}

// Prompts returns every request the fake was sent, so a test can assert what
// the loop actually put in front of the model -- which is the property that
// matters for a context-and-privacy claim.
func (f *Fake) Prompts() []Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Request(nil), f.prompts...)
}

// TextTurn is a scripted answer with no tool calls.
func TextTurn(text string) Response {
	return Response{
		Model:      "fake-model",
		Message:    Message{Role: RoleAssistant, Content: text},
		StopReason: StopEnd,
		Usage:      Usage{InputTokens: 1, OutputTokens: 1},
	}
}

// ToolTurn is a scripted answer asking for one tool.
func ToolTurn(id, name string, arguments any) Response {
	encoded, err := json.Marshal(arguments)
	if err != nil {
		encoded = []byte("{}")
	}
	return Response{
		Model: "fake-model",
		Message: Message{
			Role:      RoleAssistant,
			ToolCalls: []ToolCall{{ID: id, Name: name, Arguments: encoded}},
		},
		StopReason: StopToolCalls,
		Usage:      Usage{InputTokens: 1, OutputTokens: 1},
	}
}
