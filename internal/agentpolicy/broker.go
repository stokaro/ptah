package agentpolicy

import (
	"context"
	"fmt"
	"sync"

	"ptah.run/internal/agentdiag"
)

// ErrApprovalUnavailable reports a [VerdictAsk] reached in a session that has
// nobody to ask.
//
// It is separate from an ordinary refusal because the two are different
// instructions to the operator: a denial says change the policy, and this says
// run somewhere a human can answer, or grant the capability up front. What it
// must never become is an allow -- a non-interactive run is exactly where the
// silent promotion would go unnoticed.
var ErrApprovalUnavailable = agentdiag.Sentinel(agentdiag.CodeApprovalUnavailable,
	"operation requires approval and this session cannot ask")

// ErrApprovalRefused reports that the person asked said no.
var ErrApprovalRefused = agentdiag.Sentinel(agentdiag.CodeApprovalRefused, "approval refused")

// Subject is the exact thing an approval is being asked about.
//
// Digest is what makes the approval exact. #1483 states the requirement
// directly: an approval must refer to one artifact, and if the inputs change
// afterwards the approval is stale. An approval carrying "the migration
// directory" instead of a digest approves whatever that directory happens to
// hold when the write lands.
type Subject struct {
	// Summary is Ptah's own sentence about the operation, never the model's.
	// The model wrote the patch; letting it also write the sentence the human
	// reads before approving it is the whole of the prompt-injection problem in
	// one field.
	Summary string
	// Digest identifies the exact artifact or patch, as "sha256:...".
	Digest string
	// Details are the labeled facts the prompt shows: paths, environment,
	// resulting digest.
	Details []Detail
}

// Detail is one labeled fact in an approval prompt.
type Detail struct {
	Label string
	Value string
}

// GrantScope says how far an approval reaches.
type GrantScope int

const (
	// GrantOnce approves this subject and nothing else. A second operation, or
	// the same operation against changed content, asks again.
	GrantOnce GrantScope = iota
	// GrantSession approves the capability and scope for the rest of the
	// session. It is offered because a workflow that asks per file trains the
	// person to approve without reading, which is worse than one deliberate
	// decision. It is not persisted anywhere: a session grant dies with the
	// process, because a durable one needs revocation semantics this phase does
	// not have.
	GrantSession
)

// String names the scope for a prompt and an audit record.
func (g GrantScope) String() string {
	switch g {
	case GrantOnce:
		return "once"
	case GrantSession:
		return "session"
	}
	return fmt.Sprintf("grant(%d)", int(g))
}

// Grant is an approver's answer.
type Grant struct {
	Granted bool
	Scope   GrantScope
}

// Approver asks a human. Implementations live at the surfaces: an interactive
// prompt in Ptah Assist, a protocol elicitation on the MCP server, and nothing
// at all in a non-interactive run, where the absence is the point.
type Approver interface {
	Approve(ctx context.Context, req Request, subject Subject) (Grant, error)
}

// Outcome is one authorization, decided.
type Outcome struct {
	Request  Request
	Decision Decision
	// Approved reports that a human was asked and said yes.
	Approved bool
	// GrantScope is meaningful only when Approved.
	GrantScope GrantScope
	// FromSessionGrant reports that an earlier approval in this session
	// answered for this one, so an audit reader can tell a decision a person
	// made now from one they made earlier.
	FromSessionGrant bool
	// Permitted is the single field a call site should branch on.
	Permitted bool
	// Err is the refusal, when there is one.
	Err error
}

// Broker resolves a request against a policy and, for [VerdictAsk], through an
// approver.
//
// It is safe for concurrent use because a provider that supports parallel tool
// calls will make two of them at once, and the session-grant map is the piece
// that would otherwise race.
type Broker struct {
	policy   *Policy
	approver Approver
	recorder func(Outcome)

	mu      sync.Mutex
	granted map[grantKey]struct{}
}

// BrokerOption configures a broker.
type BrokerOption func(*Broker)

// WithApprover supplies the human. Without one, every [VerdictAsk] refuses with
// [ErrApprovalUnavailable].
func WithApprover(approver Approver) BrokerOption {
	return func(b *Broker) { b.approver = approver }
}

// WithRecorder receives every outcome, permitted or not.
//
// Refusals are recorded through the same path as permissions on purpose: the
// acceptance scenario for a hostile repository asks for an audit record showing
// the denied capability requests, and a recorder wired only into the success
// path would show a clean session.
func WithRecorder(recorder func(Outcome)) BrokerOption {
	return func(b *Broker) { b.recorder = recorder }
}

// NewBroker binds a resolved policy to a session.
func NewBroker(policy *Policy, options ...BrokerOption) *Broker {
	broker := &Broker{policy: policy, granted: make(map[grantKey]struct{})}
	for _, option := range options {
		option(broker)
	}
	return broker
}

// Policy returns the resolved table, for a surface that reports what this
// session may do.
func (b *Broker) Policy() *Policy {
	return b.policy
}

// Authorize decides one operation, asking a human when the policy says to.
//
// The returned [Outcome] carries the refusal in its Err field as well as
// returning it, so a caller that reports structured results can render the
// decision without reconstructing it from an error string.
func (b *Broker) Authorize(ctx context.Context, req Request, subject Subject) (Outcome, error) {
	decision, err := b.policy.Decide(req)
	if err != nil {
		return Outcome{Request: req, Err: err}, err
	}
	outcome := b.resolve(ctx, req, subject, decision)
	if b.recorder != nil {
		b.recorder(outcome)
	}
	return outcome, outcome.Err
}

// resolve carries out the verdict.
func (b *Broker) resolve(ctx context.Context, req Request, subject Subject, decision Decision) Outcome {
	base := Outcome{Request: req, Decision: decision}
	switch decision.Verdict {
	case VerdictAllow:
		base.Permitted = true
		return base
	case VerdictDeny:
		base.Err = &DeniedError{Request: req, Decision: decision}
		return base
	case VerdictAsk:
		return b.ask(ctx, req, subject, base)
	}
	base.Err = &DeniedError{Request: req, Decision: decision}
	return base
}

// ask consults the session grants and then the approver.
func (b *Broker) ask(ctx context.Context, req Request, subject Subject, base Outcome) Outcome {
	target := grantKey{
		cell:     cell{Capability: req.Capability, Artifact: req.Artifact, Database: req.Database},
		TargetID: req.TargetID,
	}
	if b.hasSessionGrant(target) {
		base.Permitted = true
		base.Approved = true
		base.GrantScope = GrantSession
		base.FromSessionGrant = true
		return base
	}
	if b.approver == nil {
		base.Err = fmt.Errorf("%q: %w", req, ErrApprovalUnavailable)
		return base
	}
	grant, err := b.approver.Approve(ctx, req, subject)
	if err != nil {
		base.Err = fmt.Errorf("approve %q: %w", req, err)
		return base
	}
	if !grant.Granted {
		base.Err = fmt.Errorf("%q: %w", req, ErrApprovalRefused)
		return base
	}
	if grant.Scope == GrantSession {
		b.rememberSessionGrant(target)
	}
	base.Permitted = true
	base.Approved = true
	base.GrantScope = grant.Scope
	return base
}

// grantKey identifies what a session grant covers.
//
// It is the policy cell plus the exact target. A verdict is decided about a
// class, but a grant is given about a database: approving "this dev database
// for the rest of the session" must not silently cover the next dev database,
// and repointing a target at a different URL changes its identity, so the old
// grant does not carry to the new database either.
type grantKey struct {
	cell
	TargetID string
}

func (b *Broker) hasSessionGrant(target grantKey) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, granted := b.granted[target]
	return granted
}

func (b *Broker) rememberSessionGrant(target grantKey) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.granted[target] = struct{}{}
}

// DeniedError is a refusal that carries the decision behind it.
type DeniedError struct {
	Request  Request
	Decision Decision
}

// Error names the capability and the layer that refused it, because "permission
// denied" without either sends the reader to the wrong file.
func (e *DeniedError) Error() string {
	return fmt.Sprintf("%q denied by %s policy", e.Request, e.Decision.Layer)
}

// DiagnosticCode places a refusal in the agent error taxonomy.
//
// A denial and a hard denial answer with the same code on purpose: the caller's
// next move is identical, and the layer that refused is already in the message
// for the person who wants to know which file to edit.
func (e *DeniedError) DiagnosticCode() agentdiag.Code {
	return agentdiag.CodeCapabilityDenied
}
