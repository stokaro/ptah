package agentpolicy_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/agentpolicy"
)

// stubApprover stands in for the human. It is an input to the broker rather
// than a way to choose an assertion: every test below asserts directly on the
// outcome, and this only supplies the answer a person would have given.
type stubApprover struct {
	grant agentpolicy.Grant
	err   error

	calls    int
	requests []agentpolicy.Request
	subjects []agentpolicy.Subject
}

func (s *stubApprover) Approve(
	_ context.Context,
	req agentpolicy.Request,
	subject agentpolicy.Subject,
) (agentpolicy.Grant, error) {
	s.calls++
	s.requests = append(s.requests, req)
	s.subjects = append(s.subjects, subject)
	return s.grant, s.err
}

// writeMigrations is the request every broker test drives, because
// artifact.write is the capability whose default is ask and therefore the only
// one that reaches the approver.
func writeMigrations() agentpolicy.Request {
	return agentpolicy.Request{
		Capability: agentpolicy.ArtifactWrite,
		Artifact:   agentpolicy.ClassMigrations,
		Paths:      []string{"migrations/20260823_add_status.up.sql"},
		Reason:     "create one migration file",
	}
}

func patchSubject() agentpolicy.Subject {
	return agentpolicy.Subject{
		Summary: "create migrations/20260823_add_status.up.sql",
		Digest:  "sha256:0000000000000000000000000000000000000000000000000000000000000000",
	}
}

// grantingPolicy resolves artifact.write to allow, for the tests that need a
// permitted operation without an approver in the picture.
func grantingPolicy(c *qt.C) *agentpolicy.Policy {
	policy, err := agentpolicy.Assemble(agentpolicy.LayerRules{
		Layer:  agentpolicy.LayerInvocation,
		Source: "--allow-write=migrations",
		Rules: []agentpolicy.Rule{{
			Capability: agentpolicy.ArtifactWrite,
			Artifact:   agentpolicy.ClassMigrations,
			Verdict:    agentpolicy.VerdictAllow,
		}},
	})
	c.Assert(err, qt.IsNil)
	return policy
}

func defaultPolicy(c *qt.C) *agentpolicy.Policy {
	policy, err := agentpolicy.Assemble()
	c.Assert(err, qt.IsNil)
	return policy
}

func TestBroker_AllowDoesNotAsk(t *testing.T) {
	c := qt.New(t)
	approver := &stubApprover{grant: agentpolicy.Grant{Granted: true}}
	broker := agentpolicy.NewBroker(grantingPolicy(c), agentpolicy.WithApprover(approver))

	outcome, err := broker.Authorize(context.Background(), writeMigrations(), patchSubject())

	c.Assert(err, qt.IsNil)
	c.Assert(outcome.Permitted, qt.IsTrue)
	c.Assert(outcome.Approved, qt.IsFalse)
	c.Assert(approver.calls, qt.Equals, 0)
}

func TestBroker_DenyRefusesWithoutAsking(t *testing.T) {
	// A denial must not reach the approver at all. A broker that asked would
	// let a person turn a policy denial into a grant by answering yes, which is
	// the escalation path the layering exists to close.
	c := qt.New(t)
	approver := &stubApprover{grant: agentpolicy.Grant{Granted: true}}
	broker := agentpolicy.NewBroker(defaultPolicy(c), agentpolicy.WithApprover(approver))

	outcome, err := broker.Authorize(context.Background(), agentpolicy.Request{
		Capability: agentpolicy.ArtifactDelete,
		Artifact:   agentpolicy.ClassMigrations,
	}, patchSubject())

	c.Assert(err, qt.ErrorMatches, `"artifact.delete:migrations" denied by builtin policy`)
	c.Assert(outcome.Permitted, qt.IsFalse)
	c.Assert(approver.calls, qt.Equals, 0)

	var denied *agentpolicy.DeniedError
	c.Assert(err, qt.ErrorAs, &denied)
	c.Assert(denied.Decision.Layer.String(), qt.Equals, "builtin")
}

func TestBroker_AskWithoutAnApproverRefuses(t *testing.T) {
	// This is the non-interactive case, and the property is that it fails
	// closed. Reading `--non-interactive` as "approve everything" is the defect
	// this asserts against.
	c := qt.New(t)
	broker := agentpolicy.NewBroker(defaultPolicy(c))

	outcome, err := broker.Authorize(context.Background(), writeMigrations(), patchSubject())

	c.Assert(err, qt.ErrorIs, agentpolicy.ErrApprovalUnavailable)
	c.Assert(outcome.Permitted, qt.IsFalse)
	c.Assert(outcome.Decision.Verdict.String(), qt.Equals, "ask")
}

func TestBroker_AskGrantedOnceAsksEveryTime(t *testing.T) {
	c := qt.New(t)
	approver := &stubApprover{grant: agentpolicy.Grant{Granted: true, Scope: agentpolicy.GrantOnce}}
	broker := agentpolicy.NewBroker(defaultPolicy(c), agentpolicy.WithApprover(approver))

	first, firstErr := broker.Authorize(context.Background(), writeMigrations(), patchSubject())
	second, secondErr := broker.Authorize(context.Background(), writeMigrations(), patchSubject())

	c.Assert(firstErr, qt.IsNil)
	c.Assert(secondErr, qt.IsNil)
	c.Assert(first.Permitted, qt.IsTrue)
	c.Assert(second.Permitted, qt.IsTrue)
	c.Assert(first.GrantScope.String(), qt.Equals, "once")
	c.Assert(second.FromSessionGrant, qt.IsFalse)
	c.Assert(approver.calls, qt.Equals, 2)
}

func TestBroker_AskGrantedForTheSessionAsksOnce(t *testing.T) {
	c := qt.New(t)
	approver := &stubApprover{grant: agentpolicy.Grant{Granted: true, Scope: agentpolicy.GrantSession}}
	broker := agentpolicy.NewBroker(defaultPolicy(c), agentpolicy.WithApprover(approver))

	first, firstErr := broker.Authorize(context.Background(), writeMigrations(), patchSubject())
	second, secondErr := broker.Authorize(context.Background(), writeMigrations(), patchSubject())

	c.Assert(firstErr, qt.IsNil)
	c.Assert(secondErr, qt.IsNil)
	c.Assert(first.FromSessionGrant, qt.IsFalse)
	c.Assert(second.Permitted, qt.IsTrue)
	c.Assert(second.Approved, qt.IsTrue)
	c.Assert(second.FromSessionGrant, qt.IsTrue)
	c.Assert(approver.calls, qt.Equals, 1)
}

func TestBroker_ASessionGrantCoversOneScopeOnly(t *testing.T) {
	// The control for the test above: a session grant that leaked across scopes
	// would let one approval for the test directory authorize a write to the
	// migration directory.
	c := qt.New(t)
	approver := &stubApprover{grant: agentpolicy.Grant{Granted: true, Scope: agentpolicy.GrantSession}}
	broker := agentpolicy.NewBroker(defaultPolicy(c), agentpolicy.WithApprover(approver))

	_, firstErr := broker.Authorize(context.Background(), agentpolicy.Request{
		Capability: agentpolicy.ArtifactWrite,
		Artifact:   agentpolicy.ClassTests,
	}, patchSubject())
	second, secondErr := broker.Authorize(context.Background(), writeMigrations(), patchSubject())

	c.Assert(firstErr, qt.IsNil)
	c.Assert(secondErr, qt.IsNil)
	c.Assert(second.FromSessionGrant, qt.IsFalse)
	c.Assert(approver.calls, qt.Equals, 2)
}

func TestBroker_RefusedApproval(t *testing.T) {
	c := qt.New(t)
	approver := &stubApprover{grant: agentpolicy.Grant{Granted: false}}
	broker := agentpolicy.NewBroker(defaultPolicy(c), agentpolicy.WithApprover(approver))

	outcome, err := broker.Authorize(context.Background(), writeMigrations(), patchSubject())

	c.Assert(err, qt.ErrorIs, agentpolicy.ErrApprovalRefused)
	c.Assert(outcome.Permitted, qt.IsFalse)
	c.Assert(approver.calls, qt.Equals, 1)
}

func TestBroker_ApproverFailure(t *testing.T) {
	c := qt.New(t)
	transport := errors.New("client closed the elicitation")
	approver := &stubApprover{err: transport}
	broker := agentpolicy.NewBroker(defaultPolicy(c), agentpolicy.WithApprover(approver))

	outcome, err := broker.Authorize(context.Background(), writeMigrations(), patchSubject())

	c.Assert(err, qt.ErrorIs, transport)
	c.Assert(outcome.Permitted, qt.IsFalse)
}

func TestBroker_TheApproverSeesTheExactSubject(t *testing.T) {
	// An approval that named the directory rather than the digest would approve
	// whatever the directory holds when the write lands.
	c := qt.New(t)
	approver := &stubApprover{grant: agentpolicy.Grant{Granted: true}}
	broker := agentpolicy.NewBroker(defaultPolicy(c), agentpolicy.WithApprover(approver))

	_, err := broker.Authorize(context.Background(), writeMigrations(), patchSubject())

	c.Assert(err, qt.IsNil)
	c.Assert(approver.subjects, qt.HasLen, 1)
	c.Assert(approver.subjects[0].Digest, qt.Equals, patchSubject().Digest)
	c.Assert(approver.subjects[0].Summary, qt.Equals, patchSubject().Summary)
	c.Assert(approver.requests[0].String(), qt.Equals, "artifact.write:migrations")
}

func TestBroker_TheRecorderSeesRefusals(t *testing.T) {
	// Scenario 6 of #1487 asks for an audit record showing the denied
	// capability requests a hostile repository provoked. A recorder wired only
	// into the success path would report a clean session.
	c := qt.New(t)
	var recorded []agentpolicy.Outcome
	broker := agentpolicy.NewBroker(defaultPolicy(c), agentpolicy.WithRecorder(func(outcome agentpolicy.Outcome) {
		recorded = append(recorded, outcome)
	}))

	_, writeErr := broker.Authorize(context.Background(), writeMigrations(), patchSubject())
	_, readErr := broker.Authorize(context.Background(), agentpolicy.Request{
		Capability: agentpolicy.ArtifactRead,
		Artifact:   agentpolicy.ClassSchema,
	}, agentpolicy.Subject{})

	c.Assert(writeErr, qt.ErrorIs, agentpolicy.ErrApprovalUnavailable)
	c.Assert(readErr, qt.IsNil)
	c.Assert(recorded, qt.HasLen, 2)
	c.Assert(recorded[0].Permitted, qt.IsFalse)
	c.Assert(recorded[0].Request.String(), qt.Equals, "artifact.write:migrations")
	c.Assert(recorded[0].Err, qt.ErrorIs, agentpolicy.ErrApprovalUnavailable)
	c.Assert(recorded[1].Permitted, qt.IsTrue)
}

func TestBroker_MalformedRequest(t *testing.T) {
	c := qt.New(t)
	broker := agentpolicy.NewBroker(defaultPolicy(c))

	outcome, err := broker.Authorize(context.Background(), agentpolicy.Request{
		Capability: agentpolicy.ArtifactWrite,
	}, patchSubject())

	c.Assert(err, qt.ErrorMatches, `capability "artifact.write" requires an artifact class`)
	c.Assert(outcome.Permitted, qt.IsFalse)
}
