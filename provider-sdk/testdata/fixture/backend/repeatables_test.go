package backend_test

import (
	"testing"

	"example.com/ptah-fixture-provider/backend"
	qt "github.com/frankban/quicktest"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

func repeatableScope(id string) *protocol.RepeatableScope {
	return &protocol.RepeatableScope{SessionId: id, Schema: "main", Table: "_revisions_repeatable"}
}

func prepareRepeatables(c *qt.C, owner *backend.TransactionalProvider, scope *protocol.RepeatableScope) {
	c.Helper()
	result, err := owner.PrepareRepeatables(c.Context(), &protocol.PrepareRepeatablesRequest{Scope: scope, RequestId: "prepare"})
	c.Assert(err, qt.IsNil)
	c.Assert(result.GetOutcome(), qt.Equals, protocol.RepeatableResult_APPLIED)
}

func claimRepeatable(identity, run, expected string) *protocol.MutateRepeatableRequest_Claim {
	return &protocol.MutateRepeatableRequest_Claim{Claim: &protocol.RepeatableClaim{
		Identity: identity, Insert: expected == "", ExpectedRunId: expected, RunId: run, AttemptChecksum: "h1:first",
		AttemptOrder: 1, AttemptedAtSeconds: 1700000000,
	}}
}

func mutateRepeatable(c *qt.C, owner *backend.TransactionalProvider, scope *protocol.RepeatableScope, request *protocol.MutateRepeatableRequest) *protocol.RepeatableResult {
	c.Helper()
	request.Scope, request.RequestId = scope, "write"
	result, err := owner.MutateRepeatable(c.Context(), request)
	c.Assert(err, qt.IsNil)
	return result
}

func readRepeatables(c *qt.C, owner *backend.TransactionalProvider, scope *protocol.RepeatableScope) *protocol.RepeatableSnapshot {
	c.Helper()
	result, err := owner.ReadRepeatables(c.Context(), &protocol.ReadRepeatablesRequest{Scope: scope, MaxRows: 100})
	c.Assert(err, qt.IsNil)
	return result
}

// Of two runs that read no row, the first insert claims the identity and the
// second meets a CONFLICT that names the first, across two sessions on one
// database. A claim that expects the winner then takes the row over, and the
// loser's completion conflicts and names the run that took it.
func TestRepeatableClaimsAreDecidedByTheRun(t *testing.T) {
	c := qt.New(t)
	owner := &backend.TransactionalProvider{}
	address := transactionAddress(c)
	first := repeatableScope(openTransactionSession(c, owner, address).GetId())
	second := repeatableScope(openTransactionSession(c, owner, address).GetId())
	c.Assert(readRepeatables(c, owner, first).GetExists(), qt.IsFalse)
	prepareRepeatables(c, owner, first)

	won := mutateRepeatable(c, owner, first, &protocol.MutateRepeatableRequest{Mutation: claimRepeatable("view", "run-a", "")})
	lost := mutateRepeatable(c, owner, second, &protocol.MutateRepeatableRequest{Mutation: claimRepeatable("view", "run-b", "")})
	taken := mutateRepeatable(c, owner, second, &protocol.MutateRepeatableRequest{Mutation: claimRepeatable("view", "run-b", "run-a")})
	late := mutateRepeatable(c, owner, first, &protocol.MutateRepeatableRequest{Mutation: &protocol.MutateRepeatableRequest_Complete{
		Complete: &protocol.RepeatableCompletion{Identity: "view", RunId: "run-a", Checksum: "h1:first", Body: []byte("SELECT 1;"), AppliedAtSeconds: 1700000001},
	}})

	c.Assert(won.GetOutcome(), qt.Equals, protocol.RepeatableResult_APPLIED)
	c.Assert(lost.GetOutcome(), qt.Equals, protocol.RepeatableResult_CONFLICT)
	c.Assert(lost.GetHolderRunId(), qt.Equals, "run-a")
	c.Assert(taken.GetOutcome(), qt.Equals, protocol.RepeatableResult_APPLIED)
	c.Assert(late.GetOutcome(), qt.Equals, protocol.RepeatableResult_CONFLICT)
	c.Assert(late.GetHolderRunId(), qt.Equals, "run-b")
	rows := readRepeatables(c, owner, first).GetRows()
	c.Assert(rows, qt.HasLen, 1)
	c.Assert(rows[0].GetRunId(), qt.Equals, "run-b")
	c.Assert(rows[0].GetState(), qt.Equals, protocol.RepeatableRow_RUNNING)
	c.Assert(rows[0].GetApplied(), qt.IsFalse)
}

// A completion keeps the digest, the time and the bytes, and an empty body is
// kept as one. A completion of a digest the claim did not write is REFUSED
// and changes nothing.
func TestRepeatableCompletionKeepsTheClaimedContent(t *testing.T) {
	c := qt.New(t)
	owner := &backend.TransactionalProvider{}
	scope := repeatableScope(openTransactionSession(c, owner, transactionAddress(c)).GetId())
	prepareRepeatables(c, owner, scope)
	mutateRepeatable(c, owner, scope, &protocol.MutateRepeatableRequest{Mutation: claimRepeatable("view", "run-a", "")})

	other := mutateRepeatable(c, owner, scope, &protocol.MutateRepeatableRequest{Mutation: &protocol.MutateRepeatableRequest_Complete{
		Complete: &protocol.RepeatableCompletion{Identity: "view", RunId: "run-a", Checksum: "h1:other", AppliedAtSeconds: 1700000001},
	}})
	unchanged := readRepeatables(c, owner, scope).GetRows()[0].GetApplied()
	claimed := mutateRepeatable(c, owner, scope, &protocol.MutateRepeatableRequest{Mutation: &protocol.MutateRepeatableRequest_Complete{
		Complete: &protocol.RepeatableCompletion{Identity: "view", RunId: "run-a", Checksum: "h1:first", AppliedAtSeconds: 1700000001},
	}})
	body, err := owner.ReadRepeatableBody(c.Context(), &protocol.ReadRepeatableBodyRequest{Scope: scope, Identity: "view"})

	c.Assert(other.GetOutcome(), qt.Equals, protocol.RepeatableResult_REFUSED)
	c.Assert(other.GetHolderRunId(), qt.Equals, "")
	c.Assert(unchanged, qt.IsFalse)
	c.Assert(claimed.GetOutcome(), qt.Equals, protocol.RepeatableResult_APPLIED)
	c.Assert(err, qt.IsNil)
	c.Assert(body.GetFound(), qt.IsTrue)
	c.Assert(body.GetKept(), qt.IsTrue)
	c.Assert(body.GetBody(), qt.HasLen, 0)
	row := readRepeatables(c, owner, scope).GetRows()[0]
	c.Assert(row.GetState(), qt.Equals, protocol.RepeatableRow_APPLIED)
	c.Assert(row.GetChecksum(), qt.Equals, "h1:first")
	c.Assert(row.GetBodyKept(), qt.IsTrue)
}

// A removal reports whether it deleted a row.
func TestRepeatableRemovalReportsTheRow(t *testing.T) {
	c := qt.New(t)
	owner := &backend.TransactionalProvider{}
	scope := repeatableScope(openTransactionSession(c, owner, transactionAddress(c)).GetId())
	prepareRepeatables(c, owner, scope)
	mutateRepeatable(c, owner, scope, &protocol.MutateRepeatableRequest{Mutation: claimRepeatable("view", "run-a", "")})
	remove := func() *protocol.MutateRepeatableRequest {
		return &protocol.MutateRepeatableRequest{Mutation: &protocol.MutateRepeatableRequest_Remove{Remove: &protocol.RepeatableRemoval{Identity: "view"}}}
	}

	first := mutateRepeatable(c, owner, scope, remove())
	second := mutateRepeatable(c, owner, scope, remove())

	c.Assert(first.GetRemoved(), qt.IsTrue)
	c.Assert(second.GetOutcome(), qt.Equals, protocol.RepeatableResult_APPLIED)
	c.Assert(second.GetRemoved(), qt.IsFalse)
	c.Assert(readRepeatables(c, owner, scope).GetRows(), qt.HasLen, 0)
}
