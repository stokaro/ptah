package runtime

// White-box testing required: the negotiated state of the server adapter is
// set by Describe and read by every later call, and a result from a peer that
// is not the SDK server must reach the client's own checks. A test drives
// each end alone, to show that either one keeps a reason the host did not
// negotiate, or a malformed one, out of the host's error.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// resultReason is the reason the owners below give.
const resultReason = "the engine refused the statement while compiling it"

// resultsDescription offers execution and transactions, and errors.reasons.
func resultsDescription() *protocol.Description {
	features := []string{provider.ExecuteFeature, provider.TransactionsFeature, provider.ErrorReasonsFeature}
	return &protocol.Description{
		Revision: provider.Revision, ProviderId: "example.org/reasons", Version: "1", SemanticIdentity: "reasons-1", Features: features,
		Engines: []*protocol.Engine{{
			Id: "reasons", Features: []string{provider.TransactionsFeature},
			Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT,
				IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE},
		}},
	}
}

// resultsOwner answers every execution and every finish with its canned
// result, a message that quotes an address, and a diagnostic with reason.
type resultsOwner struct {
	idleTransactions
	execution *protocol.Execution
	finished  *protocol.TransactionResult
}

func newResultsOwner(execution protocol.Execution_Outcome, finished protocol.TransactionResult_Outcome, reason string) *resultsOwner {
	applied := uint32(0)
	if execution == protocol.Execution_APPLIED {
		applied = 1
	}
	return &resultsOwner{
		execution: &protocol.Execution{Outcome: execution, AppliedCount: applied, Message: secretText, Diagnostic: &protocol.ProviderDiagnostic{Reason: reason}},
		finished:  &protocol.TransactionResult{Outcome: finished, Message: secretText, Diagnostic: &protocol.ProviderDiagnostic{Reason: reason}},
	}
}

func (o *resultsOwner) Execute(context.Context, *protocol.ExecuteRequest) (*protocol.Execution, error) {
	return o.execution, nil
}

func (o *resultsOwner) FinishTransaction(context.Context, *protocol.FinishTransactionRequest) (*protocol.TransactionResult, error) {
	return o.finished, nil
}

// resultsServer is the SDK server of owner, after a host listed hostFeatures.
func resultsServer(owner *resultsOwner, hostFeatures ...string) *serverAdapter {
	return negotiatedServer(Server{Description: resultsDescription(), Executor: owner, Transactions: owner}, hostFeatures...)
}

// resultsPeer stands for a provider outside the SDK: it sends owner's results
// without the SDK server's checks.
type resultsPeer struct {
	protocol.ProviderClient
	owner *resultsOwner
}

func (p *resultsPeer) Execute(ctx context.Context, request *protocol.ExecuteRequest, _ ...grpc.CallOption) (*protocol.Execution, error) {
	return p.owner.Execute(ctx, request)
}

func (p *resultsPeer) FinishTransaction(ctx context.Context, request *protocol.FinishTransactionRequest, _ ...grpc.CallOption) (*protocol.TransactionResult, error) {
	return p.owner.FinishTransaction(ctx, request)
}

// resultsClient is the client of a peer outside the SDK that sends owner's
// results, after a host listed hostFeatures.
func resultsClient(owner *resultsOwner, hostFeatures ...string) *Client {
	return &Client{description: provider.NegotiatedDescription(resultsDescription(), hostFeatures), rpc: &resultsPeer{owner: owner}}
}

// When the provider and the host both list the feature, the SDK server sends
// a failed result's reason, and the client puts it in the caller's error and
// nothing of the result's message.
func TestResultReasons_HappyPath(t *testing.T) {
	t.Run("the server", func(t *testing.T) {
		c := qt.New(t)
		server := resultsServer(newResultsOwner(protocol.Execution_NOT_APPLIED, protocol.TransactionResult_ROLLED_BACK, resultReason), reasonsBoth...)

		executed, err := server.Execute(c.Context(), unitRequest())
		c.Assert(err, qt.IsNil)
		c.Assert(executed.GetDiagnostic().GetReason(), qt.Equals, resultReason)
		finished, err := server.FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_COMMIT))
		c.Assert(err, qt.IsNil)
		c.Assert(finished.GetDiagnostic().GetReason(), qt.Equals, resultReason)
	})
	t.Run("the client", func(t *testing.T) {
		c := qt.New(t)
		client := resultsClient(newResultsOwner(protocol.Execution_NOT_APPLIED, protocol.TransactionResult_ROLLED_BACK, resultReason), reasonsBoth...)

		executed, err := client.Execute(c.Context(), unitRequest())
		c.Assert(err, qt.ErrorIs, provider.ErrExecution)
		c.Assert(err.Error(), qt.Equals, "provider execution: NOT_APPLIED (0 units confirmed applied): "+resultReason)
		c.Assert(executed.GetDiagnostic().GetReason(), qt.Equals, resultReason)
		finished, err := client.FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_COMMIT))
		c.Assert(err, qt.ErrorIs, provider.ErrExecution)
		c.Assert(err.Error(), qt.Equals, "provider transaction outcome is ROLLED_BACK: "+resultReason)
		c.Assert(finished.GetDiagnostic().GetReason(), qt.Equals, resultReason)
	})
}

// Without the feature on both sides, each end alone removes the reason, and
// neither changes the owner's result.
func TestResultReasonsWithoutTheFeature(t *testing.T) {
	t.Run("the server", func(t *testing.T) {
		c := qt.New(t)
		owner := newResultsOwner(protocol.Execution_NOT_APPLIED, protocol.TransactionResult_ROLLED_BACK, resultReason)
		server := resultsServer(owner)

		executed, err := server.Execute(c.Context(), unitRequest())
		c.Assert(err, qt.IsNil)
		c.Assert(executed.GetOutcome(), qt.Equals, protocol.Execution_NOT_APPLIED)
		c.Assert(executed.GetDiagnostic(), qt.IsNil)
		finished, err := server.FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_COMMIT))
		c.Assert(err, qt.IsNil)
		c.Assert(finished.GetOutcome(), qt.Equals, protocol.TransactionResult_ROLLED_BACK)
		c.Assert(finished.GetDiagnostic(), qt.IsNil)
		c.Assert(owner.execution.GetDiagnostic().GetReason(), qt.Equals, resultReason)
		c.Assert(owner.finished.GetDiagnostic().GetReason(), qt.Equals, resultReason)
	})
	t.Run("the client", func(t *testing.T) {
		c := qt.New(t)
		owner := newResultsOwner(protocol.Execution_NOT_APPLIED, protocol.TransactionResult_ROLLED_BACK, resultReason)
		client := resultsClient(owner)

		executed, err := client.Execute(c.Context(), unitRequest())
		c.Assert(err, qt.ErrorIs, provider.ErrExecution)
		c.Assert(err.Error(), qt.Equals, "provider execution: NOT_APPLIED (0 units confirmed applied)")
		c.Assert(executed.GetDiagnostic(), qt.IsNil)
		finished, err := client.FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_COMMIT))
		c.Assert(err, qt.ErrorIs, provider.ErrExecution)
		c.Assert(err.Error(), qt.Equals, "provider transaction outcome is ROLLED_BACK")
		c.Assert(finished.GetDiagnostic(), qt.IsNil)
		c.Assert(owner.execution.GetDiagnostic().GetReason(), qt.Equals, resultReason)
		c.Assert(owner.finished.GetDiagnostic().GetReason(), qt.Equals, resultReason)
	})
}

// A malformed reason keeps the result's outcome: each end alone puts the
// SDK's sentence in its place rather than refusing the result, which would
// make a confirmed outcome unknown.
func TestResultReasonsThatAreMalformed(t *testing.T) {
	const twoLines = "refused\nat line 1"
	t.Run("the server", func(t *testing.T) {
		c := qt.New(t)
		owner := newResultsOwner(protocol.Execution_NOT_APPLIED, protocol.TransactionResult_ROLLED_BACK, twoLines)
		server := resultsServer(owner, reasonsBoth...)

		executed, err := server.Execute(c.Context(), unitRequest())
		c.Assert(err, qt.IsNil)
		c.Assert(executed.GetOutcome(), qt.Equals, protocol.Execution_NOT_APPLIED)
		c.Assert(executed.GetDiagnostic().GetReason(), qt.Equals, malformedReason)
		finished, err := server.FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_COMMIT))
		c.Assert(err, qt.IsNil)
		c.Assert(finished.GetOutcome(), qt.Equals, protocol.TransactionResult_ROLLED_BACK)
		c.Assert(finished.GetDiagnostic().GetReason(), qt.Equals, malformedReason)
		c.Assert(owner.execution.GetDiagnostic().GetReason(), qt.Equals, twoLines)
	})
	t.Run("the client", func(t *testing.T) {
		c := qt.New(t)
		client := resultsClient(newResultsOwner(protocol.Execution_NOT_APPLIED, protocol.TransactionResult_ROLLED_BACK, twoLines), reasonsBoth...)

		_, err := client.Execute(c.Context(), unitRequest())
		c.Assert(err, qt.ErrorIs, provider.ErrExecution)
		c.Assert(err.Error(), qt.Equals, "provider execution: NOT_APPLIED (0 units confirmed applied): "+malformedReason)
		_, err = client.FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_COMMIT))
		c.Assert(err, qt.ErrorIs, provider.ErrExecution)
		c.Assert(err.Error(), qt.Equals, "provider transaction outcome is ROLLED_BACK: "+malformedReason)
	})
}

// A reason never accompanies a success: each end removes one beside APPLIED,
// beside COMMITTED for a commit and beside ROLLED_BACK for a rollback.
func TestResultReasonsBesideASuccess(t *testing.T) {
	t.Run("the server", func(t *testing.T) {
		c := qt.New(t)
		owner := newResultsOwner(protocol.Execution_APPLIED, protocol.TransactionResult_COMMITTED, resultReason)
		server := resultsServer(owner, reasonsBoth...)

		executed, err := server.Execute(c.Context(), unitRequest())
		c.Assert(err, qt.IsNil)
		c.Assert(executed.GetDiagnostic(), qt.IsNil)
		committed, err := server.FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_COMMIT))
		c.Assert(err, qt.IsNil)
		c.Assert(committed.GetDiagnostic(), qt.IsNil)
		owner.finished = proto.CloneOf(owner.finished)
		owner.finished.Outcome = protocol.TransactionResult_ROLLED_BACK
		rolledBack, err := server.FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_ROLLBACK))
		c.Assert(err, qt.IsNil)
		c.Assert(rolledBack.GetDiagnostic(), qt.IsNil)
	})
	t.Run("the client", func(t *testing.T) {
		c := qt.New(t)
		owner := newResultsOwner(protocol.Execution_APPLIED, protocol.TransactionResult_COMMITTED, resultReason)
		client := resultsClient(owner, reasonsBoth...)

		executed, err := client.Execute(c.Context(), unitRequest())
		c.Assert(err, qt.IsNil)
		c.Assert(executed.GetDiagnostic(), qt.IsNil)
		committed, err := client.FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_COMMIT))
		c.Assert(err, qt.IsNil)
		c.Assert(committed.GetDiagnostic(), qt.IsNil)
		c.Assert(owner.execution.GetDiagnostic().GetReason(), qt.Equals, resultReason)
	})
}
