package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// resultReasonText is the reason the results below give.
const resultReasonText = "the engine refused the statement while compiling it"

func reasonedExecution(outcome protocol.Execution_Outcome, applied uint32, reason string) *protocol.Execution {
	return &protocol.Execution{Outcome: outcome, AppliedCount: applied, Message: "cannot write db.example:2136/local",
		Diagnostic: &protocol.ProviderDiagnostic{Reason: reason}}
}

func reasonedTransaction(outcome protocol.TransactionResult_Outcome, reason string) *protocol.TransactionResult {
	return &protocol.TransactionResult{Outcome: outcome, Message: "cannot commit on db.example:2136/local",
		Diagnostic: &protocol.ProviderDiagnostic{Reason: reason}}
}

// An execution that did not apply every unit may say why, and the reason
// reaches the ExecutionError beside the outcome. The result's message never
// does.
func TestExecutionResultReason_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		outcome protocol.Execution_Outcome
		applied uint32
		cause   error
		want    string
	}{
		{name: "nothing applied", outcome: protocol.Execution_NOT_APPLIED, cause: provider.ErrExecution,
			want: "provider execution: NOT_APPLIED (0 units confirmed applied): " + resultReasonText},
		{name: "nothing started", outcome: protocol.Execution_NOT_STARTED, cause: provider.ErrExecution,
			want: "provider execution: NOT_STARTED (0 units confirmed applied): " + resultReasonText},
		{name: "a prefix applied", outcome: protocol.Execution_PARTIAL, applied: 1, cause: provider.ErrExecution,
			want: "provider execution: PARTIAL (1 units confirmed applied): " + resultReasonText},
		{name: "an unknown outcome", outcome: protocol.Execution_UNKNOWN, applied: 1, cause: provider.ErrOutcomeUnknown,
			want: "provider execution: UNKNOWN (1 units confirmed applied): " + resultReasonText},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := provider.CheckExecution(2, reasonedExecution(test.outcome, test.applied, resultReasonText))

			c.Assert(err, qt.ErrorIs, test.cause)
			c.Assert(err.Error(), qt.Equals, test.want)
			var failure *provider.ExecutionError
			c.Assert(err, qt.ErrorAs, &failure)
			c.Assert(failure.Reason, qt.Equals, resultReasonText)
		})
	}
}

// A stop at the statement timeout keeps the reason after the timeout clause.
func TestExecutionResultReasonAtTheStatementTimeout(t *testing.T) {
	c := qt.New(t)
	request := &protocol.ExecuteRequest{StatementTimeoutNanoseconds: 1, Units: []*protocol.Unit{{ChangeId: "unit"}}}
	result := reasonedExecution(protocol.Execution_NOT_APPLIED, 0, "the statement ran longer than its timeout")
	result.StatementTimedOut = true

	err := provider.CheckExecuted(request, result)

	c.Assert(err, qt.ErrorIs, provider.ErrStatementTimeout)
	c.Assert(err.Error(), qt.Equals,
		"provider execution: NOT_APPLIED at the statement timeout (0 units confirmed applied): the statement ran longer than its timeout")
}

// A reason the check cannot read gives the error no reason: a malformed one,
// and any reason of a malformed reply, whose outcome is unknown. The result's
// message is never a substitute.
func TestExecutionResultReason_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		outcome protocol.Execution_Outcome
		applied uint32
		reason  string
		cause   error
		want    string
	}{
		{name: "a reason of two lines", outcome: protocol.Execution_NOT_APPLIED, reason: "refused\nat line 1", cause: provider.ErrExecution,
			want: "provider execution: NOT_APPLIED (0 units confirmed applied)"},
		{name: "an empty reason", outcome: protocol.Execution_PARTIAL, applied: 1, cause: provider.ErrExecution,
			want: "provider execution: PARTIAL (1 units confirmed applied)"},
		{name: "a reply whose count contradicts its outcome", outcome: protocol.Execution_NOT_APPLIED, applied: 1, reason: resultReasonText,
			cause: provider.ErrOutcomeUnknown, want: "provider execution: UNKNOWN (0 units confirmed applied)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := provider.CheckExecution(2, reasonedExecution(test.outcome, test.applied, test.reason))

			c.Assert(err, qt.ErrorIs, test.cause)
			c.Assert(err.Error(), qt.Equals, test.want)
			var failure *provider.ExecutionError
			c.Assert(err, qt.ErrorAs, &failure)
			c.Assert(failure.Reason, qt.Equals, "")
		})
	}
}

// A diagnostic beside the outcome its request succeeds with fails nothing.
func TestResultReasonBesideASuccess(t *testing.T) {
	c := qt.New(t)
	c.Assert(provider.CheckExecution(2, reasonedExecution(protocol.Execution_APPLIED, 2, resultReasonText)), qt.IsNil)
	c.Assert(provider.CheckTransactionResult(protocol.FinishTransactionRequest_COMMIT,
		reasonedTransaction(protocol.TransactionResult_COMMITTED, resultReasonText)), qt.IsNil)
	c.Assert(provider.CheckTransactionResult(protocol.FinishTransactionRequest_ROLLBACK,
		reasonedTransaction(protocol.TransactionResult_NOT_STARTED, resultReasonText)), qt.IsNil)
}

// A transaction result its action does not succeed with may say why, and the
// reason reaches the TransactionError beside the outcome.
func TestTransactionResultReason_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		action  protocol.FinishTransactionRequest_Action
		outcome protocol.TransactionResult_Outcome
		cause   error
		want    string
	}{
		{name: "a commit the engine rolled back", action: protocol.FinishTransactionRequest_COMMIT, outcome: protocol.TransactionResult_ROLLED_BACK,
			cause: provider.ErrExecution, want: "provider transaction outcome is ROLLED_BACK: " + resultReasonText},
		{name: "a commit of a transaction never started", action: protocol.FinishTransactionRequest_COMMIT, outcome: protocol.TransactionResult_NOT_STARTED,
			cause: provider.ErrExecution, want: "provider transaction outcome is NOT_STARTED: " + resultReasonText},
		{name: "a commit whose outcome is unknown", action: protocol.FinishTransactionRequest_COMMIT, outcome: protocol.TransactionResult_UNKNOWN,
			cause: provider.ErrOutcomeUnknown, want: "provider transaction outcome is UNKNOWN: " + resultReasonText},
		{name: "a rollback the engine committed", action: protocol.FinishTransactionRequest_ROLLBACK, outcome: protocol.TransactionResult_COMMITTED,
			cause: provider.ErrExecution, want: "provider transaction outcome is COMMITTED: " + resultReasonText},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := provider.CheckTransactionResult(test.action, reasonedTransaction(test.outcome, resultReasonText))

			c.Assert(err, qt.ErrorIs, test.cause)
			c.Assert(err.Error(), qt.Equals, test.want)
			var failure *provider.TransactionError
			c.Assert(err, qt.ErrorAs, &failure)
			c.Assert(failure.Reason, qt.Equals, resultReasonText)
		})
	}
}

// A malformed reason gives a transaction's error no reason, and the result's
// message is never a substitute.
func TestTransactionResultReason_FailurePath(t *testing.T) {
	tests := []struct {
		name, reason string
	}{
		{name: "a reason of two lines", reason: "rolled back\nby the engine"},
		{name: "an empty reason", reason: ""},
		{name: "a padded reason", reason: " rolled back by the engine"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := provider.CheckTransactionResult(protocol.FinishTransactionRequest_COMMIT,
				reasonedTransaction(protocol.TransactionResult_ROLLED_BACK, test.reason))

			c.Assert(err, qt.ErrorIs, provider.ErrExecution)
			c.Assert(err.Error(), qt.Equals, "provider transaction outcome is ROLLED_BACK")
			var failure *provider.TransactionError
			c.Assert(err, qt.ErrorAs, &failure)
			c.Assert(failure.Reason, qt.Equals, "")
		})
	}
}
