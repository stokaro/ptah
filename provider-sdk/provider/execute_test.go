package provider_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

func timedExecuteRequest() *protocol.ExecuteRequest {
	sql := func(text string) *protocol.Unit {
		return &protocol.Unit{ChangeId: text, Body: &protocol.Unit_Sql{Sql: text}}
	}
	return &protocol.ExecuteRequest{SessionId: "session", RequestId: "run-1", Units: []*protocol.Unit{sql("a"), sql("b")},
		StatementTimeoutNanoseconds: int64(3 * time.Second)}
}

// timeoutDescription is a negotiated description in which both sides list
// statement timeouts.
func timeoutDescription() *protocol.Description {
	return &protocol.Description{Features: []string{provider.ExecuteFeature, provider.StatementTimeoutFeature}}
}

func TestValidateExecuteRequest_HappyPath(t *testing.T) {
	c := qt.New(t)
	request := timedExecuteRequest()
	c.Assert(provider.ValidateExecuteRequest(timeoutDescription(), request), qt.IsNil)
	request.StatementTimeoutNanoseconds = 0
	c.Assert(provider.ValidateExecuteRequest(timeoutDescription(), request), qt.IsNil)
	// A request without a timeout needs no negotiation: it is a request of
	// contract 25.0.
	c.Assert(provider.ValidateExecuteRequest(&protocol.Description{}, request), qt.IsNil)
}

func TestValidateExecuteRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ExecuteRequest)
	}{
		{"negative statement timeout", func(r *protocol.ExecuteRequest) { r.StatementTimeoutNanoseconds = -1 }},
		{"no session", func(r *protocol.ExecuteRequest) { r.SessionId = "" }},
		{"no request ID", func(r *protocol.ExecuteRequest) { r.RequestId = "" }},
		{"no units", func(r *protocol.ExecuteRequest) { r.Units = nil }},
		{"an empty unit", func(r *protocol.ExecuteRequest) { r.Units[0].Body = &protocol.Unit_Sql{Sql: " "} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := timedExecuteRequest()
			test.mutate(request)
			c.Assert(provider.ValidateExecuteRequest(timeoutDescription(), request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	description := dataDescription()
	description.Features = append(description.Features, provider.StatementTimeoutFeature)
	c.Assert(provider.ValidateDescription(description, nil), qt.ErrorMatches, `.*statement timeouts require execution`)
	description.Features = append(description.Features, provider.ExecuteFeature)
	c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
	notNegotiated := &protocol.Description{Features: []string{provider.ExecuteFeature}}
	c.Assert(provider.ValidateExecuteRequest(notNegotiated, timedExecuteRequest()), qt.ErrorMatches, `.*statement timeouts were not negotiated`)
}

// A stop at the statement timeout is a confirmed outcome that says so: the
// caller can tell its own limit from a canceled call.
func TestCheckExecutedStatementTimeout_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		result  *protocol.Execution
		want    error
		applied uint32
	}{
		{"nothing applied", &protocol.Execution{Outcome: protocol.Execution_NOT_APPLIED, StatementTimedOut: true}, provider.ErrExecution, 0},
		{"a prefix applied", &protocol.Execution{Outcome: protocol.Execution_PARTIAL, AppliedCount: 1, StatementTimedOut: true}, provider.ErrExecution, 1},
		{"the server may still run it", &protocol.Execution{Outcome: protocol.Execution_UNKNOWN, AppliedCount: 1, StatementTimedOut: true}, provider.ErrOutcomeUnknown, 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := provider.CheckExecuted(timedExecuteRequest(), test.result)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(err, qt.ErrorIs, provider.ErrStatementTimeout)
			var failed *provider.ExecutionError
			c.Assert(err, qt.ErrorAs, &failed)
			c.Assert(*failed, qt.Equals, provider.ExecutionError{Outcome: test.result.Outcome, Applied: test.applied, TimedOut: true})
			c.Assert(err, qt.ErrorMatches, `provider execution: .* at the statement timeout \(\d units confirmed applied\)`)
		})
	}
	c := qt.New(t)
	c.Assert(provider.CheckExecuted(timedExecuteRequest(), &protocol.Execution{Outcome: protocol.Execution_APPLIED, AppliedCount: 2}), qt.IsNil)
}

// A timed-out flag the request could not have produced, or on an outcome
// that confirms completion, is a malformed reply: an unknown outcome, never a
// timeout.
func TestCheckExecutedStatementTimeout_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		timeout int64
		result  *protocol.Execution
	}{
		{"a request without a timeout", 0, &protocol.Execution{Outcome: protocol.Execution_NOT_APPLIED, StatementTimedOut: true}},
		{"an applied batch", int64(time.Second), &protocol.Execution{Outcome: protocol.Execution_APPLIED, AppliedCount: 2, StatementTimedOut: true}},
		{"a batch not started", int64(time.Second), &protocol.Execution{Outcome: protocol.Execution_NOT_STARTED, StatementTimedOut: true}},
		{"a malformed prefix", int64(time.Second), &protocol.Execution{Outcome: protocol.Execution_UNKNOWN, AppliedCount: 3, StatementTimedOut: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := timedExecuteRequest()
			request.StatementTimeoutNanoseconds = test.timeout
			err := provider.CheckExecuted(request, test.result)
			c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
			c.Assert(err, qt.Not(qt.ErrorIs), provider.ErrStatementTimeout)
		})
	}
	c := qt.New(t)
	err := provider.CheckExecuted(timedExecuteRequest(), &protocol.Execution{Outcome: protocol.Execution_NOT_APPLIED})
	c.Assert(err, qt.ErrorIs, provider.ErrExecution)
	c.Assert(err, qt.Not(qt.ErrorIs), provider.ErrStatementTimeout)
}
