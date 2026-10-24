package runtime

// White-box testing required: the call deadline the client sets is observable
// only to the peer it dispatches to, and the saturation bound only through the
// unexported helper that computes it.

import (
	"context"
	"math"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type executePeer struct {
	protocol.ProviderClient
	result *protocol.Execution
	budget time.Duration
	calls  int
}

func (p *executePeer) Execute(ctx context.Context, _ *protocol.ExecuteRequest, _ ...grpc.CallOption) (*protocol.Execution, error) {
	p.calls++
	deadline, _ := ctx.Deadline()
	p.budget = time.Until(deadline)
	return p.result, nil
}

func timedRequest(timeout time.Duration) *protocol.ExecuteRequest {
	unit := func(id string) *protocol.Unit {
		return &protocol.Unit{ChangeId: id, Body: &protocol.Unit_Sql{Sql: "ALTER TABLE big ADD INDEX " + id}}
	}
	return &protocol.ExecuteRequest{SessionId: "session", RequestId: "run-1", Units: []*protocol.Unit{unit("a"), unit("b")},
		StatementTimeoutNanoseconds: int64(timeout)}
}

// A statement timeout widens the transport bound by the timeout of each unit,
// so the provider's stop arrives as a confirmed outcome; a request without
// one keeps the ordinary bound.
func TestExecuteBoundCoversTheStatementTimeouts(t *testing.T) {
	c := qt.New(t)
	peer := &executePeer{result: &protocol.Execution{Outcome: protocol.Execution_APPLIED, AppliedCount: 2}}
	client := &Client{description: timeoutDescription(), rpc: peer}
	_, err := client.Execute(c.Context(), timedRequest(time.Minute))
	c.Assert(err, qt.IsNil)
	c.Assert(peer.budget > CallTimeout+time.Minute, qt.IsTrue)
	c.Assert(peer.budget <= CallTimeout+2*time.Minute, qt.IsTrue)
	_, err = client.Execute(c.Context(), timedRequest(0))
	c.Assert(err, qt.IsNil)
	c.Assert(peer.budget <= CallTimeout, qt.IsTrue)
	c.Assert(executeTimeout(timedRequest(math.MaxInt64)), qt.Equals, time.Duration(math.MaxInt64))
}

// A provider's stop at the timeout reaches the caller with its confirmed
// prefix and as a timeout, not as a canceled call.
func TestExecuteReportsAStopAtTheStatementTimeout(t *testing.T) {
	c := qt.New(t)
	peer := &executePeer{result: &protocol.Execution{Outcome: protocol.Execution_PARTIAL, AppliedCount: 1, StatementTimedOut: true}}
	client := &Client{description: timeoutDescription(), rpc: peer}
	result, err := client.Execute(c.Context(), timedRequest(time.Second))
	c.Assert(err, qt.ErrorIs, provider.ErrStatementTimeout)
	c.Assert(err, qt.ErrorIs, provider.ErrExecution)
	c.Assert(err, qt.Not(qt.ErrorIs), context.DeadlineExceeded)
	c.Assert(result.AppliedCount, qt.Equals, uint32(1))
	_, err = client.Execute(c.Context(), timedRequest(0))
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(err, qt.Not(qt.ErrorIs), provider.ErrStatementTimeout)
	_, err = client.Execute(c.Context(), timedRequest(-time.Second))
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(peer.calls, qt.Equals, 2)
}

// timeoutDescription offers statement timeouts, as negotiated with a host
// that lists them.
func timeoutDescription() *protocol.Description {
	return &protocol.Description{Features: []string{provider.ExecuteFeature, provider.StatementTimeoutFeature}}
}

func TestExecuteServerRefusesANegativeStatementTimeout(t *testing.T) {
	c := qt.New(t)
	server := negotiatedServer(Server{Description: timeoutDescription(), Executor: executorOwner{}}, provider.StatementTimeoutFeature)
	_, err := server.Execute(c.Context(), timedRequest(-time.Second))
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	result, err := server.Execute(c.Context(), timedRequest(time.Second))
	c.Assert(err, qt.IsNil)
	c.Assert(result.Outcome, qt.Equals, protocol.Execution_APPLIED)
}

// A statement timeout travels only when both sides list it: the client does
// not send one to a provider that does not offer it, the server refuses one
// from a host that did not list it, and a provider never reports a stop at a
// timeout the request did not set, which a host of contract 25.0 would not
// understand.
func TestExecuteStatementTimeoutNeedsBothSides(t *testing.T) {
	c := qt.New(t)
	peer := &executePeer{result: &protocol.Execution{Outcome: protocol.Execution_APPLIED, AppliedCount: 2}}
	client := &Client{description: &protocol.Description{Features: []string{provider.ExecuteFeature}}, rpc: peer}
	_, err := client.Execute(c.Context(), timedRequest(time.Second))
	c.Assert(err, qt.ErrorMatches, `.*statement timeouts were not negotiated`)
	c.Assert(peer.calls, qt.Equals, 0)
	unlisted := negotiatedServer(Server{Description: timeoutDescription(), Executor: executorOwner{}})
	_, err = unlisted.Execute(c.Context(), timedRequest(time.Second))
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	// Before Describe, the host is taken to list nothing.
	undescribed := newServerAdapter(Server{Description: timeoutDescription(), Executor: executorOwner{}})
	_, err = undescribed.Execute(c.Context(), timedRequest(time.Second))
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	careless := negotiatedServer(Server{Description: timeoutDescription(), Executor: timingOutOwner{}})
	_, err = careless.Execute(c.Context(), timedRequest(0))
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	result, err := unlisted.Execute(c.Context(), timedRequest(0))
	c.Assert(err, qt.IsNil)
	c.Assert(result.Outcome, qt.Equals, protocol.Execution_APPLIED)
}

// timingOutOwner reports a stop at a statement timeout whatever it was asked.
type timingOutOwner struct{}

func (timingOutOwner) Execute(context.Context, *protocol.ExecuteRequest) (*protocol.Execution, error) {
	return &protocol.Execution{Outcome: protocol.Execution_NOT_APPLIED, StatementTimedOut: true}, nil
}
