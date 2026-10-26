package runtime

// White-box testing required: inject a peer and a mutable discovery service to verify that
// malformed replies and changed approval references never reach execution.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type executionInputOwner struct {
	inputs     []*protocol.ExecutionInput
	cancel     context.CancelFunc
	executed   int
	mutate     bool
	dispatched []*protocol.Unit
}

func (o *executionInputOwner) DescribeExecutionInputs(_ context.Context, request *protocol.DescribeExecutionInputsRequest) (*protocol.ExecutionInputs, error) {
	if o.mutate {
		request.Units[0].Body = &protocol.Unit_Sql{Sql: "unapproved mutation"}
	}
	if o.cancel != nil {
		o.cancel()
	}
	return &protocol.ExecutionInputs{Complete: true, Inputs: o.inputs}, nil
}

func (o *executionInputOwner) Execute(_ context.Context, request *protocol.ExecuteRequest) (*protocol.Execution, error) {
	o.executed++
	o.dispatched = request.GetUnits()
	return &protocol.Execution{Outcome: protocol.Execution_APPLIED, AppliedCount: 1}, nil
}

func runtimeInputExchange() (*protocol.Description, *protocol.ExecuteRequest, *executionInputOwner) {
	d, _, _ := normalizationExchange()
	d.Features = append(d.Features, provider.ExecuteFeature, provider.SensitiveInputsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.SensitiveInputsFeature)
	input := &protocol.ExecutionInput{Source: protocol.ExecutionInput_ENVIRONMENT, Name: "DECLARED_INPUT"}
	request := &protocol.ExecuteRequest{Engine: "test", SessionId: "session", RequestId: "apply",
		Units:          []*protocol.Unit{{ChangeId: "create", Body: &protocol.Unit_Sql{Sql: "CREATE SECRET x WITH (value = $DECLARED_INPUT)"}}},
		ExpectedInputs: []*protocol.ExecutionInput{proto.CloneOf(input)}, InputBindings: []*protocol.ExecutionInputBinding{{Input: proto.CloneOf(input)}}}
	return d, request, &executionInputOwner{inputs: []*protocol.ExecutionInput{input}}
}

func TestExecutionInputsAreRecheckedBeforeDispatch(t *testing.T) {
	c := qt.New(t)
	d, request, owner := runtimeInputExchange()
	owner.mutate = true
	server := negotiatedServer(Server{Description: d, Executor: owner, ExecutionInputs: owner}, provider.SensitiveInputsFeature)
	result, err := server.Execute(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.GetOutcome(), qt.Equals, protocol.Execution_APPLIED)
	c.Assert(owner.executed, qt.Equals, 1)
	c.Assert(proto.Equal(owner.dispatched[0], request.Units[0]), qt.IsTrue)
	owner.inputs[0].Name = "NEW_INPUT"
	result, err = server.Execute(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.FailedPrecondition)
	c.Assert(result, qt.IsNil)
	c.Assert(owner.executed, qt.Equals, 1)
	owner.inputs[0].Name = "DECLARED_INPUT"
	ctx, cancel := context.WithCancel(c.Context())
	t.Cleanup(cancel)
	owner.cancel = cancel
	result, err = server.Execute(ctx, request)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(owner.executed, qt.Equals, 1)
}

func TestExecutionSkipsDiscoveryForAnEngineWithoutInputs(t *testing.T) {
	c := qt.New(t)
	d, request, owner := runtimeInputExchange()
	d.Engines[0].Features = nil
	request.ExpectedInputs, request.InputBindings = nil, nil
	server := negotiatedServer(Server{Description: d, Executor: owner, ExecutionInputs: owner}, provider.SensitiveInputsFeature)
	result, err := server.Execute(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.GetOutcome(), qt.Equals, protocol.Execution_APPLIED)
	c.Assert(owner.executed, qt.Equals, 1)
}

type executionInputPeer struct {
	protocol.ProviderClient
	result *protocol.ExecutionInputs
	err    error
	cancel context.CancelFunc
}

func (p *executionInputPeer) DescribeExecutionInputs(context.Context, *protocol.DescribeExecutionInputsRequest, ...grpc.CallOption) (*protocol.ExecutionInputs, error) {
	if p.cancel != nil {
		p.cancel()
	}
	return p.result, p.err
}

func TestExecutionInputClientDiscardsMalformedAndCanceledReplies(t *testing.T) {
	c := qt.New(t)
	d, execute, owner := runtimeInputExchange()
	peer := &executionInputPeer{result: &protocol.ExecutionInputs{Complete: true, Inputs: owner.inputs}}
	client := &Client{description: d, rpc: peer}
	request := &protocol.DescribeExecutionInputsRequest{Engine: execute.Engine, Units: execute.Units}
	result, err := client.DescribeExecutionInputs(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.GetInputs(), qt.HasLen, 1)
	peer.result.Complete = false
	result, err = client.DescribeExecutionInputs(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.err = status.Error(codes.Unavailable, "private value")
	result, err = client.DescribeExecutionInputs(c.Context(), request)
	c.Assert(err.Error(), qt.Not(qt.Contains), "private value")
	c.Assert(result, qt.IsNil)
	peer.err, peer.result.Complete = nil, true
	ctx, cancel := context.WithCancel(c.Context())
	t.Cleanup(cancel)
	peer.cancel = cancel
	result, err = client.DescribeExecutionInputs(ctx, request)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
}
