package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func executionInputsExchange() (*protocol.Description, *protocol.DescribeExecutionInputsRequest, *protocol.ExecutionInputs, *protocol.ExecuteRequest) {
	d := description()
	d.Features = append(d.Features, provider.ExecuteFeature, provider.SensitiveInputsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.SensitiveInputsFeature)
	request := &protocol.DescribeExecutionInputsRequest{Engine: d.Engines[0].Id, Units: []*protocol.Unit{{ChangeId: "create", Body: &protocol.Unit_Sql{Sql: "CREATE SECRET secret WITH (value = $DECLARED_INPUT)"}}}}
	input := &protocol.ExecutionInput{Source: protocol.ExecutionInput_ENVIRONMENT, Name: "DECLARED_INPUT"}
	result := &protocol.ExecutionInputs{Complete: true, Inputs: []*protocol.ExecutionInput{input}}
	execute := &protocol.ExecuteRequest{Engine: request.Engine, SessionId: "session", RequestId: "apply", Units: request.Units,
		ExpectedInputs: []*protocol.ExecutionInput{proto.CloneOf(input)}, InputBindings: []*protocol.ExecutionInputBinding{{Input: proto.CloneOf(input), Value: []byte("private value")}}}
	return d, request, result, execute
}

func TestExecutionInputsPreserveReferencesAndEmptyBindings(t *testing.T) {
	c := qt.New(t)
	d, request, result, execute := executionInputsExchange()
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateExecutionInputs(d, request, result), qt.IsNil)
	c.Assert(provider.ValidateExecuteRequest(d, execute), qt.IsNil)
	execute.InputBindings[0].Value = nil
	c.Assert(provider.ValidateExecuteRequest(d, execute), qt.IsNil)
	c.Assert(provider.CheckExecutionInputs(execute.ExpectedInputs, result.Inputs), qt.IsNil)
	result.Inputs[0].Name = "UNAPPROVED_INPUT"
	c.Assert(provider.CheckExecutionInputs(execute.ExpectedInputs, result.Inputs), qt.ErrorIs, provider.ErrProtocol)
	legacy := provider.NegotiatedDescription(d, nil)
	c.Assert(provider.ValidateExecutionInputs(legacy, request, result), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(provider.ValidateExecuteRequest(legacy, execute), qt.ErrorIs, provider.ErrProtocol)
	execute.Engine, execute.ExpectedInputs, execute.InputBindings = "", nil, nil
	c.Assert(provider.ValidateExecuteRequest(legacy, execute), qt.IsNil)
	d.Features = slices.DeleteFunc(d.Features, func(feature string) bool { return feature == provider.ExecuteFeature })
	c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, ".*sensitive inputs require execution")
}

func TestExecutionInputBindingsRefuseMismatchWithoutValues(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.ExecuteRequest)
	}{
		{"missing engine", func(r *protocol.ExecuteRequest) { r.Engine = "" }},
		{"missing binding", func(r *protocol.ExecuteRequest) { r.InputBindings = nil }},
		{"repeated binding", func(r *protocol.ExecuteRequest) {
			r.InputBindings = append(r.InputBindings, proto.CloneOf(r.InputBindings[0]))
		}},
		{"unapproved input", func(r *protocol.ExecuteRequest) { r.InputBindings[0].Input.Name = "OTHER" }},
		{"unapproved source", func(r *protocol.ExecuteRequest) { r.InputBindings[0].Input.Source = 99 }},
		{"unsorted references", func(r *protocol.ExecuteRequest) {
			r.ExpectedInputs = append(r.ExpectedInputs, &protocol.ExecutionInput{Source: protocol.ExecutionInput_ENVIRONMENT, Name: "AAA"})
		}},
		{"repeated reference", func(r *protocol.ExecuteRequest) {
			r.ExpectedInputs = append(r.ExpectedInputs, proto.CloneOf(r.ExpectedInputs[0]))
		}},
		{"invalid variable name", func(r *protocol.ExecuteRequest) { r.ExpectedInputs[0].Name = "INVALID=NAME" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _, request := executionInputsExchange()
			test.mutate(request)
			err := provider.ValidateExecuteRequest(d, request)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err.Error(), qt.Not(qt.Contains), "private value")
		})
	}
}

func TestExecutionInputDiscoveryIsCompleteAndBound(t *testing.T) {
	c := qt.New(t)
	d, request, result, _ := executionInputsExchange()
	result.Complete = false
	c.Assert(provider.ValidateExecutionInputs(d, request, result), qt.ErrorIs, provider.ErrProtocol)
	result.Complete = true
	request.Units = nil
	c.Assert(provider.ValidateExecutionInputs(d, request, result), qt.ErrorIs, provider.ErrProtocol)
	result.Inputs = nil
	c.Assert(provider.ValidateExecutionInputs(d, request, result), qt.IsNil)
	request.Engine = "other"
	c.Assert(provider.ValidateExecutionInputsRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
}

func TestExecutionInputsDoNotRequireEveryEngineToDiscover(t *testing.T) {
	c := qt.New(t)
	d, discovery, result, request := executionInputsExchange()
	d.Engines[0].Features = nil
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateExecutionInputs(d, discovery, result), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(provider.ValidateExecuteRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
	request.ExpectedInputs, request.InputBindings = nil, nil
	c.Assert(provider.ValidateExecuteRequest(d, request), qt.IsNil)
}
