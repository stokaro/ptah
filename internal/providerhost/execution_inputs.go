package providerhost

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/renderer"
	"ptah.run/dbschema"
)

type preparedExecution struct {
	units  []*protocol.Unit
	inputs []*protocol.ExecutionInput
}

var _ dbschema.UnitInputPreparer = (*session)(nil)

// PrepareUnitInputs discovers references without resolving any input value.
// The caller must approve and retain the returned fragments before execution.
func (s *session) PrepareUnitInputs(ctx context.Context, fragments []renderer.Fragment) ([]renderer.Fragment, error) {
	units, snapshots, err := s.encodeUnits(ctx, fragments)
	if err != nil {
		return nil, err
	}
	for i, unit := range units {
		snapshots[i].Inputs, err = s.target.renderedInputs(ctx, unit)
		if err != nil {
			return nil, err
		}
	}
	return snapshots, ctx.Err()
}

func (t *target) executionInputs(ctx context.Context, units []*protocol.Unit) ([]*protocol.ExecutionInput, error) {
	if !t.negotiated(provider.SensitiveInputsFeature) {
		return nil, ctx.Err()
	}
	result, err := t.host.client.DescribeExecutionInputs(ctx, &protocol.DescribeExecutionInputsRequest{Engine: t.engine.GetId(), Units: units})
	if err != nil {
		return nil, err
	}
	return result.GetInputs(), ctx.Err()
}

func (t *target) renderedInputs(ctx context.Context, unit *protocol.Unit) ([]renderer.InputReference, error) {
	inputs, err := t.executionInputs(ctx, []*protocol.Unit{unit})
	if err != nil {
		return nil, err
	}
	var refs []renderer.InputReference
	for _, input := range inputs {
		refs = append(refs, renderer.InputReference{Source: renderer.InputEnvironment, Name: input.GetName()})
	}
	return refs, nil
}

func wireInputs(refs []renderer.InputReference) ([]*protocol.ExecutionInput, error) {
	if err := renderer.ValidateInputReferences(refs); err != nil {
		return nil, err
	}
	var inputs []*protocol.ExecutionInput
	for _, ref := range refs {
		inputs = append(inputs, &protocol.ExecutionInput{Source: protocol.ExecutionInput_ENVIRONMENT, Name: ref.Name})
	}
	return inputs, nil
}

func (s *session) approvedInputs(ctx context.Context, fragments []renderer.Fragment, units []*protocol.Unit) ([]*protocol.ExecutionInput, error) {
	byName := make(map[string]*protocol.ExecutionInput)
	for i, fragment := range fragments {
		expected, err := wireInputs(fragment.Inputs)
		if err != nil {
			return nil, err
		}
		actual, err := s.target.executionInputs(ctx, units[i:i+1])
		if err != nil {
			return nil, err
		}
		if err := provider.CheckExecutionInputs(expected, actual); err != nil {
			return nil, err
		}
		for _, input := range expected {
			byName[input.GetName()] = input
		}
	}
	var inputs []*protocol.ExecutionInput
	for _, name := range slices.Sorted(maps.Keys(byName)) {
		inputs = append(inputs, byName[name])
	}
	return inputs, nil
}

// bindExecutionInputs checks the exact batch against approved references before
// reading any value. Neither discovery nor preflight reads the environment.
func (s *session) bindExecutionInputs(ctx context.Context, request *protocol.ExecuteRequest, expected []*protocol.ExecutionInput) error {
	if !slices.Contains(s.target.host.description.GetFeatures(), provider.SensitiveInputsFeature) {
		return nil
	}
	request.Engine = s.target.engine.GetId()
	actual, err := s.target.executionInputs(ctx, request.Units)
	if err != nil {
		return err
	}
	if err := provider.CheckExecutionInputs(expected, actual); err != nil {
		return err
	}
	for _, input := range expected {
		if err := ctx.Err(); err != nil {
			return err
		}
		value, present := os.LookupEnv(input.GetName())
		if !present {
			return fmt.Errorf("provider execution input environment variable %q is not set", input.GetName())
		}
		request.ExpectedInputs = append(request.ExpectedInputs, proto.CloneOf(input))
		request.InputBindings = append(request.InputBindings, &protocol.ExecutionInputBinding{Input: proto.CloneOf(input), Value: []byte(value)})
	}
	return ctx.Err()
}

func clearExecutionInputs(request *protocol.ExecuteRequest) {
	for _, binding := range request.InputBindings {
		clear(binding.Value)
	}
	request.InputBindings = nil
}
