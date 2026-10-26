package main

import (
	"context"
	"os"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

const fixtureInputName = "PTAH_FIXTURE_EXECUTION_INPUT"

func (p *owner) DescribeExecutionInputs(ctx context.Context, request *protocol.DescribeExecutionInputsRequest) (*protocol.ExecutionInputs, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := &protocol.ExecutionInputs{Complete: true}
	for _, unit := range request.GetUnits() {
		name := unit.GetNative().GetOwner().GetName()
		if name == "needs_input" || strings.Contains(unit.GetSql(), `"needs_input"`) {
			result.Inputs = []*protocol.ExecutionInput{{Source: protocol.ExecutionInput_ENVIRONMENT, Name: fixtureInputName}}
			break
		}
	}
	if len(result.Inputs) == 0 {
		return result, nil
	}
	if p.inputMode == "changed" && p.inputCalls.Add(1) > 1 || p.inputMode == "batch-extra" && len(request.GetUnits()) > 1 {
		result.Inputs = append(result.Inputs, &protocol.ExecutionInput{Source: protocol.ExecutionInput_ENVIRONMENT, Name: "UNAPPROVED_INPUT"})
	}
	return result, nil
}

func (p *owner) validInputBindings(request *protocol.ExecuteRequest) bool {
	if p.inputMode == "" {
		return true
	}
	if _, inherited := os.LookupEnv(fixtureInputName); inherited {
		return false
	}
	for _, binding := range request.GetInputBindings() {
		if binding.GetInput().GetName() != fixtureInputName {
			return false
		}
		if p.inputMode == "empty" && len(binding.GetValue()) != 0 {
			return false
		}
		if p.inputMode != "empty" && !slices.Equal(binding.GetValue(), []byte("fixture-private-value")) {
			return false
		}
	}
	return true
}
