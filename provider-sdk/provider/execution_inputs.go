package provider

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// SensitiveInputsFeature enables offline input discovery and ephemeral
// execution bindings. Both peers negotiate it. Values never enter units.
const SensitiveInputsFeature = "units.sensitive-inputs"

// ExecutionInputDescriber identifies the inputs a complete batch requires.
// Discovery is deterministic, performs no I/O and reads no input values.
// The host reviews references before approval and resolves values at dispatch.
type ExecutionInputDescriber interface {
	DescribeExecutionInputs(context.Context, *protocol.DescribeExecutionInputsRequest) (*protocol.ExecutionInputs, error)
}

func executionInputError(reason string) error {
	return fmt.Errorf("%w: execution inputs: %s", ErrProtocol, reason)
}

// ValidateExecutionInputsRequest checks an offline batch before input discovery.
func ValidateExecutionInputsRequest(description *protocol.Description, request *protocol.DescribeExecutionInputsRequest) error {
	if request.GetEngine() == "" || !slices.Contains(description.GetFeatures(), SensitiveInputsFeature) {
		return executionInputError("an explicit engine and negotiated feature are required")
	}
	engine, _, err := valueTarget(description, request.GetEngine(), nil)
	if err != nil {
		return err
	}
	if !slices.Contains(engine.GetFeatures(), SensitiveInputsFeature) {
		return executionInputError("the engine does not describe sensitive inputs")
	}
	return validateExecutionInputUnits(description, request)
}

func validateExecutionInputUnits(description *protocol.Description, request *protocol.DescribeExecutionInputsRequest) error {
	seen := make(map[string]bool)
	for _, unit := range request.GetUnits() {
		if err := ValidateUnit(unit); err != nil {
			return err
		}
		if seen[unit.GetChangeId()] {
			return executionInputError("a unit ID is repeated")
		}
		seen[unit.GetChangeId()] = true
		if native := unit.GetNative(); native != nil && (native.GetProviderId() != description.GetProviderId() || native.GetOwner().GetEngine() != request.GetEngine()) {
			return executionInputError("a native unit belongs to another provider or engine")
		}
	}
	return nil
}

// ValidateExecutionInputs requires a complete, sorted list of unique input
// references. Its errors never contain binding values or provider payloads.
func ValidateExecutionInputs(description *protocol.Description, request *protocol.DescribeExecutionInputsRequest, result *protocol.ExecutionInputs) error {
	if err := ValidateExecutionInputsRequest(description, request); err != nil {
		return err
	}
	if !result.GetComplete() {
		return executionInputError("incomplete discovery")
	}
	if len(request.GetUnits()) == 0 && len(result.GetInputs()) != 0 {
		return executionInputError("an empty batch has no inputs")
	}
	return validateExecutionInputReferences(result.GetInputs())
}

func validateExecutionInputReferences(inputs []*protocol.ExecutionInput) error {
	previous := ""
	for _, input := range inputs {
		if input.GetSource() != protocol.ExecutionInput_ENVIRONMENT || !environmentInputName(input.GetName()) {
			return executionInputError("an input needs a known source and valid environment variable name")
		}
		if input.GetName() <= previous {
			return executionInputError("input references must be unique and sorted")
		}
		previous = input.GetName()
	}
	return nil
}

func environmentInputName(name string) bool {
	if name == "" {
		return false
	}
	for i, char := range name {
		if char != '_' && (char < 'A' || char > 'Z') && (char < 'a' || char > 'z') && (i == 0 || char < '0' || char > '9') {
			return false
		}
	}
	return true
}

func validateExecuteInputs(description *protocol.Description, request *protocol.ExecuteRequest) error {
	if !slices.Contains(description.GetFeatures(), SensitiveInputsFeature) {
		if request.GetEngine() != "" || len(request.GetExpectedInputs())+len(request.GetInputBindings()) != 0 {
			return executionInputError("sensitive inputs were not negotiated")
		}
		return nil
	}
	inputRequest := &protocol.DescribeExecutionInputsRequest{Engine: request.GetEngine(), Units: request.GetUnits()}
	if request.GetEngine() == "" {
		return executionInputError("an explicit engine is required")
	}
	engine, _, err := valueTarget(description, request.GetEngine(), nil)
	if err != nil {
		return err
	}
	if !slices.Contains(engine.GetFeatures(), SensitiveInputsFeature) && len(request.GetExpectedInputs())+len(request.GetInputBindings()) != 0 {
		return executionInputError("the engine does not describe sensitive inputs")
	}
	if err := validateExecutionInputUnits(description, inputRequest); err != nil {
		return err
	}
	if err := validateExecutionInputReferences(request.GetExpectedInputs()); err != nil {
		return err
	}
	expected := make(map[string]bool)
	for _, input := range request.GetExpectedInputs() {
		expected[input.GetName()] = true
	}
	bound := make(map[string]bool)
	for _, binding := range request.GetInputBindings() {
		input := binding.GetInput()
		if input.GetSource() != protocol.ExecutionInput_ENVIRONMENT || !expected[input.GetName()] || bound[input.GetName()] {
			return executionInputError("bindings must name exactly the approved inputs once")
		}
		bound[input.GetName()] = true
	}
	if len(expected) != len(bound) {
		return executionInputError("an approved input has no binding")
	}
	return nil
}

// CheckExecutionInputs requires dispatch-time discovery to match the approved
// references exactly. Values are neither compared nor exposed in diagnostics.
func CheckExecutionInputs(expected, actual []*protocol.ExecutionInput) error {
	if err := validateExecutionInputReferences(expected); err != nil {
		return err
	}
	if err := validateExecutionInputReferences(actual); err != nil {
		return err
	}
	if !slices.EqualFunc(expected, actual, func(a, b *protocol.ExecutionInput) bool {
		return a.GetSource() == b.GetSource() && a.GetName() == b.GetName()
	}) {
		return executionInputError("dispatch inputs differ from the approved references")
	}
	return nil
}
