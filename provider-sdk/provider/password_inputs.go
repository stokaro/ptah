package provider

import (
	"slices"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// PasswordInputsFeature lets a declared role password travel as an execution
// input instead of as a value (ADR 0071). With it negotiated, a host sends
// RoleCreation.password_input and RoleAlteration.password_input and never
// RoleCreation.password or RoleAlteration.password, whether the declaration
// holds a password or a password hash. The provider renders a unit that names
// the input and not the value, DescribeExecutionInputs reports the input, and
// the value reaches the provider only as an execution binding. The provider
// and each engine that takes such inputs list it, beside
// SensitiveInputsFeature and CommonObjectsFeature, and the host lists it in
// its host features. Contract 25.22.
const PasswordInputsFeature = "schema.password-inputs"

// validatePasswordInputsEngine refuses an engine that lists
// PasswordInputsFeature without the services the input travels through: the
// common-object wire that carries the role, and the sensitive inputs that
// bind its value at execution.
func validatePasswordInputsEngine(_ *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), PasswordInputsFeature) {
		return nil
	}
	if !slices.Contains(engine.GetFeatures(), SensitiveInputsFeature) || !slices.Contains(engine.GetFeatures(), CommonObjectsFeature) {
		return commonObjectError("password inputs require the engine's common objects and sensitive inputs")
	}
	return nil
}

// validateRolePassword checks how a role change carries its password. Without
// PasswordInputsFeature negotiated for engine, a password travels as a value
// and no input may be named. With it, a password travels only as an input,
// with a source and a name the sensitive-input grammar accepts, and no value
// may travel at all. Its errors never repeat a value.
func validateRolePassword(description *protocol.Description, engine *protocol.Engine, features []string, change *protocol.CommonObjectChange) error {
	var value string
	var input *protocol.ExecutionInput
	switch operation := change.GetOperation().(type) {
	case *protocol.CommonObjectChange_CreateRole:
		value, input = operation.CreateRole.GetPassword(), operation.CreateRole.GetPasswordInput()
	case *protocol.CommonObjectChange_AlterRole:
		value, input = operation.AlterRole.GetPassword(), operation.AlterRole.GetPasswordInput()
	default:
		return nil
	}
	negotiated := EngineOffers(description, engine.GetId(), PasswordInputsFeature) && slices.Contains(features, PasswordInputsFeature)
	switch {
	case input != nil && !negotiated:
		return commonObjectError("a password input was not negotiated")
	case value != "" && negotiated:
		return commonObjectError("a negotiated password travels as an execution input, never as a value")
	case input != nil && validateExecutionInputReferences([]*protocol.ExecutionInput{input}) != nil:
		return commonObjectError("a password input needs a known source and a valid environment variable name")
	}
	return nil
}
