package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// passwordInputRender is commonRender with schema.password-inputs offered by
// the provider and its engine and listed by the host, and the role changes
// carrying their passwords as inputs.
func passwordInputRender() (*protocol.Description, *protocol.RenderRequest) {
	d, request, _ := commonRender()
	d.Features = append(d.Features, provider.ExecuteFeature, provider.SensitiveInputsFeature, provider.PasswordInputsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.SensitiveInputsFeature, provider.PasswordInputsFeature)
	request.TargetFeatures = []string{provider.PasswordInputsFeature}
	creation := request.Changes[5].GetCommonObject().GetCreateRole()
	creation.Password, creation.PasswordInput = "", passwordInput("PTAH_SECRET_PASSWORD_APP")
	alteration := request.Changes[6].GetCommonObject().GetAlterRole()
	alteration.Password, alteration.PasswordInput = "", passwordInput("PTAH_SECRET_PASSWORD_APP")
	return d, request
}

func passwordInput(name string) *protocol.ExecutionInput {
	return &protocol.ExecutionInput{Source: protocol.ExecutionInput_ENVIRONMENT, Name: name}
}

func TestRenderPasswordInputs_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request := passwordInputRender()

	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateRenderRequest(d, request), qt.IsNil)
	// An alteration that removes the password names neither a value nor an
	// input.
	request.Changes[6].GetCommonObject().GetAlterRole().PasswordInput = nil
	c.Assert(provider.ValidateRenderRequest(d, request), qt.IsNil)
}

// A peer that does not negotiate the feature keeps the value on the wire, as
// contract 25.3 sends it.
func TestRenderPasswordValuesWithoutTheFeature_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request, _ := commonRender()

	c.Assert(provider.ValidateRenderRequest(d, request), qt.IsNil)
	c.Assert(request.Changes[5].GetCommonObject().GetCreateRole().GetPassword(), qt.Equals, "secret")
}

func TestRenderPasswordInputs_FailurePath(t *testing.T) {
	creation := func(r *protocol.RenderRequest) *protocol.RoleCreation {
		return r.Changes[5].GetCommonObject().GetCreateRole()
	}
	alteration := func(r *protocol.RenderRequest) *protocol.RoleAlteration {
		return r.Changes[6].GetCommonObject().GetAlterRole()
	}
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description, *protocol.RenderRequest)
	}{
		{"a value once the feature is negotiated", `.*a negotiated password travels as an execution input, never as a value`,
			func(_ *protocol.Description, r *protocol.RenderRequest) {
				creation(r).PasswordInput, creation(r).Password = nil, "secret"
			}},
		{"an altered value once the feature is negotiated", `.*a negotiated password travels as an execution input, never as a value`,
			func(_ *protocol.Description, r *protocol.RenderRequest) {
				alteration(r).PasswordInput, alteration(r).Password = nil, "next"
			}},
		{"an input the host did not list", `.*a password input was not negotiated`,
			func(_ *protocol.Description, r *protocol.RenderRequest) { r.TargetFeatures = nil }},
		{"an input the engine does not offer", `.*a password input was not negotiated`,
			func(d *protocol.Description, r *protocol.RenderRequest) {
				d.Engines[0].Features = []string{provider.ValidateSchemaFeature, provider.CommonObjectsFeature, provider.SensitiveInputsFeature}
				r.TargetFeatures = nil
			}},
		{"a value beside an input", `.*a negotiated password travels as an execution input, never as a value`,
			func(_ *protocol.Description, r *protocol.RenderRequest) { creation(r).Password = "secret" }},
		{"an input without a source", `.*a password input needs a known source and a valid environment variable name`,
			func(_ *protocol.Description, r *protocol.RenderRequest) {
				creation(r).PasswordInput.Source = protocol.ExecutionInput_SOURCE_UNSPECIFIED
			}},
		{"an input whose name is no variable", `.*a password input needs a known source and a valid environment variable name`,
			func(_ *protocol.Description, r *protocol.RenderRequest) {
				creation(r).PasswordInput.Name = "1 PASSWORD"
			}},
		{"an input for a role that declares no password", `.*a role carries a password exactly when it declares one`,
			func(_ *protocol.Description, r *protocol.RenderRequest) { creation(r).Role.PasswordDeclared = false }},
		{"an input for an alteration that keeps the password", `.*a password travels only when it changes`,
			func(_ *protocol.Description, r *protocol.RenderRequest) { alteration(r).Changed = []string{"login"} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request := passwordInputRender()
			test.mutate(d, request)

			c.Assert(provider.ValidateRenderRequest(d, request), qt.ErrorMatches, test.want)
		})
	}
}

// The value never reaches a refusal, whichever check refuses it.
func TestRenderPasswordInputRefusalsOmitTheValue_FailurePath(t *testing.T) {
	c := qt.New(t)
	d, request := passwordInputRender()
	request.Changes[5].GetCommonObject().GetCreateRole().Password = "SENTINEL-value"

	err := provider.ValidateRenderRequest(d, request)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "SENTINEL")
}

func TestPlanningPasswordInputs_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request := commonPlanning()
	d.Features = append(d.Features, provider.ExecuteFeature, provider.SensitiveInputsFeature, provider.PasswordInputsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.SensitiveInputsFeature, provider.PasswordInputsFeature)
	request.TargetFeatures = append(request.TargetFeatures, provider.PasswordInputsFeature)
	request.CommonSteps[0].CommonObject = &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_CreateRole{CreateRole: &protocol.RoleCreation{
		Role: &protocol.SQLRole{Name: "app", Login: true, PasswordDeclared: true}, PasswordInput: passwordInput("PTAH_SECRET_PASSWORD_APP")}}}

	c.Assert(provider.ValidatePlanningRequest(d, request), qt.IsNil)
}

func TestPlanningPasswordInputs_FailurePath(t *testing.T) {
	c := qt.New(t)
	d, request := commonPlanning()
	request.CommonSteps[0].CommonObject = &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_CreateRole{CreateRole: &protocol.RoleCreation{
		Role: &protocol.SQLRole{Name: "app", Login: true, PasswordDeclared: true}, PasswordInput: passwordInput("PTAH_SECRET_PASSWORD_APP")}}}

	c.Assert(provider.ValidatePlanningRequest(d, request), qt.ErrorMatches, `.*a password input was not negotiated`)
}

func TestPasswordInputsEngine_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name     string
		provider []string
		engine   []string
		want     string
	}{
		{"without sensitive inputs", []string{provider.PasswordInputsFeature}, []string{provider.PasswordInputsFeature},
			`.*password inputs require the engine's common objects and sensitive inputs`},
		{"without the provider's feature", []string{provider.SensitiveInputsFeature},
			[]string{provider.SensitiveInputsFeature, provider.PasswordInputsFeature}, `.*engine schema\.password-inputs requires the provider's`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d := commonDescription()
			d.Features = append(d.Features, provider.ExecuteFeature)
			d.Features = append(d.Features, test.provider...)
			d.Engines[0].Features = append(d.Engines[0].Features, test.engine...)

			c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, test.want)
		})
	}
}

// A host that does not list the feature sees a description without it.
func TestPasswordInputsAreTwoWay(t *testing.T) {
	c := qt.New(t)
	d, _ := passwordInputRender()

	negotiated := provider.NegotiatedDescription(d, nil)

	c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.PasswordInputsFeature)
	c.Assert(negotiated.GetFeatures(), qt.Not(qt.Contains), provider.PasswordInputsFeature)
	c.Assert(negotiated.GetEngines()[0].GetFeatures(), qt.Not(qt.Contains), provider.PasswordInputsFeature)
}
