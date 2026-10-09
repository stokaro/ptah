package engine_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

const accessOperationKind schemaext.Kind = "example.org/access-operation"

// accessOperation is an owned operation carrying its access assessment.
type accessOperation struct {
	Access schemaext.AccessEffect `json:"access"`
}

func (*accessOperation) Kind() schemaext.Kind { return accessOperationKind }
func (p *accessOperation) CloneExtension() ast.ExtensionPayload {
	return &accessOperation{Access: p.Access}
}
func (p *accessOperation) AccessEffect() schemaext.AccessEffect { return p.Access }

func accessOperationCodec() schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) { return json.Marshal(payload) }
	return schemaext.Codec{
		Prototype: &accessOperation{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["access"],"additionalProperties":false,"properties":{"access":` +
			string(schemaext.AccessEffectSchema()) + `}}`),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			return payload.(*accessOperation).CloneExtension(), nil
		}, Encode: encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			return schemaext.DecodeJSON[*accessOperation](data)
		},
	}
}

// accessPlanningProvider plans every change as one access operation carrying
// the given assessment.
func accessPlanningProvider(access schemaext.AccessEffect) engine.Provider {
	provider := planningProvider(planningFunc(func(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
		contribution := plangraph.Contribution[featureplan.Operation]{Owner: "example.org/reverser"}
		result := featureplan.Result{Complete: true}
		for _, change := range request.Changes {
			step := plangraph.StepID{Owner: contribution.Owner, Name: change.Subject.Name.Source}
			contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: step, Payload: featureplan.Operation{
				Role: ast.StatementExtension, Payload: &accessOperation{Access: access},
			}})
			result.Changes = append(result.Changes, featureplan.ChangePlan{Subject: change.Subject, Kind: change.Value.Kind(),
				Strategy: "replace the policy", Steps: []plangraph.StepID{step}})
		}
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
		return result, nil
	}))
	provider.Codecs = append(provider.Codecs, accessOperationCodec())
	provider.Planning[0].OperationKinds = append(provider.Planning[0].OperationKinds, accessOperationKind)
	return provider
}

func TestPlanning_HappyPath_KeepsTheOwnersAccessAssessment(t *testing.T) {
	c := qt.New(t)
	access := schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "a permissive policy admits more rows"}
	result, err := mustRuntime(c, accessPlanningProvider(access)).PlanFeatures(t.Context(), planningRequest())
	c.Assert(err, qt.IsNil)
	c.Assert(result.Contributions, qt.HasLen, 1)
	c.Assert(result.Contributions[0].Steps, qt.HasLen, len(planningRequest().Changes))
	c.Assert(len(result.Contributions[0].Steps) > 0, qt.IsTrue)
	for _, step := range result.Contributions[0].Steps {
		source, ok := step.Payload.Payload.(schemaext.AccessEffectSource)
		c.Assert(ok, qt.IsTrue)
		c.Assert(source.AccessEffect(), qt.Equals, access)
	}
}

// A reply whose operation declares an access assessment and does not make one
// is malformed; the runtime refuses the whole batch rather than handing on an
// operation the safety classifier could only guess about.
func TestPlanning_FailurePath_RefusesAnOperationWithoutAnAccessAssessment(t *testing.T) {
	tests := []struct {
		name   string
		access schemaext.AccessEffect
	}{
		{name: "missing", access: schemaext.AccessEffect{}},
		{name: "unrecognized", access: schemaext.AccessEffect{Access: "safe", Reason: "not a declared assessment"}},
		{name: "unexplained", access: schemaext.AccessEffect{Access: schemaext.AccessNarrows}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := mustRuntime(c, accessPlanningProvider(tc.access)).PlanFeatures(t.Context(), planningRequest())
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, featureplan.Result{})
		})
	}
}
