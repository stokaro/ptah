package provider

// White-box testing required: a validation that checks an engine's value
// models once per request answers exactly as one that checks them again for
// every table, so only the answers the validation holds show how often it
// asked.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

const checkedKind = "example.org/fixture/retention"

// checkedDescription offers one engine with one desired model.
func checkedDescription() *protocol.Description {
	return &protocol.Description{Revision: Revision, ProviderId: "example.org/fixture", Version: "1", SemanticIdentity: "table-1",
		Features: []string{NormalizeValuesFeature},
		Engines: []*protocol.Engine{{Id: "fixture", Features: []string{"retention"}, ValueModels: []*protocol.ValueModel{{
			Kind: checkedKind, Representation: protocol.ValueInput_DESIRED, SchemaVersion: 1, Encoding: "json", DefinitionJson: []byte(`{"type":"object"}`),
			DefinitionSha256: "a2c799262a3ce3c19ef5cdd983bf3d12b43ab3c426227091b909dcb7054738c0", CanonicalFormat: "seconds-json-v1",
		}}}},
	}
}

// checkedValue is a normalized desired value of the engine's model, owned by
// table name.
func checkedValue(description *protocol.Description, name string) *protocol.NormalizedValue {
	owner := &protocol.Reference{Engine: "fixture", Kind: "table", Name: name}
	return &protocol.NormalizedValue{Id: name, Value: &protocol.Envelope{
		DefinitionSha256: description.Engines[0].ValueModels[0].DefinitionSha256, ProviderId: description.ProviderId, Kind: checkedKind,
		SchemaVersion: 1, Encoding: "json", Payload: []byte(`{"seconds":60}`), Owner: owner,
	}, NormalizedOwner: proto.CloneOf(owner), CanonicalFormat: "seconds-json-v1", CanonicalProjection: []byte(`{"seconds":60}`)}
}

// A validation asks for each target once and keeps the answer; a validation
// that holds no answers asks every time.
func TestTargetChecksAskOncePerQuestion(t *testing.T) {
	c := qt.New(t)
	description := checkedDescription()
	checks := new(targetChecks)

	engine, _, err := checks.valueTarget(description, "fixture", []string{"retention"})
	c.Assert(err, qt.IsNil)
	again, _, err := checks.valueTarget(description, "fixture", []string{"retention"})
	c.Assert(err, qt.IsNil)
	_, features, err := checks.valueTarget(description, "fixture", nil)
	c.Assert(err, qt.IsNil)
	_, _, err = checks.valueTarget(description, "other", nil)
	c.Assert(err, qt.ErrorIs, ErrProtocol)
	var none *targetChecks
	unheld, _, err := none.valueTarget(description, "fixture", []string{"retention"})

	c.Assert(err, qt.IsNil)
	c.Assert(again, qt.Equals, engine)
	c.Assert(unheld, qt.Equals, engine)
	c.Assert(features, qt.HasLen, 0)
	c.Assert(checks.answers, qt.HasLen, 3)
}

// The captured features of several tables, checked under one validation, ask
// for their target once.
func TestCapturedFeaturesShareTheAnswersOfTheirValidation(t *testing.T) {
	c := qt.New(t)
	description := checkedDescription()
	request := &protocol.PlanFeaturesRequest{Engine: "fixture", TargetFeatures: []string{"retention"}}
	checks := new(targetChecks)

	for _, name := range []string{"a", "b", "c"} {
		captured := &protocol.CapturedFeatures{
			Objects: &protocol.ComparisonState{Representation: protocol.ValueInput_DESIRED, Coverage: &protocol.ComparisonCoverage{}},
			Facets: &protocol.ComparisonState{Representation: protocol.ValueInput_DESIRED, Coverage: &protocol.ComparisonCoverage{}, Records: []*protocol.ComparisonRecord{{
				Kind: checkedKind, Subject: &protocol.ComparisonIdentity{Kind: "table", Name: &protocol.ComparisonPart{Source: name, Normalized: name}},
				Value: checkedValue(description, name),
			}}},
		}
		c.Assert(checkCapturedFeatures(checks, description, request, description.Engines[0], protocol.ValueInput_DESIRED, captured), qt.IsNil)
	}

	c.Assert(checks.answers, qt.HasLen, 1)
}
